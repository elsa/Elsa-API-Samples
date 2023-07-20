#!/usr/bin/env python3
# This client implements ELSA websocket protocol to stream audio to the scripted API.
# Example: python3 streaming_scripted_client.py --token <CLIENT_TOKEN> --sentence "pizza party" --audio_path pizza_party.wav

import argparse
import glob
import logging
import os
import shutil
import signal
import sys
import tempfile
import threading
import time
import queue
import ujson
from pydub import AudioSegment

from ws4py.client.threadedclient import WebSocketClient

# timeout class
class Timeout:
    """Timeout class using ALARM signal."""

    class Timeout(Exception):
        pass

    def __init__(self, sec):
        self.sec = sec

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.raise_timeout)
        signal.alarm(self.sec)

    def __exit__(self, *args):
        signal.alarm(0)  # disable alarm

    def raise_timeout(self, *args):
        raise Timeout.Timeout()

AUDIO_PACKET_SIZE = 0.5
SLEEP_TIME = AUDIO_PACKET_SIZE

# keep track of last word length
last_word_len = 0

# Audio splitting into chunks
class SplitWavAudio():
    def __init__(self, output_folder, input_filepath):
        self.input_filepath = input_filepath
        self.input_filename = os.path.basename(input_filepath)
        self.output_folder = output_folder        
        self.audio = AudioSegment.from_wav(self.input_filepath)
    
    def get_duration(self):
        return self.audio.duration_seconds
    
    def single_split(self, from_msec, to_msec, split_filename):
        split_audio = self.audio[from_msec:to_msec]
        split_audio.export(os.path.join(self.output_folder, split_filename), format="wav")
        
    def multiple_split(self, msec_per_split):
        total_msec = int(self.get_duration() * 1000)
        for i in range(0, total_msec, int(msec_per_split)):
            split_fn = str(i).zfill(10) + '_' + self.input_filename
            self.single_split(i, i+msec_per_split, split_fn)

# Thread class with overloaded join() method that implements a flag to stop execution
class AudioThread(threading.Thread):
    """ Ask the thread to stop by calling its join() method. """

    def __init__(self, ws, file_path, start_packet=0):
        super(AudioThread, self).__init__()
        self.ws = ws
        self.file_path = file_path  # path of the file to be sent
        self.stoprequest = False
        self.startPacket = start_packet
        self.record = True  # start recording as soon as thread starts
        self.stream = None

    def run(self):
        # We initially send the audio and then iterate with silence until we
        # receive a request to stop
        # split the initial audio into pieces of 0.5s
        directory_name = tempfile.mkdtemp()

        # Audio file splitting
        split_wav = SplitWavAudio(directory_name, self.file_path)
        split_wav.multiple_split(AUDIO_PACKET_SIZE * 1000)

        # sort files_list
        files_list = glob.glob("%s/*.%s" % (directory_name, 'wav'))
        files_list.sort()

        # send each of the pieces
        for idx in range(self.startPacket, len(files_list)):
            if self.stoprequest:
                logging.info("Stopping sending audio packets")
                break
            audioFile = files_list[idx]
            logging.info("Sending packet %d" % idx)
            with open(audioFile, "rb") as af:
                self.ws.send(af.read(), binary=True)
            time.sleep(SLEEP_TIME)  # simulate 0.5s blocks wait

        # send special control code to the server to signal that no more blocks are left.
        logging.info("Sending end_stream")
        self.ws.send('{"type": "ELSA:end_stream"}')

        # cleanup
        shutil.rmtree(directory_name)

    def stop(self, timeout=None):
        self.stoprequest = True
        self.record = False


# This is an overloaded class from ws4py
class MyClient(WebSocketClient):

    def __init__(self, args, protocols=None, extensions=None, heartbeat_freq=None, start_packet=0):
        url = "wss://api.elsanow.io/api/v2/ws/score_audio"
        super(MyClient, self).__init__(url, protocols, extensions, heartbeat_freq, headers=[
            ('Authorization', f'ELSA {args.token}')])
        self.fn = args.audio_path
        self.results_queue = queue.Queue()  # where results from the server are stored
        self.start_packet = start_packet
        self.sentence = args.sentence
        self.connected = False
        self.result_received = False  # hack to avoid printing spurious errors after endpoint is detected
        self.api_plan = args.api_plan
        self.return_feedback_hints = args.return_feedback_hints
        self.audio_thread = None


    def is_connected(self):
        return self.connected

    def send_data(self, data):
        logging.info("Sending audio data")
        self.send(data, binary=True)

    # this gets called once the ws has connected
    def opened(self):
        logging.info("Socket opened! ready for action.")
        self.connected = True

    # we explicitly start a new stream
    def start_stream(self):
        logging.info("Starting a new stream")

        request = {
            "type": "ELSA:start_stream",
            "data": {
                "stream_info": {
                    "api_plan": self.api_plan,
                },
            }
        }

        # if sentence is provided
        if len(self.sentence):
            request["data"]["stream_info"]["sentence"] = self.sentence

        self.send(ujson.dumps(request))

        # logging
        request_body = ujson.dumps(request, indent=4)
        logging.info("Sending start_stream request")
        logging.debug("Request body:\n%s", request_body)

        # initiate the audio thread
        self.audio_thread = AudioThread(self, self.fn, self.start_packet)

    # This gets called every time a message is received from the server
    def received_message(self, m):
        global last_word_len

        # reads the response
        response = ujson.loads(str(m))

        # every type in the response spawns a new message being sent
        if "type" in response:

            # Some error sent from the server
            if response["type"] == "ELSA:error":
                logging.error("Received error message >>%s<<, closing", response["message"])
                self.audio_thread.stop()
                self.close(100, "closing please")

            # Some warnings sent form the server
            elif response["type"] == "ELSA:warning":
                logging.warning("Received warning message: %s", response["message"])

            # stream is open, we can start sending audio
            elif response["type"] == "ELSA:start_stream_result":
                logging.info("Received start_stream response, starting to send audio.")
                self.audio_thread.detach = True
                self.audio_thread.start()

            # Server finished computing and sends back the results
            elif response["type"] == "ELSA:decoding_result":
                self.result_received = True
                logging.info(
                    "Received decoding result, sentence: %s" % response['data']['utterance'][0]['sentence'])
                if 'stream_finished' in response['data'] and response['data']["stream_finished"] is True:
                    self.audio_thread.stop()

            # Server informs the client that the server is ready to receive new streams
            elif response["type"] == "ELSA:ready":
                self.results_queue.put(ujson.loads('{"type": "stream_finished"}'))

            # used for the server to send a message to the client
            elif response["type"] == "ELSA:message":
                self.results_queue.put(response)

            # Server acknowledges that it received an audio packet
            elif response["type"] == "ELSA:audioACK":
                pass

            # Server sends information on what specific server the client connected to
            elif response["type"] == "ELSA:wsConnect":
                pass

            # Server informs that it stopped accepting audio. Any more audio sent after this will be ignored
            elif response["type"] == "ELSA:stopped_listening":
                logging.info("Got ELSA:stopped_listening - Stop sending audio")
                self.audio_thread.stop()

            else:
                logging.error("Received unknown response, closing ws: %s" % str(m))
                self.close()

            # we store the response for later processing (and also for the main program to know when to stop)
            self.results_queue.put(response)

        else:
            logging.error("Received message without type in the body. Closing connection")
            self.close()

    def get_result(self, timeout=60):
        return self.results_queue.get(timeout=timeout)

    # This will trigger in case there is any issue on the server side, or we just decide to close the connection for
    # other reasons.
    def closed(self, code=0, reason=None):
        if code != 1000:
            logging.error("Websocket is being closed, code is %d, reason %s" % (code, reason))
        # We make the client trigger the closing here so that it does make sure the socket was closed on server side
        self.results_queue.put(ujson.loads('{"type": "client_finished"}'))


def main():
    parser = argparse.ArgumentParser(description='Command line client for streaming scripted API')
    parser.add_argument('--sentence', dest='sentence', help="Sentence to be decoded", required=True)
    parser.add_argument('--audio_path', dest='audio_path', help="Local path of audio file to be sent to server.", required=True)
    parser.add_argument('--api_plan', dest='api_plan', help="API plan", default='standard')
    parser.add_argument('--return_feedback_hints', dest='return_feedback_hints', help="Return feedback hints",
                        action='store_true')
    parser.add_argument('--token', dest='token', help="token for authentication")
    parser.add_argument('-o', '--output_file', help="Path to output file", default="./response.json")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    # ws connection
    ws = MyClient(args)
    try:
        ws.connect()
    except Exception as e:
        logging.error("Failed to connect. Reason: %s" % e)
        return

    # block until ws is connected
    while not ws.is_connected():
        logging.info("Waiting for websocket connection...")
        time.sleep(0.1)  # 100ms

    # this should block 150s or until we get a result in the queue
    # We iteratively take out anything that gets stored into the queue and (optionally) print it out.
    # We do it until we read a type indicating end of process or timeout happens
    try:
        max_time = 150
        with Timeout(max_time):  # by default, max 150s to complete
            st = time.time()
            ws.start_stream()
            while True:
                result = ws.get_result(max_time)
                if "type" in result:
                    if result["type"] in ["ELSA:decoding_result", "ELSA:error"]:
                        et = time.time()
                        logging.info("Stream time: {0:5.2f} sec".format(et - st))
                        break
    except Timeout.Timeout:
        logging.critical("Timed out")
        sys.exit(1)

    # output the response
    logging.info("Writing response to %s" % args.output_file)
    with open(args.output_file, 'w') as outfile:
        outfile.write(ujson.dumps(result, indent=4))

    # all good, exit
    sys.exit(0)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# This client implements ELSA websocket protocol to stream audio to the unscripted API.
# example: python3 streaming_unscripted_client.py --token <CLIENT_TOKEN> --audio_path pizza_party.wav --return_json

import argparse
import glob
import json
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

# thread class with overloaded join() method that implements a flag to stop execution
class AudioThread(threading.Thread):
    """ Ask the thread to stop by calling its join() method. """

    def __init__(self, ws, file_path, chunk_size, sleep_time):
        super(AudioThread, self).__init__()
        self.ws = ws
        self.file_path = file_path  # path of the file to be sent
        self.stoprequest = False
        self.record = True  # start recording as soon as thread starts
        self.stream = None
        self.chunk_size = chunk_size
        self.sleep_time = sleep_time

    def run(self):
        # We initially send the audio and then iterate with silence until we
        # receive a request to stop
        # split the initial audio into chunks (e.g. 0.5s)
        directory_name = tempfile.mkdtemp()

        # Audio file
        split_wav = SplitWavAudio(directory_name, self.file_path)
        split_wav.multiple_split(self.chunk_size * 1000)

        # sort files_list
        files_list = glob.glob("%s/*.%s" % (directory_name, 'wav'))
        files_list.sort()

        # send each of the pieces
        for idx in range(0, len(files_list)):
            if self.stoprequest:
                logging.info("Stopping sending audio packets")
                break
            audioFile = files_list[idx]
            logging.info("Sending packet %d", idx)
            with open(audioFile, "rb") as af:
                self.ws.send(af.read(), binary=True)

            if idx == len(files_list) - 1:
                # send special control code to signal that no more blocks are left.
                logging.info("Sending end_stream")
                self.ws.send('{"type": "ELSA:end_stream"}')

            # simulate waiting for packets
            if self.sleep_time < 0:
                time.sleep(self.chunk_size)
            else:
                time.sleep(self.sleep_time)

        # cleanup
        shutil.rmtree(directory_name)

    def stop(self):
        self.stoprequest = True
        self.record = False


# This is an overloaded class from ws4py
class MyClient(WebSocketClient):

    def __init__(self, args, protocols=None, extensions=None, heartbeat_freq=None):
        url = 'wss://api.elsanow.io/api/v1/ws/score_audio_plus'
        super(MyClient, self).__init__(url, protocols, extensions, heartbeat_freq, headers=[
            ('Authorization', f'ELSA {args.token}')])
        self.fn = args.audio_path
        self.results_queue = queue.Queue()  # where results from the server are stored
        self.connected = False
        self.result_received = False  # hack to avoid printing spurious errors after endpoint is detected
        self.api_plan = args.api_plan
        self.stream_id = None
        self.chunk_size = 0.5 # audio chunk size we want to stream to the server
        self.sleep_time = -1 # when set to -1 it simulates a real streaming scenario, else it streams audio in shorter periods
        self.return_json = args.return_json

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

        # 1) Send config with the info for the stream to be created
        request = {
            'api_plan': self.api_plan,
            'return_json': self.return_json
        }
        logging.info("Sending config")
        self.send(ujson.dumps(request))

        # 2) open a new audio thread
        self.audio_thread = AudioThread(self, self.fn, self.chunk_size, self.sleep_time)

    # This gets called every time a message is received from the server
    def received_message(self, m):
        # reads the response
        response = ujson.loads(str(m))

        # every status in the response spawns a new message being sent
        if "status" in response:
            if response["status"] == "failed":
                logging.error('failed: %s', response["reason"])
                self.audio_thread.stop()
                self.close()

            elif response["status"] == "processing":
                if "stream_id" in response:
                    logging.info('processing stream ID: %s', response["stream_id"])
                    self.stream_id = response["stream_id"]
                    self.audio_thread.detach = True
                    self.audio_thread.start()
                else:
                    logging.info('processing: %s', ujson.dumps(response, indent=4))

            elif response['status'] == 'success':
                self.audio_thread.stop()

                logging.info('success: %s', ujson.dumps(response, indent=4))
                if 'result_link' in response:
                    logging.info('result url: %s', response['result_link'])
                else:
                    logging.info('Something went wrong, no result url was received')

                logging.info('audio url: %s', response['audio_link'])
                self.result_received = True

            else:
                logging.error("Received unknown status: %s",  str(m))

            # we store the response for later processing (and also for the main program to know when to stop)
            self.results_queue.put(response)

        else:
            logging.error("Received message without status in the body. Closing connection. Msg: %s" % str(m))
            self.close()

    def get_result(self, timeout=60):
        return self.results_queue.get(timeout=timeout)

    # This will trigger in case there is any issue on the server side, or we just decide to close the connection for
    # other reasons.
    def closed(self, code=0, reason=None):
        if code != 1000:
            logging.error("Websocket is being closed, code is %d, reason %s" % (code, reason))
        # We make the client trigger the closing here so that it does make sure the socket was closed on server side
        self.results_queue.put(json.loads('{"type": "client_finished"}'))


def main():
    parser = argparse.ArgumentParser(description='Command line client for streaming unscripted API')
    parser.add_argument('--audio_path', help="Local path to audio file to be tested", required=True)
    parser.add_argument('--api_plan', default="standard", dest="api_plan", help="API plan for metering")
    parser.add_argument('--token', dest='token', help="token for authentication")
    parser.add_argument('--return_json', default=False, dest="return_json", help="Returns Json on response", action='store_true')
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
        max_time = 150  # s
        with Timeout(max_time):
            start_time = time.time()
            ws.start_stream()
            while True:
                result = ws.get_result(max_time)
                if "status" in result:
                    if result["status"] in ["success", "failed"]:
                        logging.info("Stream time: {0:5.2f} sec".format(time.time() - start_time))
                        break
    except Timeout.Timeout:
        logging.critical("Timed out")
        sys.exit(1)

    # all good, exit
    sys.exit(0)


if __name__ == "__main__":
    main()

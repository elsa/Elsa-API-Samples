
## This client implements ELSA websocket protocol to stream audio to the scripted API.

````bash
python3 streaming_scripted_client.py --token <CLIENT_TOKEN> --sentence "pizza party" --audio_path pizza_party.wav
````

# This client implements ELSA websocket protocol to stream audio to the unscripted API.

````bash
python3 streaming_unscripted_client.py --token <CLIENT_TOKEN> --audio_path pizza_party.wav --return_json
````

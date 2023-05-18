import random
# import boto3
from datetime import datetime, timedelta
from paho.mqtt import client as mqtt_client
import json
import time, string
import statistics
import requests
from google.oauth2 import service_account
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from tqdm import tqdm

# broker = '52.201.8.27'
broker = '44.206.62.89'
port = 1883
topic = "test"
client_id = f'python-mqtt-{random.randint(0, 100)}'

# Load your service account credentials from the JSON key file
SERVICE_ACCOUNT_FILE = './single-portal-370222-e246ff8e255b.json'
SCOPES = ['https://www.googleapis.com/auth/documents']

credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
docs_service = discovery.build('sheets', 'v4', credentials=credentials)

sheet_id = '1ZXWPNRMMosoQASPTHYDKGM_nKW3Wdc4OCZe_6YiHVCY'

now = datetime.now()
dt = timedelta(hours = 1)

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


column_mapping = {}

def subscribe(client: mqtt_client):

    def on_message(client, userdata, msg):

        # print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        if msg.topic == 'report':
            try:
                payload = msg.payload.decode()
                payload = payload.replace(",\"","\",\"")
                payload = payload.replace("\"\"","\"")
                payload = json.loads(payload)

                param = payload['param']
                value = float(payload['value'])
                global column_mapping
                if not param in column_mapping:
                    create_column(service, sheet_id, param)
                # Append values to a column
                values = [value]

                append_values_to_column(service, sheet_id, param, values)


            except Exception as e:
                # print(e)
                pass



    # client.subscribe(topic)
    client.subscribe('report')
    # client.subscribe('irrigation')
    # client.subscribe('setparam')
    # client.subscribe('fans')
    client.on_message = on_message



desired_states = {"front":1,"back":1,"center":1,"left":0,"right":0}
manual_mode = 2
pushing_states = time.time()
def push_states():
    global desired_states
    global devices
    if not manual_mode:
        pushing_states = time.time()
        for k in desired_states.keys():

            if k in devices:
                attempts = 0
                wait = 3
                while (devices[k] != desired_states[k]) and (attempts < 1):
                    attempts += 1
                    print("PUBLISHING:",k.upper())
                    client.publish('fans',k.upper())
                    time.sleep(wait)
                    wait += 2



def create_column(service, spreadsheet_id, key):
    try:
        global column_mapping
        # The next column is one position after the last one in the column_mapping
        col_letter = chr(65 + len(column_mapping))  # Convert the index to a character starting from 'A'


        range_ = f"{col_letter}1:{col_letter}1"  # This will generate a range like 'A1:A1' or 'B1:B1', etc.
        print(range_)
        request = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_,
            valueInputOption="USER_ENTERED",
            body={
                "values": [[key]]
            }
        )
        response = request.execute()
        print(response)

        # Update the column_mapping dictionary
        column_mapping[key] = col_letter

    except HttpError as error:
        print(f"An error occurred: {error}")


def append_values_to_column(service, spreadsheet_id, key, values):
    try:
        # Get the corresponding column letter
        col_letter = column_mapping.get(key)
        if not col_letter:
            print(f"No column found for the key: {key}")
            return

        range_ = f"{col_letter}:{col_letter}"  # This will generate a range like 'A:A' or 'B:B', etc.

        request = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=range_,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={
                "values": [[value] for value in values]
            }
        )
        response = request.execute()
        print(response)

    except HttpError as error:
        print(f"An error occurred: {error}")

# Create columns



client = connect_mqtt()
subscribe(client)

if __name__ == '__main__':
    waitTime = 60
    startTime = time.time()
    m = 0
    # run()

    while True:
        client.loop()
        elapsedTime = time.time() - startTime
        if elapsedTime > waitTime:
            # client.publish('fans','1')
            #push_desired("front",1)
            #push_states()
            m += 1
            if m == 60:
                m = 0

            # if not (m%30):
            #     pass
            startTime = time.time()

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
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = discovery.build('sheets', 'v4', credentials=credentials)

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
                # Append values to a column
                values = [value]

                if 'sheet' in payload:
                    sheet_name = payload['sheet']
                    append_values_to_column(service, sheet_id, param, values, sheet_name)
                else:
                    append_values_to_column(service, sheet_id, param, values)


            except Exception as e:
                print(e)
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
def get_column_letter(service, spreadsheet_id, key, sheet_name='Sheet1'):
    try:
        # Get the data in the first row
        range_ = f"{sheet_name}!1:1"
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_).execute()
        values = result.get('values', [])

        print('RES:',result)

        if not values:
            return None

        values = values[0]  # The first (and only) row

        # Find the column (index) of the key
        try:
            col_index = values.index(key)
        except ValueError:
            # If the key is not found in the columns
            return None

        # Convert the column index to a letter (A, B, C, etc.)
        col_letter = chr(65 + col_index)  # Convert the index to a character starting from 'A'

        return col_letter

    except HttpError as error:
        print(f"An error occurred: {error}")
        return None

def create_column(service, spreadsheet_id, key, sheet_name='Sheet1'):
    try:
        col_letter = get_column_letter(service, spreadsheet_id, key, sheet_name)

        # If the column does not exist, create it
        if not col_letter:
            # Get the data in the first row
            range_ = f"{sheet_name}!1:1"
            result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_).execute()
            values = result.get('values', [])

            # Find the next empty column
            col_index = len(values[0]) if values else 0
            col_letter = chr(65 + col_index)  # Convert the index to a character starting from 'A'

            range_ = f"{sheet_name}!{col_letter}1:{col_letter}1"  # This will generate a range like 'A1:A1' or 'B1:B1', etc.
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

        else:
            print(f"Column {key} already exists.")

    except HttpError as error:
        print(f"An error occurred: {error}")


def append_values_to_column(service, spreadsheet_id, key, values, sheet_name='Sheet1'):
    try:
        # Get the corresponding column letter
#         col_letter = column_mapping.get(key)
        col_letter = get_column_letter(service, spreadsheet_id, key, sheet_name)
        if not col_letter:
            create_column(service, spreadsheet_id, key, sheet_name)
            col_letter = get_column_letter(service, spreadsheet_id, key, sheet_name)
            if not col_letter:
                print(f"No column found for the key: {key}")
                return

        range_ = f"{sheet_name}!{col_letter}:{col_letter}"  # This will generate a range like 'A:A' or 'B:B', etc.

        # Get the data in the column
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_).execute()
        rows = result.get('values', [])

        # Count the number of rows and find the next row
        next_row = len(rows) + 1

        for value in values:
            range_ = f"{sheet_name}!{col_letter}{next_row}:{col_letter}{next_row}"  # This will generate a range like 'A2:A2' or 'B3:B3', etc.
            request = service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_,
                valueInputOption="USER_ENTERED",
                body={
                    "values": [[value]]
                }
            )
            response = request.execute()
            print(response)

            next_row += 1

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

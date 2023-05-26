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
import pytz

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

all_params = ['t','h','l']
column_mapping = {}

def subscribe(client: mqtt_client):

    def on_message(client, userdata, msg):

        # print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        if msg.topic == 'report':
            try:
                payload = msg.payload.decode()
                payload = payload.replace(",\"","\",\"")
                payload = payload.replace("\"\"","\"")
                data = json.loads(payload)
                print('DATA:',data,'\n')
                # append_to_row(service, sheet_id, data["sheet"], data)
                #
                # for is_param in all_params:
                #     if is_param in payload['values']:
                #         param = is_param
                #         value = payload['value'][param]
                #         append_values_to_column(service, sheet_id, param, values, sheet_name)
                # # Append values to a column
                # values = [value]
                # print(payload)
                if 'sheet' in payload:
                    sheet_name = data['sheet']
                    # append_values_to_column(service, sheet_id, param, values, sheet_name)
                    append_to_row(service, sheet_id, sheet_name, data)
                else:
                    # append_values_to_column(service, sheet_id, param, values)
                    append_to_row(service, sheet_id, "Sheet1", data)
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




# def get_column_letters(service, spreadsheet_id, sheet_name, key):
#     # Request for the first row
#     response = service.spreadsheets().values().get(
#         spreadsheetId=spreadsheet_id,
#         range=f"{sheet_name}!1:1"
#     ).execute()
#
#     values = response.get('values', [])
#
#     if values and key in values[0]:
#         return chr(65 + values[0].index(key))  # ASCII code 65 = 'A'
#     else:
#         return None

def find_last_empty_row(service, spreadsheet_id, sheet_name):
    # Request for the number of filled rows in the first column
    response = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A:A"
    ).execute()
    values = response.get('values', [])
    return len(values) + 1  # Next empty row

def get_column_letter(service, spreadsheet_id, key, sheet_name='Sheet1'):
    try:
        # Get the data in the first row
        range_ = f"{sheet_name}!1:1"
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_).execute()
        values = result.get('values', [])
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

def append_to_row(service, spreadsheet_id, sheet_name, data):
    batch_data = []
    for key, value in data["values"].items():
        column_letter = get_column_letter(service, spreadsheet_id, key, sheet_name)
        if not column_letter:
            create_column(service, spreadsheet_id, key, sheet_name)
        column_letter = get_column_letter(service, spreadsheet_id, key, sheet_name)
        last_empty_row = find_last_empty_row(service, spreadsheet_id, sheet_name, column_letter)
        range_ = f"{sheet_name}!{column_letter}{last_empty_row}"
        batch_data.append({"range": range_, "values": [[value]]})

    # Add the id to the "id" column
    id_column_letter = get_column_letter(service, spreadsheet_id, "DEVICE ID", sheet_name)
    if not id_column_letter:
        create_column(service, spreadsheet_id, "DEVICE ID", sheet_name)
    id_column_letter = get_column_letter(service, spreadsheet_id, "DEVICE ID", sheet_name)

    if id_column_letter:
        range_ = f"{sheet_name}!{id_column_letter}{last_empty_row}"
        batch_data.append({"range": range_, "values": [[data["id"]]]})

    # Now batch update all the rows at once
    body = {"valueInputOption": "USER_ENTERED", "data": batch_data}
    service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()






def find_last_empty_row(service, spreadsheet_id, sheet_name, column):
    # Request for the number of filled rows in the specified column
    response = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!{column}:{column}"
    ).execute()
    values = response.get('values', [])
    return len(values) + 1  # Next empty row

# def append_to_row(service, spreadsheet_id, sheet_name, data):
#     batch_data = []
#     for key, value in data["values"].items():
#         column_letter = get_column_letters(service, spreadsheet_id, sheet_name, key)
#         if column_letter:
#             last_empty_row = find_last_empty_row(service, spreadsheet_id, sheet_name, column_letter)
#             range_ = f"{sheet_name}!{column_letter}{last_empty_row}"
#             batch_data.append({"range": range_, "values": [[value]]})
#
#     # Add the id to the "id" column
#     id_column_letter = get_column_letters(service, spreadsheet_id, sheet_name, "id")
#     if id_column_letter:
#         last_empty_row = find_last_empty_row(service, spreadsheet_id, sheet_name, id_column_letter)
#         range_ = f"{sheet_name}!{id_column_letter}{last_empty_row}"
#         batch_data.append({"range": range_, "values": [[data["id"]]]})
#
#     # Now batch update all the rows at once
#     body = {"valueInputOption": "USER_ENTERED", "data": batch_data}
#     service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

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


def append_to_row(service, spreadsheet_id, sheet_name, data):
    batch_data = []
    for key, value in data["values"].items():
        column_letter = get_column_letter(service, spreadsheet_id, key, sheet_name)
        if not column_letter:
            create_column(service, spreadsheet_id, key, sheet_name)
        column_letter = get_column_letter(service, spreadsheet_id, key, sheet_name)
        last_empty_row = find_last_empty_row(service, spreadsheet_id, sheet_name, column_letter)
        range_ = f"{sheet_name}!{column_letter}{last_empty_row}"
        batch_data.append({"range": range_, "values": [[value]]})

    # Add the id to the "id" column
    id_column_letter = get_column_letter(service, spreadsheet_id, "DEVICE ID", sheet_name)
    if not id_column_letter:
        create_column(service, spreadsheet_id, "DEVICE ID", sheet_name)
    id_column_letter = get_column_letter(service, spreadsheet_id, "DEVICE ID", sheet_name)
    if id_column_letter:
        range_ = f"{sheet_name}!{id_column_letter}{last_empty_row}"
        batch_data.append({"range": range_, "values": [[data["id"]]]})

    # Add the time to the "Time" column
    time_column_letter = get_column_letter(service, spreadsheet_id, "Time", sheet_name)
    if not time_column_letter:
        create_column(service, spreadsheet_id, "Time", sheet_name)
    time_column_letter = get_column_letter(service, spreadsheet_id, "Time", sheet_name)
    if time_column_letter:
        time_str = "{}:{}:{}".format(data["time"]["h"], data["time"]["m"], data["time"]["s"])
        range_ = f"{sheet_name}!{time_column_letter}{last_empty_row}"
        batch_data.append({"range": range_, "values": [[time_str]]})

    # Now batch update all the rows at once
    body = {"valueInputOption": "USER_ENTERED", "data": batch_data}
    service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def append_to_row(service, spreadsheet_id, sheet_name, data):
    # Find the last empty row in the 'Time' column at the start
    column_letter = get_column_letter(service, spreadsheet_id, "real_time", sheet_name)
    if not column_letter:
        create_column(service, spreadsheet_id, "real_time", sheet_name)
    column_letter = get_column_letter(service, spreadsheet_id, "real_time", sheet_name)
    last_empty_row = find_last_empty_row(service, spreadsheet_id, sheet_name, column_letter)

    batch_data = []
    for key, value in data["values"].items():
        column_letter = get_column_letter(service, spreadsheet_id, key, sheet_name)
        if not column_letter:
            create_column(service, spreadsheet_id, key, sheet_name)
        column_letter = get_column_letter(service, spreadsheet_id, key, sheet_name)
        range_ = f"{sheet_name}!{column_letter}{last_empty_row}"
        batch_data.append({"range": range_, "values": [[value]]})

    # Add the id to the "id" column
    id_column_letter = get_column_letter(service, spreadsheet_id, "device_id", sheet_name)
    if not id_column_letter:
        create_column(service, spreadsheet_id, "device_id", sheet_name)
    id_column_letter = get_column_letter(service, spreadsheet_id, "device_id", sheet_name)
    if id_column_letter:
        range_ = f"{sheet_name}!{id_column_letter}{last_empty_row}"
        batch_data.append({"range": range_, "values": [[data["id"]]]})

    # Add the time to the "Time" column
    time_column_letter = get_column_letter(service, spreadsheet_id, "rtc_time", sheet_name)
    if not time_column_letter:
        create_column(service, spreadsheet_id, "rtc_time", sheet_name)
    time_column_letter = get_column_letter(service, spreadsheet_id, "rtc_time", sheet_name)
    if time_column_letter:
        time_str = "{}:{}:{}".format(data["time"]["h"], data["time"]["m"], data["time"]["s"])
        range_ = f"{sheet_name}!{time_column_letter}{last_empty_row}"
        batch_data.append({"range": range_, "values": [[time_str]]})

    # Add the real time to the "real_time" column
    real_time_column_letter = get_column_letter(service, spreadsheet_id, "real_time", sheet_name)
    if not real_time_column_letter:
        create_column(service, spreadsheet_id, "real_time", sheet_name)
    real_time_column_letter = get_column_letter(service, spreadsheet_id, "real_time", sheet_name)
    if real_time_column_letter:
        cst = pytz.timezone('America/Chicago')  # Central Standard Time
        real_time = datetime.now(cst).strftime("%H:%M:%S")  # Get the current time in CST
        range_ = f"{sheet_name}!{real_time_column_letter}{last_empty_row}"
        batch_data.append({"range": range_, "values": [[real_time]]})

    # Now batch update all the rows at once
    body = {"valueInputOption": "USER_ENTERED", "data": batch_data}
    service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def append_to_row(service, spreadsheet_id, sheet_name, data):
    # Define the column order
    column_order = ["real_time", "rtc_time", "device_id"] + list(data["values"].keys())

    # Create columns if they don't exist
    for column_name in column_order:
        column_letter = get_column_letter(service, spreadsheet_id, column_name, sheet_name)
        if not column_letter:
            create_column(service, spreadsheet_id, column_name, sheet_name)

    # Find the last empty row in the 'real_time' column at the start
    column_letter = get_column_letter(service, spreadsheet_id, "real_time", sheet_name)
    last_empty_row = find_last_empty_row(service, spreadsheet_id, sheet_name, column_letter)

    # Add the real time to the "real_time" column
    cst = pytz.timezone('America/Chicago')  # Central Standard Time
    real_time = datetime.now(cst).strftime("%H:%M:%S")  # Get the current time in CST
    batch_data = [{"range": f"{sheet_name}!{column_letter}{last_empty_row}", "values": [[real_time]]}]

    # Add the id, rtc_time and values to their respective columns
    for column_name in column_order[1:]:
        column_letter = get_column_letter(service, spreadsheet_id, column_name, sheet_name)
        if column_name == "rtc_time":
            time_str = "{}:{}:{}".format(data["time"]["h"], data["time"]["m"], data["time"]["s"])
            value = time_str
        elif column_name == "device_id":
            value = data["id"]
        else:  # For any other column
            value = data["values"][column_name]

        range_ = f"{sheet_name}!{column_letter}{last_empty_row}"
        batch_data.append({"range": range_, "values": [[value]]})

    # Now batch update all the rows at once
    body = {"valueInputOption": "USER_ENTERED", "data": batch_data}
    service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

client = connect_mqtt()
subscribe(client)

if __name__ == '__main__':
    waitTime = 60
    startTime = time.time()
    m = 0
    first = True
    # run()


    while True:
        client.loop()
        if first:
            print('HELLO')
            data = {"id":"3", "time":{"h":"15","m":"34","s":"7"}, "values":{"param1":"87.1", "param2":"22"}, "sheet":"Sheet1"}
            append_to_row(service, sheet_id, data["sheet"], data)
            first = False
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

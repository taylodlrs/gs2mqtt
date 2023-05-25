import random
import time
from paho.mqtt import client as mqtt_client


broker = '44.206.62.89'
port = 1883
client_id = f'python-mqtt-{random.randint(0, 100)}'
colors = ["red", "green", "blue"]

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def publish(client):
    msg_count = 0
    while True:
        time.sleep(30)
        color = colors[msg_count % 3]
        msg = str(random.randint(1, 255))
        result = client.publish(color, msg)

        # Publish 0 to other colors
        for other_color in colors:
            if other_color != color:
                client.publish(other_color, "0")

        # If publish was successful
        if result[0] == 0:
            print(f"Sent {msg} to topic {color}")
        else:
            print(f"Failed to send message to topic {color}")
        msg_count += 1

def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)

if __name__ == "__main__":
    run()

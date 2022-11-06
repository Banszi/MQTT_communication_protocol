from paho.mqtt import client as mqtt_client
import time


class MQTT_subscriber:

    def connect_mqtt(self, broker_url, mqtt_port, client_id) -> mqtt_client:
        def on_connect(client, userdata, flags, errorCode):
            if errorCode == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect MQTT Broker!")

        self.client = mqtt_client.Client(client_id)
        self.client.on_connect = on_connect
        self.client.connect(broker_url, mqtt_port, keepalive=60)
        return self.client

    def subscribe(self, client: mqtt_client, topic: str):
        def on_message(client, userdata, msg):
            print(f"Received message \n"
                  f"Topic: {msg.topic} \n"
                  f"Payload: {msg.payload.decode()} \n")

        def on_log(client, userdata, level, buf):
            print("Log: " + buf)

        def on_disconnect(client, userdata, flags, errorCode=0):
            print(f"Subscriber disconnected with errorCode = {errorCode}")

        print(topic)
        time.sleep(1)
        self.client.subscribe(topic, qos=1)
        self.client.on_message = on_message
        self.client.on_log = on_log
        self.client.on_disconnect = on_disconnect

    def run(self, broker_url, mqtt_port, topic, client_id):
        self.client = self.connect_mqtt(broker_url, mqtt_port, client_id)
        self.subscribe(self.client, topic)
        self.client.loop_start()

        time.sleep(40)

        self.client.loop_stop()
        self.client.disconnect()





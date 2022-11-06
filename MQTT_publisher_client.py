from paho.mqtt import client as mqtt_client
import time
import sys
import subprocess


class MQTT_publisher:

    def __init__(self, argv_list):
        self.argv_list = argv_list
        print(self.argv_list)
        self.broker_url = argv_list[1]
        self.mqtt_port = int(argv_list[2])
        self.topic = argv_list[3]
        self.client_id = argv_list[4]

    def connect_mqtt(self, broker_url, mqtt_port, client_id) -> mqtt_client:
        def on_connect(client, userdata, flags, errorCode):
            if errorCode == 0:
                print("Connected to MQTT Broker!")
            else:
                print(f"Failed to connect, return code {errorCode} \n")

        def on_log(client, userdata, level, buf):
            print("Log: " + buf)

        def on_disconnect(client, userdata, flags, errorCode=0):
            print(f"Publisher disconnected with errorCode = {errorCode}")

        client = mqtt_client.Client(client_id)
        client.on_connect = on_connect
        client.on_log = on_log
        client.on_disconnect = on_disconnect
        client.connect(broker_url, mqtt_port, keepalive=60)
        return client

    def publish(self, client: mqtt_client, topic: str):
        msg_count = 0
        for index_send_msg in range(5):
            time.sleep(10)
            msg = f"This is {msg_count} message send by MQTT protocol!"
            #msg = f"Message: {msg_count}"
            pub_result = client.publish(topic, msg, qos=1)
            status = pub_result[0]
            if status == 0:
                print(f"Send message: \n"
                      f"Payload: {msg} \n"
                      f"Topic: {topic} \n")
            else:
                print(f"Failed to send message to topic: {topic}")

            msg_count += 1

    def run(self):
        client = self.connect_mqtt(self.broker_url, self.mqtt_port, self.client_id)
        client.loop_start()
        self.publish(client, self.topic)
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":

    arg_list = sys.argv

    publiser = MQTT_publisher(arg_list)
    publiser.run()





from MQTT_publisher_client import MQTT_publisher
from MQTT_subscriber_client import MQTT_subscriber
from time import sleep
import os
import subprocess
from threading import Thread
from multiprocessing import Process
import sys


if __name__ == "__main__":

    broker_url = "broker.hivemq.com"
    mqtt_port = 1883
    topic = "topic1/mqtt1"
    sub_client_id = "2"
    pub_client_id = "1"

    script_name = "MQTT_publisher_client.py"
    id = subprocess.Popen([sys.executable, script_name, broker_url, str(mqtt_port), topic, pub_client_id],
                          stdout=subprocess.PIPE)


    sleep(1)

    subscriber1 = MQTT_subscriber()
    subscriber1.run(broker_url, mqtt_port, topic, sub_client_id)


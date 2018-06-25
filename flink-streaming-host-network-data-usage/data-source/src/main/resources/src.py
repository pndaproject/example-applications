"""
Name        : src.py
Purpose     : Test script to push data into kafka for consumption by the example
network interface data processing application using flink streaming. Not intended
for any kind of serious purpose.

usage: python src.py <kafka_broker> <NUMBER_OF_RECORDS> <TOTAL_HOSTS> <INTERFACES_PER_HOST>
e.g.: python src.py 192.168.12.24:9092 50 1 1

Author      : PNDA team
Created     : 21/05/2018
Copyright (c) 2018 Cisco and/or its affiliates.
This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
The code, technical concepts, and all information contained herein, are the property of
Cisco Technology, Inc. and/or its affiliated entities, under various laws including copyright,
international treaties, patent, and/or contract. Any use of the material herein must be in
accordance with the terms of the License. All rights not expressly granted by the License are
reserved. Unless required by applicable law or agreed to separately in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied.
"""
import io
import os
import sys
import json
import random
import time
import avro.schema
import avro.io
from kafka import KafkaProducer

# Path to user AVRO schema file
HERE = os.path.abspath(os.path.dirname(__file__))
SCHEMA_PATH = HERE + "/dataplatform-raw.avsc"

# Kafka topic
TOPIC = "avro.pnda.netmeter"

# lambda functions
CURRENT_MILLI_TIME = lambda: int(round(time.time() * 1000))


def run(brokers, record_count, host_count, int_per_host):
    """
    Run the test
    """
    schema = avro.schema.parse(open(SCHEMA_PATH).read())
    producer = KafkaProducer(bootstrap_servers=[brokers])
    writer = avro.io.DatumWriter(schema)

    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    extra_bytes = bytes('')

    all_hosts = dict()
    for x_host in range(1, host_count + 1):
        all_hosts['host' + str(x_host)] = dict()
        current_host = all_hosts['host' + str(x_host)]

        # Create Interface
        for x_int in range(int_per_host):
            current_host['eth' + str(x_int)] = {}
            current_int = current_host['eth' + str(x_int)]

            # Add Stats
            current_int['RX-Bytes'], current_int['TX-Bytes'] = 0, 0
            current_int['upload-speed-bytes'], current_int['download-speed-bytes'] = 75, 100

    # Triggers 1 records per second
    records_per_second = 1.0

    # Network fluctuation Configuration
    network_fluctuation = [i for i in range(-20, 21, 5)]

    # Generate Records
    start_time = time.time()
    for _ in range(0, record_count):
        time_sec = CURRENT_MILLI_TIME()

        # Random host and interface selection
        host_name = random.choice(all_hosts.keys())
        interface_name = random.choice(all_hosts.get(host_name).keys())

        # Select the interface to generate data
        interface = all_hosts.get(host_name).get(interface_name)

        # Generate random stable/increase/decrease(0,5,10,15,20) in network speed
        interface['upload-speed-bytes'] += random.choice(network_fluctuation)
        interface['download-speed-bytes'] += random.choice(network_fluctuation)

        if interface['upload-speed-bytes'] < 0:
            interface['upload-speed-bytes'] = 0
        if interface['download-speed-bytes'] < 0:
            interface['download-speed-bytes'] = 0

        # Generate received and transmitted bytes
        interface['RX-Bytes'] += long(interface['download-speed-bytes'] * records_per_second)
        interface['TX-Bytes'] += long(interface['upload-speed-bytes'] * records_per_second)

        data = {"hostname": host_name, "nw_interface": interface_name,
                "RX_Bytes": interface.get('RX-Bytes'), "TX_Bytes": interface.get('TX-Bytes'),
                "timestamp": time_sec}

        writer.write({"source": "hndu-src", "timestamp": time_sec, "rawdata": json.dumps(data)}
                     , encoder)

        raw_bytes = bytes_writer.getvalue()

        # reset buffer to start index
        bytes_writer.seek(0)

        producer.send(TOPIC, extra_bytes + raw_bytes)
        print({"source": "hndu-src", "rawdata": json.dumps(data)})

        time.sleep(1.0 / records_per_second)
    print "Time Taken for execution : %s SECONDS" % (time.time() - start_time)


if __name__ == '__main__':

    BROKER_LIST = None
    if len(sys.argv) != 5:
        print "Usage: python src.py <kafka_broker:port> NUMBER_OF_RECORDS " \
              "TOTAL_HOSTS INTERFACES_PER_HOST"
        print "Ex - python src.py 192.168.12.24:9092 100 1 1"
        sys.exit(1)

    BROKER_LIST, TOTAL_RECORDS = sys.argv[1], int(sys.argv[2])
    TOTAL_HOSTS, INTERFACES_PER_HOST = int(sys.argv[3]), int(sys.argv[4])
    run(BROKER_LIST, TOTAL_RECORDS, TOTAL_HOSTS, INTERFACES_PER_HOST)

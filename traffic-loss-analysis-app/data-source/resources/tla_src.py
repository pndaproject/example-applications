"""
Name:       tla-src.py
Purpose:    Test script to push data into kafka for consumption by the example traffice loss analysis app.
            Not intended for any kind of serious purpose.
            usage: src.py kafka_broker num_to_send
             e.g.: src.py 192.168.12.24 250
Author:     PNDA team
Created:    25/04/2018
Copyright (c) 2018 Cisco and/or its affiliates.

This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc.
and/or its affiliated entities, under various laws including copyright, international treaties, patent, and/or contract.
Any use of the material herein must be in accordance with the terms of the License. All rights not expressly granted by the License are reserved.

Unless required by applicable law or agreed to separately in writing, software distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

import io
import random
import json
import sys
import time
import logging
import avro.schema
import avro.io
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

logging.basicConfig(
	   format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
	   level=logging.DEBUG)
kafka = KafkaClient(sys.argv[1])
producer = SimpleProducer(kafka)

# Path to user.avsc avro schema
schema_path = "./dataplatform-raw.avsc"

# Kafka topic
topic = "avro.tla.metrics"
schema = avro.schema.parse(open(schema_path).read())

# Network topology
hosts = ['IOS-xrv9k-1', 'IOS-xrv9k-2']
infs = ['GigabitEthernet0/0/0/1', 'GigabitEthernet0/0/0/2']
topo = dict(zip(hosts, infs))
metrics = ['interface.packets-sent', 'interface.packets-received']

# lambda functions
current_milli_time = lambda: int(round(time.time() * 1000))
random_packets = lambda: random.randint(180000, 390000)
pkt_drop_percent = lambda: random.uniform(0.2, 0.4)
seq = 0
num = 12 # nubmer of continuous packet drops
metric_cache = [0, 0]

while seq < int(sys.argv[2]):
    num_pks = random_packets()
    metric_cache[0] += num_pks
    ts = current_milli_time()
    for k, v in topo.iteritems():
        for m in metrics:
            data = {'metric': m,
		    'tags': {'host': k,
		        'interface-name': v},
		    'timestamp': ts,
		    'value': metric_cache[0]}
            if ((seq % 120)) < num and (m == 'interface.packets-sent') and (k == 'IOS-xrv9k-2'):
	        metric_cache[1] += int(round(num_pks * 0.9))
		data['value'] = metric_cache[1]
            else:
		data['value'] = metric_cache[0]

            # wrap data in avro format and send to kafka
            writer = avro.io.DatumWriter(schema)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write({"src": "tla-src", "timestamp": ts, "host_ip": k, "rawdata": json.dumps(data)}, encoder)
            raw_bytes = bytes_writer.getvalue()
            producer.send_messages(topic, raw_bytes)

    seq += 1
    print seq
    time.sleep(30)
	


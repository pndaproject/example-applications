"""
Name:       src.py
Purpose:    Test script to push data into kafka for consumption by the example spark streaming app.
            Not intended for any kind of serious purpose.
            usage: src.py kafka_broker num_to_send
             e.g.: src.py 192.168.12.24 250
Author:     PNDA team

Created:    07/04/2016

Copyright (c) 2016 Cisco and/or its affiliates.

This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc. and/or its affiliated entities, under various laws including copyright, international treaties, patent, and/or contract. Any use of the material herein must be in accordance with the terms of the License. All rights not expressly granted by the License are reserved.

Unless required by applicable law or agreed to separately in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""

import io
import os
import sys
import time
import logging
import avro.schema
import avro.io
from kafka import KafkaProducer

if len(sys.argv[1:]) != 2:
    print 'Usage: src.py kafka_broker num_to_send'
    sys.exit(1)

logging.basicConfig(format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s', level=logging.DEBUG)
kafka = map(str, sys.argv[1].split(','))
producer = KafkaProducer(bootstrap_servers=kafka)

# Avro schema
here = os.path.abspath(os.path.dirname(__file__))
schema_path = here + "/dataplatform-raw.avsc"

# Kafka topic
topic = "avro.events.samples"
schema = avro.schema.parse(open(schema_path).read())

current_milli_time = lambda: int(round(time.time() * 1000))

seq = 0

while seq < int(sys.argv[2]):
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write({"src": "test-src", "timestamp": current_milli_time(), "host_ip": "0.0.0.0",
                  "rawdata": "a=1;b=2;c=%s;gen_ts=%s"%(seq, current_milli_time())}, encoder)
    raw_bytes = bytes_writer.getvalue()
    producer.send(topic, raw_bytes)
    seq += 1

producer.close()

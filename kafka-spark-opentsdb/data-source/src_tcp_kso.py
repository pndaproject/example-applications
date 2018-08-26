"""
Copyright (c) 2016 Cisco and/or its affiliates.

This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
The code, technical concepts, and all information contained herein, are the property of
Cisco Technology, Inc. and/or its affiliated entities, under various laws including copyright,
international treaties, patent, and/or contract. Any use of the material herein must be in
accordance with the terms of the License.
All rights not expressly granted by the License are reserved.

Unless required by applicable law or agreed to separately in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied.


Purpose: Example script to drive data compatible with example application over TCP, can be used
         with Logstash and Avro/Kafka plugins to deliver data into PNDA

"""

import time
from time import gmtime, strftime
import socket
import random

SOCK = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
SOCK.connect(('127.0.0.1', 20519))

while True:
    for i in range(0, 9):
        collectd_value = random.randint(0, 100)
        collectd_alea = "{'host':'pnda" + str(i) + "','collectd_type':'cpu','value':'" + \
                    str(collectd_value) + "','timestamp':'" + \
                    strftime("%Y-%m-%dT%H:%M:%S.000Z", gmtime()) + "'}\n"

        SOCK.send(collectd_alea)
        print collectd_alea
        time.sleep(1)

        collectd_value = random.randint(0, 4000000)
        collectd_alea = "{'host':'pnda" + str(i) + "','collectd_type':'memory','value':'" + \
            str(collectd_value) + "','timestamp':'" + \
            strftime("%Y-%m-%dT%H:%M:%S.000Z", gmtime()) + "'}\n"

        SOCK.send(collectd_alea)
        print collectd_alea
        time.sleep(1)

    time.sleep(5)

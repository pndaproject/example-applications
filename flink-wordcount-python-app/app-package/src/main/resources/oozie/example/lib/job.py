#
# Name:       job.py
# Purpose:    Create and start a Flink batch job to count the words in input file and write output to an another file.
# Author:     PNDA team
#
# Created:    13/04/2018
#

#
# Copyright (c) 2018 Cisco and/or its affiliates.
#
# This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc.
# and/or its affiliated entities, under various laws including copyright, international treaties, patent,
# and/or contract. Any use of the material herein must be in accordance with the terms of the License.
# All rights not expressly granted by the License are reserved.
#
# Unless required by applicable law or agreed to separately in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied.
#


import sys

from flink.plan.Environment import get_environment
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.plan.Constants import WriteMode

'''

Simple example wordwount program, This will read text from one input file
and produce wordcount output to a file.

Example Usage: - hdfs://path/to/input_file hdfs://path/to/output_file

'''

class Aggregate(GroupReduceFunction):
    def reduce(self, iterator, collector):
        count, word = iterator.next()
        count += sum([x[0] for x in iterator])
        collector.collect((count, word))

if __name__ == "__main__":


    # get environment
    env = get_environment()

    # check if user passed an input file to process
    if len(sys.argv) == 3:
        data = env.read_text(sys.argv[1])
    else:
        data = env.from_elements("this is an example text, it will be used if user does not provide input file")


    # map each word into a tuple, then flat map across that, and group by the key, and aggregate on it
    result = data.flat_map(lambda x, c: [(1, word) for word in x.lower().split()]) \
        .group_by(1) \
        .reduce_group(Aggregate(), combinable=True) \
        .map(lambda y: '%s, %s' % (y[1], y[0]))

    # check if user passed an output file to write output
    if len(sys.argv) == 3:
        result.write_text(sys.argv[2], write_mode=WriteMode.OVERWRITE)
    else:
        result.output()

    # execute the plan
    env.execute()


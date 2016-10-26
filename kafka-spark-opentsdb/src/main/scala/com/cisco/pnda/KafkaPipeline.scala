/**
  * Name:       KafkaPipeline
  * Purpose:    Set up the spark streaming processing graph.
  * Author:     PNDA team
  *
  * Created:    07/04/2016
  */
  
/*
Copyright (c) 2016 Cisco and/or its affiliates.

This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc.
and/or its affiliated entities, under various laws including copyright, international treaties, patent,
and/or contract. Any use of the material herein must be in accordance with the terms of the License.
All rights not expressly granted by the License are reserved.

Unless required by applicable law or agreed to separately in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied.
*/

package com.cisco.pnda;

import com.cisco.pnda._
import com.cisco.pnda.model._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.log4j.Logger;

class KafkaPipeline extends Serializable {

    object Holder extends Serializable {
       @transient lazy val logger = Logger.getLogger(getClass.getName)
    }

    def create() = {

        val parseMessages = (messagesDstream: DStream[DataPlatformEvent]) => {

            val parsedMessages = messagesDstream.flatMap(dataPlatformEvent => {
            val parsed = dataPlatformEvent.getRawdata();
            Some(parsed);
            });
            parsedMessages
        }: DStream[String];

        val props = AppConfig.loadProperties();
        val checkpointDirectory = props.getProperty("app.checkpoint_path");
        val batchSizeSeconds = Integer.parseInt(props.getProperty("app.batch_size_seconds"));

        val sparkConf = new SparkConf();
        Holder.logger.info("Creating new spark context with checkpoint directory: " + checkpointDirectory)
        val ssc = new StreamingContext(sparkConf, Seconds(batchSizeSeconds));

        if (checkpointDirectory.length() > 0) {
            ssc.checkpoint(checkpointDirectory);
        }

        val inputStream = new KafkaInput().readFromKafka(ssc);
        val parsedStream = parseMessages(inputStream);
        val writeCounts: DStream[Integer] =
        
        new OpenTSDBOutput().putOpentsdb(
            props.getProperty("opentsdb.ip"),
            parsedStream);

        writeCounts.reduce(_ + _).print(1);
        ssc;
    }: StreamingContext
}

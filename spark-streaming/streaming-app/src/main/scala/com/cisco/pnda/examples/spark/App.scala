/**
  * Name:       App
  * Purpose:    Application entry point to create, configure and start spark streaming job.
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

package com.cisco.pnda.examples.spark;

import com.cisco.pnda.examples.spark._
import com.cisco.pnda.examples.spark.model._

import org.apache.spark.streaming.StreamingContext

import org.apache.log4j.Logger;

object App {

    private[this] val logger = Logger.getLogger(getClass().getName());

    def main(args: Array[String]) = {
        val props = AppConfig.loadProperties();
        val loggerUrl = props.getProperty("environment.metric_logger_url")
        val appName = props.getProperty("component.application")
        val checkpointDirectory = props.getProperty("component.checkpoint_path");
        val batchSizeSeconds = Integer.parseInt(props.getProperty("component.batch_size_seconds"));
        val pipeline = new KafkaPipeline()
        // Create the streaming context, or load a saved one from disk
        val ssc = if (checkpointDirectory.length() > 0) StreamingContext.getOrCreate(checkpointDirectory, pipeline.create) else pipeline.create();

        sys.ShutdownHookThread {
            logger.info("Gracefully stopping Spark Streaming Application")
            ssc.stop(true, true)
            logger.info("Application stopped")
        }

        logger.info("Starting spark streaming execution")
        logger.info("Logger url: " + loggerUrl)
        ssc.addStreamingListener(new StatReporter(appName, loggerUrl))
        ssc.start()
        ssc.awaitTermination()
  }
}

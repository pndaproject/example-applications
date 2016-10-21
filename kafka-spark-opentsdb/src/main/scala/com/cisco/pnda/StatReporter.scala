/**
  * Name:       StatReporter
  * Purpose:    Report batch processing metrics to the PNDA metric logger
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

package com.cisco.pnda

import scala.util.control.NonFatal
import java.io.StringWriter
import java.io.PrintWriter
import org.apache.spark.streaming.scheduler.StreamingListener
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted
import org.apache.log4j.Logger
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient

class StatReporter(appName: String, metricsUrl: String) extends StreamingListener {

    private[this] val logger = Logger.getLogger(getClass().getName())

    override def onBatchCompleted(batch: StreamingListenerBatchCompleted) = {
        def doSend(metricName: String, metricValue: String) = {
            try {
                val httpClient = new DefaultHttpClient()
                val post = new HttpPost(metricsUrl)
                post.setHeader("Content-type", "application/json")
                val ts = java.lang.System.currentTimeMillis()
                val body = f"""{
                    |    "data": [{
                    |        "source": "application.$appName",
                    |        "metric": "application.kpi.$appName.$metricName",
                    |        "value": "$metricValue",
                    |        "timestamp": $ts%d
                    |    }],
                    |    "timestamp": $ts%d
                    |}""".stripMargin

                logger.debug(body)
                post.setEntity(new StringEntity(body))
                val response = httpClient.execute(post)
                if (response.getStatusLine.getStatusCode() != 200) {
                    logger.error("POST failed: " + metricsUrl + " response:" + response.getStatusLine.getStatusCode())
                }

            } catch {
                case NonFatal(t) => {
                    logger.error("POST failed: " + metricsUrl)
                    val sw = new StringWriter
                    t.printStackTrace(new PrintWriter(sw))
                    logger.error(sw.toString)
                }
            }
        }
        doSend("processing-delay", batch.batchInfo.processingDelay.get.toString())
        doSend("scheduling-delay", batch.batchInfo.schedulingDelay.get.toString())
    }
}

/**
  * Name:       OpenTSDBOutput
  * Purpose:    Write a dstream to OpenTSDB
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

import org.joda.time.DateTime
import scala.util.control.NonFatal
import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

class OpenTSDBOutput extends Serializable {


  def putOpentsdb[T](opentsdbIP: String,
      stream: DStream[String]) = {
    stream.mapPartitions(partition => {
      var count = 0;
      partition.foreach(rowData =>
        {

          val json = parse(rowData.replace("'", "\""))
          val host = compact(render((json \\ "host"))).replace("\"", "")
          val timestampStr = compact(render((json \\ "timestamp"))).replace("\"", "")
          val value = (compact(render((json \\ "value"))).replace("\"", "")).toDouble
          val collectd_type = compact(render((json \\ "collectd_type"))).replace("\"", "")
          var metric:String = "kso.collectd"
          metric = metric.concat("." + collectd_type)
          val timestamp = new DateTime(timestampStr).getMillis
          val body = f"""{
                    |        "metric": "$metric",
                    |        "value": "$value",
                    |        "timestamp": $timestamp,
                    |        "tags": {"host": "$host"}
                    |}""".stripMargin

          var openTSDBUrl = "http://" + opentsdbIP + "/api/put"
          try {
                val httpClient = new DefaultHttpClient()
                val post = new HttpPost(openTSDBUrl)
                post.setHeader("Content-type", "application/json")
                post.setEntity(new StringEntity(body))
                httpClient.execute(post)

            } catch {
                case NonFatal(t) => {
                    
                }
            }

          count += 1
        });
      Iterator[Integer](count)
    });
  }
}
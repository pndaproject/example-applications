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

import scalaj.http._
import java.nio.charset.Charset
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import java.sql.Timestamp
import org.apache.spark.streaming.dstream.DStream

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
          val value = compact(render((json \\ "value"))).replace("\"", "")
          val collectd_type = compact(render((json \\ "collectd_type"))).replace("\"", "")
          var metric:String = "kso.collectd"
          metric = metric.concat("." + collectd_type)
          val timestamp = Timestamp.valueOf(timestampStr.replace("T"," ").replace("Z","")).getTime
          var tags = Map[String, String]("host" -> host)

          var post_json = ("metric" -> metric) ~
            ("timestamp" -> timestamp) ~
            ("value" -> value.toDouble) ~
            ("tags" -> tags.map { case(akey, avalue) =>   (akey -> avalue) } )

          var result = Http("http://" + opentsdbIP + "/api/put").postData(compact(render(post_json)))
            .header("Content-Type", "application/json")
            .header("Charset", "UTF-8")
            .timeout(connTimeoutMs = 10000, readTimeoutMs = 100000).asString
          count += 1
        });
      Iterator[Integer](count)
    });
  }
}
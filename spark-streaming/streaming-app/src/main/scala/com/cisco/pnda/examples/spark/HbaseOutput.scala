/**
  * Name:       HbaseOutput
  * Purpose:    Write a dstream to HBase
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

import org.apache.spark.streaming.dstream.DStream
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.{ HBaseAdmin, HTable, Put, Get }
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConversions._
import java.util.ArrayList;

class HbaseOutput extends Serializable {
   def writeToHbase[T](zk: String,
      shards: Int,
      tableName: String,
      columnFamily: String,
      hadoopDistro: String,
      rowKey: String,
      stream: DStream[Map[String,String]]) = {
    stream.mapPartitions(partition => {
      // For higher hbase throughput
      // obtain connections from a static pool
      val hbaseConfiguration = new HBaseConfiguration();
      hbaseConfiguration.set("hbase.zookeeper.quorum", zk);
      var hbasePath = "";
      if (hadoopDistro == "HDP") {
          hbasePath = "/hbase-unsecure";
      }
      else {
          hbasePath = "/hbase";
      }
      hbaseConfiguration.set("zookeeper.znode.parent", hbasePath)
      val admin = new HBaseAdmin(hbaseConfiguration);
      val table = new HTable(admin.getConfiguration(), tableName);
      val puts = new ArrayList[Put]();

      var shard = 0;
      var count = 0;
      partition.foreach(rowData =>
        {
          val rowId = rowData.get(rowKey).get.toString();
          val hBasePutRow = new Put(Bytes.toBytes(shard + "_" + rowId.toString()));
          shard = shard + 1;
          if (shard >= shards) {
            shard = 0
          }
          val serializedColumnFamilyName = Bytes.toBytes(columnFamily);
          rowData.foreach {
            case (col, value) =>
              val serializedValue = Bytes.toBytes(value.toString);
              hBasePutRow.add(serializedColumnFamilyName, Bytes.toBytes(col), serializedValue);
          }
          puts.add(hBasePutRow);
          count += 1
          if (puts.length > 500) {
            table.put(puts);
            table.flushCommits();
            puts.clear();
          }
        });

      table.put(puts);
      table.flushCommits();

      table.close();
      admin.close();
      Iterator[Integer](count)
    });
  }
}

/**
  * Name:       KuduOutput
  * Purpose:    Write a dstream to kudu
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
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.client._;
import scala.collection.JavaConversions._
import java.util.ArrayList;

class KuduOutput extends Serializable {
   def writeToKudu[T](kuduMaster: String,
      tableName: String,
      stream: DStream[Map[String,String]]) = {
        stream.mapPartitions(partition => {
            // For higher kudu throughput
            // obtain connections from a static pool

            // connect
            val client = new KuduClient.KuduClientBuilder(kuduMaster).build();

            val num_columns = 4
            val columns = new ArrayList[ColumnSchema](num_columns);
            columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING).key(true).build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("c", Type.STRING).build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("gen_ts", Type.STRING).build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("proc_ts", Type.STRING).build());
            val schema = new Schema(columns)
            try {
                client.createTable(tableName, schema, new CreateTableOptions().setNumReplicas(1));
            }
            catch { case _: Throwable => }

            val table = client.openTable(tableName);
            val session = client.newSession();

            var count = 0;

            partition.foreach(rowData => {
                // write
                count += 1
                val insert = table.newInsert();
                val row = insert.getRow();
                row.addString(0, rowData.get("rowId").get.toString());
                row.addString(1, rowData.get("c").get.toString());
                row.addString(2, rowData.get("gen_ts").get.toString());
                row.addString(3, rowData.get("proc_ts").get.toString());

                // Should apply in batches of more than 1?
                session.apply(insert);
            });

            // flush and close
            client.shutdown();

            Iterator[Integer](count)
        });
    }
}

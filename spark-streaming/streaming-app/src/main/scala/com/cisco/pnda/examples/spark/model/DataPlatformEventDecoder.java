/**
  * Name:       DataPlatformEventDecoder
  * Purpose:    Encodes and secodes binary event data from kafka to/from a DataPlatformEvent object
  * Author:     PNDA team
  *
  * Created:    07/04/2016
  */

/*
Copyright (c) 2016 Cisco and/or its affiliates.
 
This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 
The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc. and/or its affiliated entities, under various laws including copyright, international treaties, patent, and/or contract. Any use of the material herein must be in accordance with the terms of the License. All rights not expressly granted by the License are reserved.
 
Unless required by applicable law or agreed to separately in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*/

package com.cisco.pnda.examples.spark.model;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.log4j.Logger;

public class DataPlatformEventDecoder 
{
    private static final Logger LOGGER = Logger.getLogger(DataPlatformEventDecoder.class.getName());

    private Schema.Parser _parser = new Schema.Parser();
    private DatumReader<GenericRecord> _reader;
    private DatumWriter<GenericRecord> _writer;
    private Schema _schema;
    private String _schemaDef;
    
    public DataPlatformEventDecoder(String schemaDef) throws IOException
    {
        _schemaDef = schemaDef;
        _schema = _parser.parse(schemaDef);
        _reader = new GenericDatumReader<GenericRecord>(_schema);
        _writer = new GenericDatumWriter<GenericRecord>(_schema);
    }

    public DataPlatformEvent decode(byte[] data) throws IOException
    {
        
        try
        {
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        GenericRecord r = _reader.read(null, decoder);
        return new DataPlatformEvent((String) r.get("src").toString(),
                (Long) r.get("timestamp"),
                (String) r.get("host_ip").toString(),
                new String(((ByteBuffer)r.get("rawdata")).array()));
        }
        catch(Exception ex)
        {
            LOGGER.error("Failed to decode event", ex);
            throw new IOException(ex);
        }
    }
    
    public byte[] encode(DataPlatformEvent e) throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericRecord datum = new GenericData.Record(_schema);
        datum.put("src", e.getSrc());
        datum.put("timestamp", e.getTimestamp());
        datum.put("host_ip", e.getHostIp());
        datum.put("rawdata", ByteBuffer.wrap(e.getRawdata().getBytes("UTF-8")));
        _writer.write(datum, encoder);
        encoder.flush();
        out.close();
        return out.toByteArray();
    }
}
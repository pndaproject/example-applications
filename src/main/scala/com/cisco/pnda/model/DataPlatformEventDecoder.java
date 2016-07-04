/**
  * Name:       DataPlatformEventDecoder
  * Purpose:    Encodes and secodes binary event data from kafka to/from a DataPlatformEvent object
  * Author:     PNDA team
  *
  * Created:    07/04/2016
  */

package com.cisco.pnda.model;

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
            LOGGER.error("data:" + hexStr(data) + " schema: " + _schemaDef, ex);
            throw new IOException(ex);
        }
    }
    
    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();
    public static String hexStr(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
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
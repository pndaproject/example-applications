/**
    * Name:       AvroSchemaSerialization
    * Purpose:    Serialize the data in AVRO Format with provided schemas.
*/

package com.cisco.pnda.examples.util;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@SuppressWarnings("deprecation")
public class AvroSchemaSerialization<T> implements SerializationSchema<T> {

	private static final long serialVersionUID = 1L;
	private final Class<T> avroType;
	private transient GenericDatumWriter<T> writer;
	private transient BinaryEncoder encoder;

	public AvroSchemaSerialization(Class<T> avroType) {
		this.avroType = avroType;
	}

	@Override
	public byte[] serialize(T obj) {
		ensureInitialized();

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		encoder = EncoderFactory.get().binaryEncoder(out, null);
		byte[] serializedBytes = null;
		try {
			writer.write(obj, encoder);
			encoder.flush();
			serializedBytes = out.toByteArray();
			out.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return serializedBytes;
	}

	private void ensureInitialized() {
		if (writer == null) {
			if (org.apache.avro.specific.SpecificRecordBase.class.isAssignableFrom(avroType)) {
				writer = new SpecificDatumWriter<T>(avroType);
			} else {
				writer = new ReflectDatumWriter<T>(avroType);
			}
		}
	}
}

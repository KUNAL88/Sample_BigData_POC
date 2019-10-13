package com.kunal.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.ByteBufferInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AvroUtil {

    static Schema stringTypeSchema;

    public static Schema createSchemafromAvroFile(ClassLoader classLoader,String avroFile) throws IOException {

        InputStream inputStream=classLoader.getResourceAsStream(avroFile);

        Schema schema;
        try {
            schema=(new Schema.Parser()).parse(inputStream);
        }finally {
            if(inputStream!=null){
                inputStream.close();
            }
        }

        return schema;
    }

    public static <T> T deserialize(DecoderFactory decoderFactory,Schema writerSchema,
                                    Schema readerSchema,byte[] data,int offset,int payloadLen) throws IOException {
        DatumReader<T> reader=new SpecificDatumReader<>(writerSchema,readerSchema);
        BinaryDecoder decoder=decoderFactory.binaryDecoder(data,offset,payloadLen, null);
        T o=reader.read(null,decoder);
        return o;
    }

    public static <T> T deserializeWithSchema(Class<T> dtoClass,byte[] data) throws IOException {
        DatumReader<T> datumReader=new SpecificDatumReader<>(dtoClass);
        return deserializeWithSchema(datumReader,data);
    }

    public static <T> T deserializeWithSchema(DatumReader<T> datumReader,byte[] data) throws IOException {
        ByteBuffer bb=ByteBuffer.wrap(data);
        List<ByteBuffer> list=new ArrayList<>(1);
        list.add(bb);
        ByteBufferInputStream bis=new ByteBufferInputStream(list);
        DataFileStream dataFileStream=new DataFileStream(bis,datumReader);

        T var1;

        try {
            var1=(T)dataFileStream.next();

        }finally {
            dataFileStream.close();
        }

        return var1;
    }
}

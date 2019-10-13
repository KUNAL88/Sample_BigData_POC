package com.kunal.avro;

import com.kunal.ingestion.schema.avro.RawMessage;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class AvroDeserializer {

    public static void main(String[] args) throws IOException {

        byte[] data= Files.readAllBytes(new File("/home/kunal/sample_bigdata_poc/Sample_BigData_POC/bigdata-poc/common-data-objects/src/resources/rawfile/RawMessage.avro").toPath());
        RawMessage rawMessage=AvroUtil.deserializeWithSchema(RawMessage.class,data);

    }
}

package com.kunal.avro;


import com.kunal.ingestion.schema.avro.Person;
import com.kunal.ingestion.schema.avro.RawMessage;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;

public class AvroDataProducer {

    public AvroDataProducer(){

    }

    public static void main(String[] args) {
        new AvroDataProducer().createDataList();//deserializeData();//.createDataList();
    }

    private void createDataList(){
        DatumWriter<RawMessage> datumWriter=new SpecificDatumWriter<RawMessage>(RawMessage.class);
        DataFileWriter<RawMessage> dataFileWriter=new DataFileWriter<RawMessage>(datumWriter);
        try {
            dataFileWriter.create(RawMessage.SCHEMA$,new File("/home/kunal/sample_bigdata_poc/Sample_BigData_POC/bigdata-poc/common-data-objects/src/main/resources/rawfile/RawMessage.avro"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        for(int i=1;i<10;i++){
            Person p=new Person();
            p.setId(i);
            p.setUsername("ABC_0"+i);
            p.setEmailAddress("abc@gmail.com");
            p.setPhoneNumber("1234567890");
            p.setFirstName("ABC");
            p.setLastName("XYZ");
            p.setJoinDate("2011-08-08");
            p.setMiddleName("PQR");
            p.setSex("M");
            p.setBirthdate("1988-01-0"+i);
            p.setJoinDate("2001-01-01");
            p.setPreviousLogins(i);
            p.setLastIp("127.0.0.1");

           byte[] bytes= serializeToByte(p);

            RawMessage rawMessage=new RawMessage();
            rawMessage.setDataBlob(ByteBuffer.wrap(bytes));
            rawMessage.setPublishTs(1L);
            rawMessage.setSource("ABC@localhost");
            rawMessage.setCountry("IN");
            rawMessage.setCity("BLR");

            try {
                dataFileWriter.append(rawMessage);
                dataFileWriter.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private byte[] serializeToByte(Person person){

        ByteArrayOutputStream byteOutputStream=new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream=null;

        try {
            objectOutputStream=new ObjectOutputStream(byteOutputStream);
            objectOutputStream.writeObject(person);
            objectOutputStream.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return byteOutputStream.toByteArray();
    }

    private Person deSerializeFromByte(byte[] bytes){

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput in = null;
        Person person =null;
        try {
            in = new ObjectInputStream(bis);
            person = (Person) in.readObject();
        } catch (IOException e){
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return person;
    }

    public void deserializeData(){

        DatumReader<RawMessage> datumReader=new SpecificDatumReader<RawMessage>(RawMessage.class);
        try {
            DataFileReader dataFileReader=new DataFileReader(new File("./src/resources/rawfile/RawMessage.avro"),datumReader);
            RawMessage rawMessage=null;

            while (dataFileReader.hasNext()){
                rawMessage=(RawMessage) dataFileReader.next();
               // System.out.println(rawMessage);
                Person p=this.deSerializeFromByte(rawMessage.getDataBlob().array());
                System.out.println(p.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

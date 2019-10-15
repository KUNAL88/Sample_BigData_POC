package com.kunal.producer;

import com.kunal.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

public class AvroProducer {

    public static void main(String[] args) throws IOException, InterruptedException {

        if(args.length<1){
            System.out.println("Missing Producer Config ... ");
            System.exit(0);
        }

        Config config=new Config(args[0]);
        Properties prop=config.getProducerConfig();
        String topicName=config.getInputTopicName();
        Producer<String,byte[]> producer=new KafkaProducer<String, byte[]>(prop);

        for(int i=0;i<100;i++){
            byte[] data= Files.readAllBytes(new File("/home/kunal/sample_bigdata_poc/Sample_BigData_POC/bigdata-poc/" +
                    "common-data-objects/src/resources/rawfile/RawMessage.avro").toPath());
            producer.send(new ProducerRecord<String, byte[]>(topicName,null,data));

            Thread.sleep(1000);
        }

    }

}

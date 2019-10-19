package com.kunal.consumer;

import com.kunal.avro.AvroUtil;
import com.kunal.config.Config;
import com.kunal.ingestion.schema.avro.RawMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class AvroConsumer {

    public static void main(String[] args) throws IOException, InterruptedException {


        if(args.length<1){
            System.out.println("Missing Consumer Config ... ");
            System.exit(0);
        }

        Config config=new Config(args[0]);
        Properties prop=config.getConsumerConfig();
        String topicName=config.getInputTopicName();

        KafkaConsumer<String,byte[]> consumer=new KafkaConsumer<String, byte[]>(prop);
        consumer.subscribe(Arrays.asList(topicName));
        while (true){
            ConsumerRecords<String,byte[]> records=consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,byte[]> record:records){
                byte[] data=record.value();
                RawMessage rawMessage= AvroUtil.deserializeWithSchema(RawMessage.class,data);
                System.out.println(rawMessage.toString());

                Thread.sleep(1000);
            }
        }
    }

}

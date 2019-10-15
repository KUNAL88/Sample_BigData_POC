package com.kunal.config;

import com.kunal.util.Constants;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Config {

    private Properties commonConfig;
    private String inputTopicName,outputTopicName;

    public Config(String configFilePath) throws IOException {

        FileInputStream fis=new FileInputStream(new File(configFilePath));
        commonConfig=new Properties();
        commonConfig.load(fis);
        inputTopicName=commonConfig.getProperty(Constants.INPUT_TOPIC);
        outputTopicName=commonConfig.getProperty(Constants.OUTPUT_TOPIC);
    }

    public Properties getProducerConfig(){
        Properties prop=new Properties();

        prop.put("bootstrap.servers",commonConfig.getProperty(Constants.BROKER_SOURCE));
        prop.put("group.id",commonConfig.getProperty(Constants.CONSUMER_GROUP));
       /* prop.put("acks","all");
        prop.put("retries",0);
        prop.put("batch.size",1633384);
        prop.put("linger.ms",1);
        prop.put("buffer.memory",545544);*/
        prop.put("key.serializer",commonConfig.getProperty(Constants.KEY_SERIALIZER));
        prop.put("value.serializer",commonConfig.getProperty(Constants.VALUE_SERIALIZER));

        return prop;
    }

    public Properties getConsumerConfig(){
        Properties prop=new Properties();

        prop.put("bootstrap.servers",commonConfig.getProperty(Constants.BROKER_SOURCE));
        prop.put("group.id",commonConfig.getProperty(Constants.CONSUMER_GROUP));
        prop.put("acks","all");
        prop.put("retries",0);
        prop.put("batch.size",1024);
        prop.put("linger.ms",1);
        prop.put("buffer.memory",545544);
        prop.put("key.deserializer",commonConfig.getProperty(Constants.KEY_DESERIALIZER));
        prop.put("value.deserializer",commonConfig.getProperty(Constants.VALUE_DESERIALIZER));

        return prop;
    }

    public String getInputTopicName() {
        return inputTopicName;
    }

    public String getOutputTopicName() {
        return outputTopicName;
    }

}

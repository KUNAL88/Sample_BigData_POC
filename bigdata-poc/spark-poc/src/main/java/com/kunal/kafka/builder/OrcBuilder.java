package com.kunal.kafka.builder;

import com.kunal.kafka.KafkaAvroToOrc;
import com.kunal.kafka.config.SparkKafkaConfig;
import org.apache.spark.sql.SparkSession;

public class OrcBuilder {
    private SparkSession sparkSession;
    private SparkKafkaConfig config;

    public OrcBuilder sparkSession(SparkSession sparkSession){
        this.sparkSession=sparkSession;
        return this;
    }

    public KafkaAvroToOrc build(){
        if(sparkSession!=null && config!=null){
            return new KafkaAvroToOrc(sparkSession,config);
        }else {
            throw new IllegalArgumentException("KafkaOrcBuilder not initialized properly");
        }
    }

    public OrcBuilder config(SparkKafkaConfig config){
        this.config=config;
        return this;
    }
}


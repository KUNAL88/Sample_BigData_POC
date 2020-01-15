package com.kunal.kafka;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.kunal.kafka.builder.OrcBuilder;
import com.kunal.kafka.config.SparkKafkaConfig;
import com.kunal.kafka.offset.OffsetManager;
import com.kunal.kafka.offset.ZookeeperOffsetManager;
import org.apache.spark.sql.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaAvroToOrc {

    private SparkSession sparkSession;
    private SparkKafkaConfig sparkKafkaConfig;
    private OffsetManager offsetManager;

    public KafkaAvroToOrc(SparkSession sparkSession,SparkKafkaConfig config){
        this.sparkSession=sparkSession;
        this.sparkKafkaConfig=config;
        this.offsetManager=new ZookeeperOffsetManager(config.getZkHosts(),config.getConsumerGroup());
    }

    public static OrcBuilder builder(){
        return new OrcBuilder();
    }

    public void write(){
        String staringOffsets=getStartingOffsets();

        Dataset<Row> df=sparkSession
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers",sparkKafkaConfig.getKafkaBrokerServers())
                .option("subscribe",sparkKafkaConfig.getKafkaTopic())
                .option("startingOffsets",staringOffsets)
                .option("endingOffsets","latest")
                .load();

        long totalRecords=df.count();
        if(totalRecords>0){
            System.out.println("Total Records: "+df.count());

            System.out.println("Kafka Ending Offsets: ");
            Dataset<Row> endOffsets=df
                    .select("partition","offset")
                    .groupBy("partition")
                    .agg(functions.max("offset")).as("offset");

            endOffsets.show(100,false);

            List<Row> offsets=endOffsets.collectAsList();

            System.out.println("Deserializing records");

            df=df.withColumn("d_value",functions.expr("deserialize(value)"));
            /*df=df.select(functions.expr("d_value[0] as file_sha2")
                    ,functions.expr("d_value[1] as server_ts"),functions.expr("d_value[2] as payload"));
            */

            df=df.select(functions.expr("d_value[0] as data_blob"),
                    functions.expr("d_value[1] as publish_ts"),
                    functions.expr("d_value[2] as source"),
                    functions.expr("d_value[3] as country"),
                    functions.expr("d_value[4] as city"));

            df.show(1);
            System.out.println("Completed deserializing records ...");
            System.out.println("Writing ORC file to dir : "+sparkKafkaConfig.getOrcOutputDir());

            df.write().mode(SaveMode.Overwrite).orc(sparkKafkaConfig.getOrcOutputDir());
            System.out.println("ORC file generated successfully .");

            System.out.println("Saving offsets...");
            writeOffsets(staringOffsets,offsets);
            System.out.println("Offsets saved successfully ...");
        }else {
            System.out.println("No New Data");
        }


    }


    private Map<Integer,Long> getPartitionOffsetMap(List<Row> partitionOffsets){
        Map<Integer,Long> map=new HashMap<>();
        partitionOffsets.forEach(row -> {
            map.put(row.getInt(0),row.getLong(1)+1);
        });
        return map;
    }

    private String getStartingOffsets(){
        String startOffsets=offsetManager.readOffsets(sparkKafkaConfig.getKafkaTopic());

        if(startOffsets==null || startOffsets.isEmpty()){
            startOffsets=getEarliestOffset();
        }

        return startOffsets;
    }

    private String getEarliestOffset(){
        Map<Integer,Long> offsets=new HashMap<>();
        for(int i=0;i<sparkKafkaConfig.getPartitionCount();i++){
            offsets.put(i,-2L);
        }

        Gson gson=new Gson();
        Map<String,Map<Integer,Long>> topicOffsets=new HashMap<>();
        topicOffsets.put(sparkKafkaConfig.getKafkaTopic(),offsets);

        String topicOffsetGson=gson.toJson(topicOffsets);

        return topicOffsetGson;
    }

    private void writeOffsets(String startingOffsets,List<Row> endOffsets){
        Map<Integer,Long> endOffsetMap=getPartitionOffsetMap(endOffsets);

        Gson gson=new Gson();
        JsonObject startOffsetObj=gson
                .fromJson(startingOffsets,JsonObject.class)
                .getAsJsonObject(sparkKafkaConfig.getKafkaTopic());

        for(int partition=0;partition<sparkKafkaConfig.getPartitionCount();partition++){
            if(!endOffsetMap.containsKey(partition)){
                endOffsetMap.put(partition,startOffsetObj.get(Integer.toString(partition)).getAsLong());
            }
        }

        Map<String,Map<Integer,Long>> topicEndOffsetMap=new HashMap<>();
        topicEndOffsetMap.put(sparkKafkaConfig.getKafkaTopic(),endOffsetMap);

        String endOffsetJson=gson.toJson(topicEndOffsetMap);

        offsetManager.writeOffsets(sparkKafkaConfig.getKafkaTopic(),endOffsetJson);
    }
}

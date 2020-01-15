package com.kunal.kafka;

import com.kunal.kafka.builder.OrcBuilder;
import com.kunal.kafka.config.SparkKafkaConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.List;

public class KafkaTool {

    private static final Logger log=Logger.getLogger(KafkaTool.class);

    public static void main(String[] args) {

        if(args.length<1){
            log.warn("Usage: {missing config file}");
            System.exit(0);
        }

        String configFilePath=args[0];

        SparkKafkaConfig sparkKafkaConfig=new SparkKafkaConfig(configFilePath);
        System.out.println(sparkKafkaConfig.toString());

        SparkConf sparkConf=creteSparkConfig(sparkKafkaConfig);
        SparkSession session=createSparkSession(sparkConf,sparkKafkaConfig);

        KafkaAvroToOrc kafkaAvroToOrc=KafkaAvroToOrc
                .builder()
                .sparkSession(session)
                .config(sparkKafkaConfig)
                .build();

        kafkaAvroToOrc.write();

        session.close();
    }

    private static SparkConf creteSparkConfig(SparkKafkaConfig sparkKafkaConfig){

        SparkConf sparkConf=new SparkConf();
        sparkConf.setAppName("KafkaTool_Avro_to_ORC");
        if(sparkKafkaConfig.isLocal()){
            sparkConf.setMaster("local[2]");
        }

        return sparkConf;
    }

    private static SparkSession createSparkSession(SparkConf sparkConf,SparkKafkaConfig config){

        SparkSession session=SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
        Logger.getRootLogger().setLevel(config.getLogLevel());
        return session;
    }

    private static void registerDeserializer(SparkSession sparkSession,String schemaStr){

        sparkSession.udf().register("deserialize", new UDF1<byte[], String[]>() {
            @Override
            public String[] call(byte[] bytes) throws Exception {
                Schema schema=new Schema.Parser().parse(schemaStr);
                DatumReader<GenericRecord> datumReader=new GenericDatumReader<>(schema);
                Decoder decoder=null;
                GenericRecord record=datumReader.read(null,decoder);
                List<String> row=new ArrayList<String>();
                schema.getFields().forEach(field -> row.add(record.get(field.name()).toString()));
                return row.toArray(new String[1]);
            }
        }, DataTypes.createArrayType(DataTypes.StringType));
    }
}

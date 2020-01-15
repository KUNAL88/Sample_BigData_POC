package com.kunal.kafka.config;

import com.kunal.kafka.config.Constants.*;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Properties;

public class SparkKafkaConfig {

    private static final Logger LOG=Logger.getLogger(SparkKafkaConfig.class);

    private Properties prop;
    private boolean local=false;
    private Schema schema;
    private String schemaStr;
    private String kafkaBrokerServers;
    private String kafkaTopic;
    private String orcOutputDir;
    private Level logLevel=Level.WARN;
    private String zkHosts;
    private String consumerGroup;
    private int partitionCount;

    public SparkKafkaConfig(String configPath){

        Configuration config=new Configuration();
        config.addResource(Constants.DEFAULT_CORE_SITE_XML);
        config.addResource(Constants.DEFAULT_HDFS_SITE_XML);

        FileSystem fs=null;
        URI uri=URI.create(configPath);

        try {
        if(configPath.startsWith("file:")){

        }else {

                fs=FileSystem.get(uri,config);

        }

            Path path=new Path(configPath);

            prop=new Properties();
            prop.load(new InputStreamReader(fs.open(path),"UTF-8"));

            fs.close();
            initialize();


        } catch (IOException e) {
            LOG.error(" Error: Loading Config File .....");
        }
    }

    private void initialize(){
        local=Boolean.parseBoolean(prop.getProperty(Constants.LOCAL,"false"));
        String defaultSchema="";
        schemaStr=prop.getProperty(Constants.SCHEMA,defaultSchema);
        schema=new Schema.Parser().parse(schemaStr);
        kafkaBrokerServers=prop.getProperty(Constants.KAFKA_BROKER_SERVERS);
        kafkaTopic=prop.getProperty(Constants.KAFKA_TOPIC);
        orcOutputDir=prop.getProperty(Constants.ORC_OUTPUT_DIR);
        String logLevelObj=prop.getProperty(Constants.LOG_LEVEL);
        logLevel=Level.toLevel(logLevelObj);
        zkHosts=prop.getProperty(Constants.ZK_HOSTS);
        consumerGroup=prop.getProperty(Constants.GROUP_ID);
        partitionCount=getPartitionCount();

        validateConfig();
    }

    private void validateConfig(){

        if(StringUtils.isBlank(kafkaBrokerServers) ||
                StringUtils.isBlank(kafkaTopic) ||
                StringUtils.isBlank(orcOutputDir) ||
                StringUtils.isBlank(zkHosts) ||
                StringUtils.isBlank(consumerGroup)){
            throw new IllegalArgumentException(" Error : Mandatory configs are missing ...");
        }

    }

    public int getPartitionCount(){
        Properties prop=new Properties();

        prop.put("bootstrap.servers",getKafkaBrokerServers());
        prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializar");
        KafkaProducer<String,byte[]> producer=new KafkaProducer<String, byte[]>(prop);
        int count=producer.partitionsFor(getKafkaTopic()).size();
        producer.close();
        return count;
    }

    public boolean isLocal(){
        return local;
    }

    public Schema getSchema() {
        return schema;
    }

    public String getSchemaStr() {
        return schemaStr;
    }

    public String getKafkaBrokerServers() {
        return kafkaBrokerServers;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public String getOrcOutputDir() {
        return orcOutputDir;
    }

    public Level getLogLevel() {
        return logLevel;
    }

    public String getZkHosts() {
        return zkHosts;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }



}

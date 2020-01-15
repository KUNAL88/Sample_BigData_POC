package com.kunal.kafka.offset;

import com.kunal.kafka.config.Constants;
import org.I0Itec.zkclient.ZkClient;

public class ZookeeperOffsetManager  implements OffsetManager{

    private ZkClient zkClient;
    private String consumerId;

    public ZookeeperOffsetManager(String zkHosts,String consumerId){
        zkClient=new ZkClient(zkHosts);
        this.consumerId=consumerId;
        createZkParentPath();
    }

    @Override
    public String readOffsets(String topic) {
        String offsets=zkClient.readData(getTopicPath(topic),true);
        System.out.println("Topic: "+topic+" , Read Offsets: "+offsets);
        return offsets;
    }

    @Override
    public void writeOffsets(String kafkaTopic, String endOffsetJson) {
        if(!zkClient.exists(getTopicPath(kafkaTopic))){
            System.out.println(" Zookeeper Node does not exists, creating new ... ");
            zkClient.createPersistent(getTopicPath(kafkaTopic),true);
        }

        System.out.println("Topic Name :"+kafkaTopic+"Writing offses: "+endOffsetJson);
        zkClient.writeData(getTopicPath(kafkaTopic),endOffsetJson);

    }

    private void createZkParentPath(){
        zkClient.createPersistentSequential(getConsumerPath(),true);
    }

    private String getConsumerPath(){
        return Constants.ZK_PARENT_PATH+"/"+consumerId;
    }


    private String getTopicPath(String topicName){
        return getConsumerPath()+"/"+topicName;
    }
}

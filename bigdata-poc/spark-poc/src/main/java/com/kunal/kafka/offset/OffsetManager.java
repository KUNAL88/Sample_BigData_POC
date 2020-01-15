package com.kunal.kafka.offset;

public interface OffsetManager {

    public String readOffsets(String topic);
    public void writeOffsets(String kafkaTopic,String endOffsetJson);
}

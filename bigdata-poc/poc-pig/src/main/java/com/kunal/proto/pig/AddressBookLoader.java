package com.kunal.proto.pig;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.*;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AddressBookLoader extends LoadFunc implements LoadMetadata {

    private static Logger LOG= LoggerFactory.getLogger(AddressBookLoader.class);

    private RecordReader<Text,Object> recordReader;
    private String schema;

    public AddressBookLoader(String schema){
        this.schema=schema;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job,location);
    }

    @Override
    public InputFormat<Text,Object> getInputFormat() throws IOException {
        return new AddressBookInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader record, PigSplit pigSplit) throws IOException {
        recordReader=record;
    }

    @Override
    public Tuple getNext() throws IOException {

        Tuple t=null;

        try {
            if(recordReader.nextKeyValue()){
                t=(Tuple) this.recordReader.getCurrentValue();
                return t;
            }
        } catch (InterruptedException e) {
            LOG.warn("Error getting next tuple "+e);
        }

        return null;
    }

    @Override
    public ResourceSchema getSchema(String s, Job job) throws IOException {
        return new ResourceSchema(Utils.getSchemaFromString(schema));
    }

    @Override
    public ResourceStatistics getStatistics(String s, Job job) throws IOException {
        return null;
    }

    @Override
    public String[] getPartitionKeys(String s, Job job) throws IOException {
        return null;
    }

    @Override
    public void setPartitionFilter(Expression expression) throws IOException {

    }
}

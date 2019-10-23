package com.kunal.json.pig;

import com.kunal.json.pigutils.EmployeeUtils;
import com.kunal.json.pigutils.EmployeeUtilsImpl;
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

public class EmployeeLoader extends LoadFunc implements LoadMetadata {

    private static final Logger LOG= LoggerFactory.getLogger(EmployeeLoader.class);
    EmployeeUtils employeeUtils;

    private RecordReader<Text,Object> recordReader;
    private String schema;

    public EmployeeLoader(String schema){
        this.schema=schema;
        employeeUtils=new EmployeeUtilsImpl();
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job,location);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new EmployeeInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
            this.recordReader=recordReader;

    }

    @Override
    public Tuple getNext() throws IOException {

        try {
            if(recordReader.nextKeyValue()){
                return employeeUtils.getTuple(recordReader.getCurrentValue());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
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

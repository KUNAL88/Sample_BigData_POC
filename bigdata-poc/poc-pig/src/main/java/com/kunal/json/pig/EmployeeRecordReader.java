package com.kunal.json.pig;

import com.kunal.json.parser.OuterZIPParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.zip.ZipInputStream;

public class EmployeeRecordReader extends RecordReader<Text,Object> {

    private static final Logger LOG= LoggerFactory.getLogger(EmployeeRecordReader.class);
    private ZipInputStream zipInputStream=null;
    private OuterZIPParser outerZIPParser=null;

    private long startOfSplit;
    private long posOfSplit;
    private long endOfSplit;

    private String outerZipFileName;

    private Text key=new Text();
    private Object value;

    private FSDataInputStream outerZipFileInputStream;
    private boolean hasNextKeyValue=true;
    private FileSystem fs;


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSplit split=(FileSplit) inputSplit;
        Configuration conf=taskAttemptContext.getConfiguration();

        startOfSplit=split.getStart();
        endOfSplit=startOfSplit+split.getLength();
        final Path path=split.getPath();
        outerZipFileName=path.getName();
        fs=path.getFileSystem(conf);
        outerZipFileInputStream=fs.open(path);
        zipInputStream=new ZipInputStream(outerZipFileInputStream);
        outerZIPParser=new OuterZIPParser(zipInputStream,outerZipFileName);
    }

    @Override
    public boolean nextKeyValue() throws InterruptedException {

       if(hasNextKeyValue){
           hasNextKeyValue=outerZIPParser.hasNextRecord();

           if(hasNextKeyValue){
               value=outerZIPParser.getRecord();
           }

           try {
               posOfSplit=outerZipFileInputStream.getPos();
           } catch (IOException e) {
               LOG.error("Corrupted Zip file. Skipping",e);
           }
       }

        return hasNextKeyValue;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        key.set(outerZipFileName);
        return key;
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
       return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {

        if(startOfSplit==endOfSplit){
            return 0.0f;
        }

        return Math.min(1.0f,(posOfSplit-startOfSplit)/(float)(endOfSplit-startOfSplit));
    }

    @Override
    public void close() throws IOException {
        if(outerZipFileInputStream!=null){
            outerZipFileInputStream.close();
        }
    }
}

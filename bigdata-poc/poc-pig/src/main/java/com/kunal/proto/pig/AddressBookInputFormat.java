package com.kunal.proto.pig;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;

import java.io.IOException;
import java.util.List;

public class AddressBookInputFormat extends FileInputFormat<Text,Object> {
    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new AddressBookRecordReader();
    }

    @Override
    public boolean isSplitable(JobContext jobContext, Path filename){
        return false;
    }

    @Override
    protected List<FileStatus> listStatus(JobContext jobContext) throws IOException {
        return MapRedUtil.getAllFileRecursively(super.listStatus(jobContext),jobContext.getConfiguration());
    }


}

package com.kunal.json.pig;

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

public class EmployeeInputFormat extends FileInputFormat<Text,Object> {
    @Override
    public RecordReader<Text, Object> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new EmployeeRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        return MapRedUtil.getAllFileRecursively(super.listStatus(job),job.getConfiguration());
    }
}

package com.kunal.proto.pig;

import com.kunal.orc.AddressBook;
import com.kunal.orc.AddressBookRecord;
import com.kunal.proto.parser.AddressBookParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class AddressBookRecordReader extends org.apache.hadoop.mapreduce.RecordReader {

    private static final Logger LOG= LoggerFactory
            .getLogger(AddressBookRecordReader.class);

    private Text key=new Text();
    private Tuple tuple;

    //progress meter
    protected long start;
    protected long pos;
    protected long end;

    private FSDataInputStream fsin;

    private static List<AddressBookRecord> addressBookRecords;
    private static AddressBook addressBook;

    private long bytePOS=0;


    private static String srcFilename;
    private static AddressBookParser addressBookParserInstance=AddressBookParser.getInstance();


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration conf=taskAttemptContext.getConfiguration();

        FileSplit fileSplit=(FileSplit) inputSplit;

        pos=start=fileSplit.getStart();
        end=start+=fileSplit.getLength();

        final Path file=fileSplit.getPath();

        key.set(file.getName());
        FileSystem fs=file.getFileSystem(conf);
        fsin=fs.open(fileSplit.getPath());

        byte[] splitBytes=new byte[fsin.available()];
        fsin.readFully(0,splitBytes);

        srcFilename=file.getName();
        addressBook=new AddressBook();

        addressBookRecords=addressBookParserInstance.parseAddressBook(srcFilename,splitBytes);
        addressBook.setAddressBookRecordList(addressBookRecords);


    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        tuple=null;

        while (tuple==null){

            if(fsin.available()==0){
                LOG.debug("Protobuf data blob reaches to end. Nothing Left to read");
                return false;
            }

            tuple=addressBookParserInstance.populateTuple(addressBook);

            addressBookRecords=null;
            addressBook=null;

            if(tuple==null){
                return false;
            }
        }

        pos=bytePOS;
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Writable getCurrentValue() throws IOException, InterruptedException {
        return tuple;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {

        if(start==end){
            return 0.0f;
        }else {
            return Math.min(1.0f,(pos-start)/(float)(end-start));
        }
    }

    @Override
    public void close() throws IOException {

        if(fsin!=null){
            fsin.close();
        }

    }
}

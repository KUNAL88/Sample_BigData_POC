package com.kunal.pigrunner;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PigRunner {

    private static PigServer pigServer;
    private static final String TEMP_DIR=System.getProperty("test.build.data","target");
    private static final String PIG_TEMP_DIR=TEMP_DIR+"/pig-temp";

    public static PigServer makePigServer() throws ExecException {
        pigServer=new PigServer(ExecType.LOCAL);
        pigServer.getPigContext()
                .getProperties()
                .setProperty("io.compression.codecs","org.apache.hadoop.io.compress.GzipCodec");

        pigServer.getPigContext()
                .getProperties()
                .setProperty("pig.temp.dir",PIG_TEMP_DIR);

        return pigServer;
    }

    public static void main(String[] args) throws IOException {

        PigServer pigServer=makePigServer();

        Map<String,String> params=new HashMap<>();
        params.put("inputDir","/home/kunal/sample_bigdata_poc/Sample_BigData_POC/bigdata-poc/common-data-objects/src/resources/rawfile/zip_gson/");
        params.put("outputDir","/home/kunal/sample_bigdata_poc/Sample_BigData_POC/bigdata-poc/poc-pig/src/resources/output/gson/");

        pigServer.registerScript("/home/kunal/sample_bigdata_poc/Sample_BigData_POC/bigdata-poc/poc-pig/src/pig/employee-record-converter.pig",params);

        System.out.println("Start of Pig Query .... ");

        Iterator<Tuple> itr=pigServer.openIterator("employee_final_ping");
        System.out.println(" All tuples as shown below .. ");

        int count=0;
        while (itr.hasNext()){
            count++;
            System.out.println(itr.next());
        }

        System.out.println("Total Records Processed  "+count);
        System.out.println("End of Pig Query ..... ");
    }
}

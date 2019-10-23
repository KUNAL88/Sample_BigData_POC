package com.kunal.gson;

import com.google.gson.Gson;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.utils.IOUtils;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class DataDeserializer {

    public static void main(String[] args) throws IOException {

        String path="/home/kunal/sample_bigdata_poc/Sample_BigData_POC/bigdata-poc/" +
                "common-data-objects/src/resources/rawfile/zip_gson/employee_gson_20191019.zip";

        ZipInputStream zis=new ZipInputStream(new FileInputStream(new File(path)));

        ZipEntry zipEntry=null;//zis.getNextEntry();
        Gson gson=new Gson();
        while ((zipEntry=zis.getNextEntry())!=null){

            System.out.println(zipEntry.getName());
            ByteArrayOutputStream out=new ByteArrayOutputStream();
            IOUtils.copy(zis,out);
            out.flush();
            out.close();

            byte[] bytes=out.toByteArray();

            if(bytes!=null){
                System.out.println("---> "+new String(bytes));
                Employee employee=gson.fromJson(new String(bytes),Employee.class);

            }
        }

        // while (zis.)
    }
}

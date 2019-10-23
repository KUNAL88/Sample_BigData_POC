package com.kunal.gson;

import com.google.gson.Gson;
import org.joda.time.LocalDate;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class DataSerializer {

    private static ArrayList<byte[]> arrayList=new ArrayList<>();

    public static void main(String[] args) throws IOException {

        Employee employee=null;
        Gson gson=new Gson();
        for(int i=0;i<10;i++){
            employee=new Employee();

            employee.setId(i);
            employee.setFirstName("Name-"+i);
            employee.setLastName("Lastname-"+i);
           // employee.setDob(LocalDate.now());
            employee.setEmail("abc_"+i+"@gmail.com");

           // arrayList.add(employee);

            String gsonString=gson.toJson(employee,Employee.class);
            arrayList.add(gsonString.getBytes());
           // System.out.println("gsonString "+gsonString);
        }

        ZipOutputStream zipOutputStream=new ZipOutputStream(
                new FileOutputStream(
                        new File("/home/kunal/sample_bigdata_poc/Sample_BigData_POC/bigdata-poc/" +
                                "common-data-objects/src/resources/rawfile/zip_gson/employee_gson_20191019.zip")
                )
        );

        Iterator<byte[]> itr=arrayList.iterator();
        int count=1;
        while (itr.hasNext()){
            zipOutputStream.putNextEntry(new ZipEntry("smaple-"+count+".json"));
            zipOutputStream.write(itr.next());
            count++;
        }
/*
        byte[] buffer = new byte[1024];

        zipOutputStream.write(buffer);*/
        zipOutputStream.closeEntry();
        zipOutputStream.close();
        System.out.println("Zip file has been created !");


    }
}

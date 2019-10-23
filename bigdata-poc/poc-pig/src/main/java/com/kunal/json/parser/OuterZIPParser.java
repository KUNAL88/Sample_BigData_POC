package com.kunal.json.parser;

import com.google.gson.Gson;
import com.kunal.gson.Employee;
import com.kunal.orc.EmployeeRecord;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class OuterZIPParser {

    private static final Logger LOG= LoggerFactory.getLogger(OuterZIPParser.class);

    private String filename;
    private ZipInputStream zipInputStream;
    private ZipEntry zipEntry;

    private boolean hasNextRecord=true;
    private Object nextRecord;
    private int failedZipEntryCount;

    private EmployeeJsonParser employeeJsonParser;
    private EmployeeRecord employeeRecord;
    private ArrayList<Employee> employees;

    public OuterZIPParser(ZipInputStream zipInputStream,String srcFilename){

        if(zipInputStream==null){
            throw new NullPointerException("ZIP Input Stream is null ...");
        }

        if(srcFilename==null){
            throw new InvalidParameterException("Src File Name is null ...");
        }

        this.zipInputStream=zipInputStream;
        filename=srcFilename;
    }

    public Object getRecord(){
        if(hasNextRecord==false){
            return null;
        }

        return nextRecord;
    }

    public boolean hasNextRecord(){

        employeeRecord=new EmployeeRecord();
        Employee employee=null;

        while (hasNextRecord){

            try {
                zipEntry=zipInputStream.getNextEntry();
                if(zipEntry==null){
                    hasNextRecord=false;
                    return false;
                }
            } catch (IOException e) {
                LOG.error("Unable to get next entry from corrupted "+filename,e);
                return false;
            }

            String entryName=zipEntry.getName();


            ByteArrayOutputStream out=new ByteArrayOutputStream();

            try {
                IOUtils.copy(zipInputStream,out);
                out.flush();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }


            byte[] bytes=out.toByteArray();


            if(bytes!=null){
               // employees.add(employeeJsonParser.parseEmployeRecord(bytes,entryName));
               // employeeJsonParser=new EmployeeJsonParser(filename);
               // employeeJsonParser.parseEmployeRecord(bytes,entryName);
                Gson gson=new Gson();
                System.out.println(new String(bytes));
               employee=gson.fromJson(new String(bytes),Employee.class);
              //  employees.add(employee);
               // return employee;
            }

           /* if(employeeJsonParser.){

            }*/
        }
       // employeeRecord.setEmployees(employees);
        nextRecord=(Object) employee;
        return hasNextRecord;
    }
}

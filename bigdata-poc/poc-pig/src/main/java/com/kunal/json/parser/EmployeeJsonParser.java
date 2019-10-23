package com.kunal.json.parser;

import com.google.gson.Gson;
import com.kunal.gson.Employee;

public class EmployeeJsonParser {

    private byte[] data;
    private String gsonFilename;
    private String outerZipFilename;
    private Gson gson;

    public EmployeeJsonParser(String outerZipFilename){
        //this.data=data;
        //this.gsonFilename=gsonFilename;
        gson=new Gson();
        this.outerZipFilename=outerZipFilename;
    }

    public Employee parseEmployeRecord(byte[] data,String gsonFilename){
        Employee employee=gson.fromJson(new String(data.toString()),Employee.class);
        return employee;
    }


}

package com.kunal.json.pigutils;

import com.kunal.gson.Employee;
import com.kunal.orc.EmployeeRecord;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class EmployeeUtilsImpl implements EmployeeUtils {

    private static final Logger LOG= LoggerFactory.getLogger(EmployeeUtils.class);
    private static final TupleFactory tupleFactory=TupleFactory.getInstance();

    private static final int noOfOutputFields=5;

    @Override
    public Tuple getTuple(Object record) {

        EmployeeRecord employee=(EmployeeRecord)record;

        Tuple tuple=null;

        if(employee!=null){
            tuple=tupleFactory.newTuple(1);

            try {
                tuple.set(0,getEmployeeRecord(employee.getEmployees()));
            } catch (ExecException e) {
                LOG.error("Error creating bag of employee "+e);
            }
        }

        return tuple;
    }

    private static DataBag getEmployeeRecord(ArrayList<Employee> employees){

        int numOfFields=5;
        DataBag dataBag= BagFactory.getInstance().newDefaultBag();

        if(employees!=null && !employees.isEmpty()){

            for(Employee employee:employees){

                try {
                    Tuple employeeTuple=tupleFactory.newTuple(numOfFields);
                    employeeTuple.set(0,employee.getId());
                    employeeTuple.set(1,employee.getFirstName());
                    employeeTuple.set(2,employee.getLastName());
                    employeeTuple.set(3,employee.getEmail());

                    dataBag.add(employeeTuple);
                } catch (ExecException e) {
                    e.printStackTrace();
                }
            }
        }

        return dataBag;
    }
}

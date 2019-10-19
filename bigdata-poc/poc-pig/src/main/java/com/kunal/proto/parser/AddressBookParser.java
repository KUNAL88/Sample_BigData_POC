package com.kunal.proto.parser;

import com.google.protobuf.CodedInputStream;
import com.kunal.orc.AddressBook;
import com.kunal.orc.AddressBookRecord;
import com.kunal.proto.AddressBookProtos;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class AddressBookParser {

    private static final Logger LOG= LoggerFactory.getLogger(AddressBookParser.class);
    private static AddressBookParser addressBookParserInstace;
    private static List<AddressBookRecord> addressBookRecordList;
    private static CodedInputStream codedInputStream;
    private static final TupleFactory tupleFactory=TupleFactory.getInstance();

    public static AddressBookParser getInstance(){

        if(addressBookParserInstace==null){
            addressBookParserInstace=new AddressBookParser();
        }
        return addressBookParserInstace;
    }

    public static List<AddressBookRecord> parseAddressBook(String srcFilenme,byte[] data){

        if(data!=null){
            addressBookRecordList=new ArrayList<>();
            ByteBuffer byteBuffer=ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);

            while (byteBuffer.hasRemaining()){
                int length=byteBuffer.getInt();

                codedInputStream=CodedInputStream.newInstance(data,byteBuffer.position(),length);

                try {
                    AddressBookRecord addressBookRecord=new AddressBookRecord(srcFilenme, AddressBookProtos.Person.parseFrom(codedInputStream));
                    addressBookRecordList.add(addressBookRecord);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


        return addressBookRecordList;
    }

    public static Tuple populateTuple(AddressBook addressBook){

        Tuple outputTuple=null;

        if(addressBook!=null){
            outputTuple=tupleFactory.newTuple(1);

            try {
                outputTuple.set(0,getAddressBookBag(addressBook.getAddressBookRecordList()));
            } catch (ExecException e) {
                e.printStackTrace();
            }
        }

        return outputTuple;
    }

    private static DataBag getAddressBookBag(List<AddressBookRecord> addressBookRecordList){

        int numOfFields=5;

        DataBag addressBookBag= BagFactory.getInstance().newDefaultBag();

        if(addressBookRecordList!=null && !addressBookRecordList.isEmpty()){

            for (AddressBookRecord addressBookRecord : addressBookRecordList){

                Tuple tuple=tupleFactory.newTuple(numOfFields);

                try {
                    tuple.set(0,addressBookRecord.getSrcFilename());
                    tuple.set(1,addressBookRecord.getPersonID());
                    tuple.set(2,addressBookRecord.getPersonName());
                    tuple.set(3,addressBookRecord.getEmailID());
                    tuple.set(4,addressBookRecord.getMobileNumber());

                    addressBookBag.add(tuple);
                } catch (ExecException e) {
                    e.printStackTrace();
                }
            }
        }

        return addressBookBag;
    }
}

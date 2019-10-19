package com.kunal.orc;

import com.kunal.proto.AddressBookProtos;

public class AddressBookRecord {

    private Long serverTS;
    private Integer personID;
    private String personName;
    private String emailID;
    private String mobileNumber;
    private String srcFilename;

    public AddressBookRecord(){}

    public AddressBookRecord(String srcFilename, AddressBookProtos.Person addressBookProtos){

        this.srcFilename=srcFilename;

        personID=addressBookProtos.hasId() ? addressBookProtos.getId() : null;
        personName=addressBookProtos.hasName() ? addressBookProtos.getName() :null;
        emailID=addressBookProtos.hasEmail() ? addressBookProtos.getEmail() : null;
        mobileNumber=addressBookProtos.getPhones(0).getNumber();

    }

    public Long getServerTS() {
        return serverTS;
    }

   /* public void setServerTS(Long serverTS) {
        this.serverTS = serverTS;
    }*/

    public Integer getPersonID() {
        return personID;
    }

   /* public void setPersonID(Integer personID) {
        this.personID = personID;
    }*/

    public String getPersonName() {
        return personName;
    }

   /* public void setPersonName(String personName) {
        this.personName = personName;
    }*/

    public String getEmailID() {
        return emailID;
    }

  /*  public void setEmailID(String emailID) {
        this.emailID = emailID;
    }*/

    public String getMobileNumber() {
        return mobileNumber;
    }

  /*  public void setMobileNumber(String mobileNumber) {
        this.mobileNumber = mobileNumber;
    }*/

    public String getSrcFilename() {
        return srcFilename;
    }
}

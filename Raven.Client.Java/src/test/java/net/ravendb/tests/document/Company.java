package net.ravendb.tests.document;

import java.util.List;

import com.mysema.query.annotations.QueryEntity;

@QueryEntity
public class Company {
  private double accountsReceivable;
  private String id;
  private String name;
  private String address1;
  private String address2;
  private String address3;
  private List<Contact>  contacts;
  private int phone;
  private CompanyType type;


  public Company() {
    super();
  }


  public Company(String id, String name) {
    super();
    this.id = id;
    this.name = name;
  }


  public double getAccountsReceivable() {
    return accountsReceivable;
  }


  public void setAccountsReceivable(double accountsReceivable) {
    this.accountsReceivable = accountsReceivable;
  }


  public String getId() {
    return id;
  }


  public void setId(String id) {
    this.id = id;
  }


  public String getName() {
    return name;
  }


  public void setName(String name) {
    this.name = name;
  }


  public String getAddress1() {
    return address1;
  }


  public void setAddress1(String address1) {
    this.address1 = address1;
  }


  public String getAddress2() {
    return address2;
  }


  public void setAddress2(String address2) {
    this.address2 = address2;
  }


  public String getAddress3() {
    return address3;
  }


  public void setAddress3(String address3) {
    this.address3 = address3;
  }


  public List<Contact> getContacts() {
    return contacts;
  }


  public void setContacts(List<Contact> contacts) {
    this.contacts = contacts;
  }


  public int getPhone() {
    return phone;
  }


  public void setPhone(int phone) {
    this.phone = phone;
  }


  public CompanyType getType() {
    return type;
  }


  public void setType(CompanyType type) {
    this.type = type;
  }


  public static enum CompanyType {
    PUBLIC,

    PRIVATE;
  }
}

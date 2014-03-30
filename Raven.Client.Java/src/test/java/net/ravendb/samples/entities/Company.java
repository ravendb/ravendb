package net.ravendb.samples.entities;

import java.util.List;

import com.mysema.query.annotations.QueryEntity;

@QueryEntity
public class Company {

  public Company() {
    super();
  }

  public Company(String id, String name, List<Employee> employees, String country, int numberOfHappyCustomers) {
    super();
    this.id = id;
    this.name = name;
    this.employees = employees;
    this.country = country;
    this.numberOfHappyCustomers = numberOfHappyCustomers;
  }
  private String id;
  private String name;
  private List<Employee> employees;
  private String country;
  private int numberOfHappyCustomers;
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
  public List<Employee> getEmployees() {
    return employees;
  }
  public void setEmployees(List<Employee> employees) {
    this.employees = employees;
  }
  public String getCountry() {
    return country;
  }
  public void setCountry(String country) {
    this.country = country;
  }
  public int getNumberOfHappyCustomers() {
    return numberOfHappyCustomers;
  }
  public void setNumberOfHappyCustomers(int numberOfHappyCustomers) {
    this.numberOfHappyCustomers = numberOfHappyCustomers;
  }


}

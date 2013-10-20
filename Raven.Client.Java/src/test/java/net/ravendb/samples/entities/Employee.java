package net.ravendb.samples.entities;

import java.util.Date;
import java.util.List;

import com.mysema.query.annotations.QueryEntity;

@QueryEntity
public class Employee {

  public Employee() {
    super();
  }
  public Employee(String name, List<String> specialties, Date hiredAt, double hourlyRate) {
    super();
    this.name = name;
    this.specialties = specialties;
    this.hiredAt = hiredAt;
    this.hourlyRate = hourlyRate;
  }
  private String name;
  private List<String> specialties;
  private Date hiredAt;
  private double hourlyRate;
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
  public List<String> getSpecialties() {
    return specialties;
  }
  public void setSpecialties(List<String> specialties) {
    this.specialties = specialties;
  }
  public Date getHiredAt() {
    return hiredAt;
  }
  public void setHiredAt(Date hiredAt) {
    this.hiredAt = hiredAt;
  }
  public double getHourlyRate() {
    return hourlyRate;
  }
  public void setHourlyRate(double hourlyRate) {
    this.hourlyRate = hourlyRate;
  }


}

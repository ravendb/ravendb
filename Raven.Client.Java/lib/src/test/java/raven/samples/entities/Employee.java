package raven.samples.entities;

import java.util.Date;

import com.mysema.query.annotations.QueryEntity;

@QueryEntity
public class Employee {

  public Employee() {
    super();
  }
  public Employee(String name, String[] specialties, Date hiredAt, double hourlyRate) {
    super();
    this.name = name;
    this.specialties = specialties;
    this.hiredAt = hiredAt;
    this.hourlyRate = hourlyRate;
  }
  private String name;
  private String[] specialties;
  private Date hiredAt;
  private double hourlyRate;
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
  public String[] getSpecialties() {
    return specialties;
  }
  public void setSpecialties(String[] specialties) {
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

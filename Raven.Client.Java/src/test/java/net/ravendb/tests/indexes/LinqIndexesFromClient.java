package net.ravendb.tests.indexes;

import com.mysema.query.annotations.QueryEntity;

public class LinqIndexesFromClient {

  public static enum Gender {
    /**
     * Male
     */
    MALE,

    /**
     * Female
     */
    FEMALE;
  }

  @QueryEntity
  public static class User {
    private String id;
    private String name;
    private String location;

    private int age;
    private Gender gender;
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
    public String getLocation() {
      return location;
    }
    public void setLocation(String location) {
      this.location = location;
    }
    public int getAge() {
      return age;
    }
    public void setAge(int age) {
      this.age = age;
    }
    public Gender getGender() {
      return gender;
    }
    public void setGender(Gender gender) {
      this.gender = gender;
    }

  }

  public static class LocationCount {
    private String location;
    private int count;
    public String getLocation() {
      return location;
    }
    public void setLocation(String location) {
      this.location = location;
    }
    public int getCount() {
      return count;
    }
    public void setCount(int count) {
      this.count = count;
    }

  }

  @QueryEntity
  public static class LocationAge {
    private String location;
    private double averageAge;
    private int count;
    private double ageSum;

    public String getLocation() {
      return location;
    }
    public void setLocation(String location) {
      this.location = location;
    }
    public double getAverageAge() {
      return averageAge;
    }
    public void setAverageAge(double averageAge) {
      this.averageAge = averageAge;
    }
    public int getCount() {
      return count;
    }
    public void setCount(int count) {
      this.count = count;
    }
    public double getAgeSum() {
      return ageSum;
    }
    public void setAgeSum(double ageSum) {
      this.ageSum = ageSum;
    }

  }


}

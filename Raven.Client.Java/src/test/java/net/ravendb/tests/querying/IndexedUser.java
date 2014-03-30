package net.ravendb.tests.querying;

import java.util.Date;

import com.mysema.query.annotations.QueryEntity;

@QueryEntity
public class IndexedUser {
  private int age;
  private Date birthday;
  private String name;
  private String email;

  public int getAge() {
    return age;
  }
  public void setAge(int age) {
    this.age = age;
  }
  public Date getBirthday() {
    return birthday;
  }
  public void setBirthday(Date birthday) {
    this.birthday = birthday;
  }
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
  public String getEmail() {
    return email;
  }
  public void setEmail(String email) {
    this.email = email;
  }

}

package raven.samples.entities;

import java.io.Serializable;

import com.mysema.query.annotations.QueryEntity;

@QueryEntity
public class GroupResult implements Serializable {
  private String name;
  private int count;
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
  public int getCount() {
    return count;
  }
  public void setCount(int count) {
    this.count = count;
  }


}

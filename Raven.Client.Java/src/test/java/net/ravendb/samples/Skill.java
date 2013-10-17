package net.ravendb.samples;

import com.mysema.query.annotations.QueryEntity;

@QueryEntity
public class Skill {

  private String name;
  private Integer expPoints;

  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
  public Integer getExpPoints() {
    return expPoints;
  }
  public void setExpPoints(Integer expPoints) {
    this.expPoints = expPoints;
  }



}

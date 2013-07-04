package raven.samples;

import com.mysema.query.annotations.QueryEntity;

@QueryEntity
public class PersonProjection {
  private String name;

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName(String name) {
    this.name = name;
  }


}

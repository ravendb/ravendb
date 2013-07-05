package raven.samples;

import com.mysema.query.annotations.QueryEntity;
import com.mysema.query.annotations.QueryProjection;

@QueryEntity
public class PersonResult {
  private String name;
  private int count;

  @QueryProjection
  public PersonResult(String name, int count) {
    super();
    this.name = name;
    this.count = count;
  }
  /**
   * @return the count
   */
  public int getCount() {
    return count;
  }
  /**
   * @param count the count to set
   */
  public void setCount(int count) {
    this.count = count;
  }
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

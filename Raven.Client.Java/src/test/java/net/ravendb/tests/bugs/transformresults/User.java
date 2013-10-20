package net.ravendb.tests.bugs.transformresults;

import com.mysema.query.annotations.QueryEntity;

@QueryEntity
public class User {
  private String id;
  private String displayName;

  public User() {
    super();
  }
  public User(String id, String displayName) {
    super();
    this.id = id;
    this.displayName = displayName;
  }
  public String getDisplayName() {
    return displayName;
  }
  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }
  public String getId() {
    return id;
  }
  public void setId(String id) {
    this.id = id;
  }

}

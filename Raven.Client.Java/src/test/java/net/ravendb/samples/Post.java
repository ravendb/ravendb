package net.ravendb.samples;

import java.util.ArrayList;
import java.util.List;

import com.mysema.query.annotations.QueryEntity;

@QueryEntity
public class Post {
  private String id;
  private String name;
  private List<Comment> comments = new ArrayList<>();

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
  public List<Comment> getComments() {
    return comments;
  }
  public void setComments(List<Comment> comments) {
    this.comments = comments;
  }

}

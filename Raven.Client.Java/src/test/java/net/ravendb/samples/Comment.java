package net.ravendb.samples;

import java.util.ArrayList;
import java.util.List;

import com.mysema.query.annotations.QueryEntity;

@QueryEntity
public class Comment {

  private String id;
  private String author;
  private String test;
  private List<Comment> comments = new ArrayList<>();

  public String getId() {
    return id;
  }
  public void setId(String id) {
    this.id = id;
  }
  public String getAuthor() {
    return author;
  }
  public void setAuthor(String author) {
    this.author = author;
  }
  public String getTest() {
    return test;
  }
  public void setTest(String test) {
    this.test = test;
  }
  public List<Comment> getComments() {
    return comments;
  }
  public void setComments(List<Comment> comments) {
    this.comments = comments;
  }


}

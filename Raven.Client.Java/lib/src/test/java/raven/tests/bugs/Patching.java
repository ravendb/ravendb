package raven.tests.bugs;

import java.util.List;

import com.mysema.query.annotations.QueryEntity;

import raven.client.RemoteClientTest;

public class Patching extends RemoteClientTest {

  //TODO: finish me

  @QueryEntity
  public static class Post {
    private String id;
    private List<Comment> comments;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public List<Comment> getComments() {
      return comments;
    }
    public void setComments(List<Comment> comments) {
      this.comments = comments;
    }

  }

  @QueryEntity
  public static class Comment {
    private String authorId;

    public String getAuthorId() {
      return authorId;
    }

    public void setAuthorId(String authorId) {
      this.authorId = authorId;
    }


  }

}

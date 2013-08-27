package raven.tests.document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;

import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;

public class CasingIssueTest extends RemoteClientTest {

  @Test
  public void canQueryByEntityType() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {

        Post post = new Post();
        post.setTitle("test");
        post.setBody("casing");
        session.store(post);
        session.saveChanges();

        Post single = session.advanced().luceneQuery(Post.class).waitForNonStaleResults().single();

        assertEquals("test", single.getTitle());
      }
    }

  }

  @Test
  public void unitOfWorkEvenWhenQuerying() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {

        Post post = new Post();
        post.setTitle("test");
        post.setBody("casing");
        session.store(post);
        session.saveChanges();

        Post single = session.advanced().luceneQuery(Post.class).waitForNonStaleResults().single();

        assertSame(post, single);
      }
    }
  }


  @QueryEntity
  public static class Post {
    private String id;
    private String title;
    private String body;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public String getTitle() {
      return title;
    }
    public void setTitle(String title) {
      this.title = title;
    }
    public String getBody() {
      return body;
    }
    public void setBody(String body) {
      this.body = body;
    }

  }

}

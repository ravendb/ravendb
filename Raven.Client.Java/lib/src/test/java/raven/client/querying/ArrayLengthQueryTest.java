package raven.client.querying;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentQueryCustomizationFactory;
import raven.client.document.DocumentStore;
import raven.client.linq.IRavenQueryable;
import raven.tests.bugs.PatchingTest.Comment;
import raven.tests.bugs.PatchingTest.Post;
import raven.tests.bugs.QPatchingTest_Post;
import raven.tests.bugs.QUser;
import raven.tests.bugs.User;


public class ArrayLengthQueryTest extends RemoteClientTest {

  private User generateUser(String name, String... tags) {
    User user = new User();
    user.setName(name);
    user.setTags(tags);
    return user;
  }

  private Post generatePost(String commentPrefix, int comments) {
    Post post = new Post();
    List<Comment> commentsList = new ArrayList<>();
    for (int i = 0; i < comments; i++) {
      Comment comment = new Comment();
      comment.setAuthorId(commentPrefix);
      commentsList.add(comment);
    }
    post.setComments(commentsList);
    return post;
  }

  @Test
  public void canUseSizeOnSimpleArray() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(generateUser("Ayende", "c#", "developer"));
        session.store(generateUser("Marcin", "java","php", "c#"));
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        QUser x = QUser.user;

        IRavenQueryable<User> query = session.query(User.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .where(x.tags.size().eq(3));
        assertEquals("Tags.Length:3", query.toString());
        assertEquals("Marcin", query.single().getName());
      }
    }
  }

  @Test
  public void canUseSizeOnList() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(generatePost("post", 5));
        session.store(generatePost("xxx", 2));
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        QPatchingTest_Post x = QPatchingTest_Post.post;
        IRavenQueryable<Post> query = session.query(Post.class)
          .where(x.comments.size().eq(5))
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults());
        List<Post> comments = query.toList();
        assertEquals("Comments.Count:5", query.toString());
        assertEquals(1, comments.size());
      }
    }
  }

  @Test
  public void canOrderOnSizeOfArray() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(generateUser("Ayende", "c#", "developer"));
        session.store(generateUser("Marcin", "java","php", "c#"));
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        QUser x = QUser.user;

        IRavenQueryable<User> query = session.query(User.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .orderBy(x.tags.size().desc());
        List<User> result = query.toList();
        assertEquals(2, result.size());
        assertEquals("Marcin", result.get(0).getName());
        assertEquals("Ayende", result.get(1).getName());

        query = session.query(User.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .orderBy(x.tags.size().asc());
        result = query.toList();
        assertEquals(2, result.size());
        assertEquals("Ayende", result.get(0).getName());
        assertEquals("Marcin", result.get(1).getName());
      }
    }
  }
  @Test
  public void canOrderOnListSize() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(generatePost("post", 5));
        session.store(generatePost("xxx", 2));
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        QPatchingTest_Post x = QPatchingTest_Post.post;
        IRavenQueryable<Post> query = session.query(Post.class)
          .orderBy(x.comments.size().asc())
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults());
        List<Post> posts = query.toList();
        assertEquals(2, posts.size());
        assertEquals("xxx", posts.get(0).getComments().get(0).getAuthorId());
        assertEquals("post", posts.get(1).getComments().get(0).getAuthorId());

        query = session.query(Post.class)
          .orderBy(x.comments.size().desc())
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults());
        posts = query.toList();
        assertEquals(2, posts.size());
        assertEquals("post", posts.get(0).getComments().get(0).getAuthorId());
        assertEquals("xxx", posts.get(1).getComments().get(0).getAuthorId());
      }
    }
  }


}

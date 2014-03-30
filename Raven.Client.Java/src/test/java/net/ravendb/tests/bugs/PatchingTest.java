package net.ravendb.tests.bugs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import net.ravendb.abstractions.commands.ICommandData;
import net.ravendb.abstractions.commands.PatchCommandData;
import net.ravendb.abstractions.data.PatchCommandType;
import net.ravendb.abstractions.data.PatchRequest;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJValue;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;


public class PatchingTest extends RemoteClientTest {
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

  @Test
  public void canConvertToAndFromJsonWithNestedPatchRequests() throws Exception {
    PatchRequest patch = new PatchRequest();
    patch.setName("Comments");
    patch.setType(PatchCommandType.MODIFY);
    patch.setPosition(0);

    PatchRequest subPatch1 = new PatchRequest();
    subPatch1.setName("AuthorId");
    subPatch1.setType(PatchCommandType.SET);
    subPatch1.setValue(new RavenJValue("authors/456"));

    PatchRequest subPatch2 = new PatchRequest();
    subPatch2.setName("AuthorName");
    subPatch2.setType(PatchCommandType.SET);
    subPatch2.setValue(new RavenJValue("Tolkien"));

    patch.setNested(new PatchRequest[] { subPatch1, subPatch2 });

    RavenJObject jsonPatch = patch.toJson();
    PatchRequest backToPatch = PatchRequest.fromJson(jsonPatch);
    assertEquals(patch.getName(), backToPatch.getName());
    assertEquals(patch.getNested().length, backToPatch.getNested().length);
  }

  @Test
  public void canConvertToAndFromJsonWithoutNestedPatchRequests() {
    PatchRequest patch = new PatchRequest();
    patch.setName("Comments");
    patch.setType(PatchCommandType.MODIFY);
    patch.setPosition(0);
    patch.setNested(null);

    RavenJObject jsonPatch = patch.toJson();
    PatchRequest backToPatch = PatchRequest.fromJson(jsonPatch);
    assertEquals(patch.getName(), backToPatch.getName());
    assertArrayEquals(patch.getNested(), backToPatch.getNested());
  }

  @Test
  public void canConvertToAndFromJsonWithEmptyNestedPatchRequests() {
    PatchRequest patch = new PatchRequest();
    patch.setName("Comments");
    patch.setType(PatchCommandType.MODIFY);
    patch.setPosition(0);
    patch.setNested(new PatchRequest[] { });

    RavenJObject jsonPatch = patch.toJson();
    PatchRequest backToPatch = PatchRequest.fromJson(jsonPatch);
    assertEquals(patch.getName(), backToPatch.getName());
    assertArrayEquals(patch.getNested(), backToPatch.getNested());
  }

  @Test
  public void canModifyValue() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Post post = new Post();
        Comment comment = new Comment();
        comment.setAuthorId("authors/123");
        post.setComments(Arrays.asList(comment));
        session.store(post);
        session.saveChanges();
      }

      PatchCommandData patchCommandData = new PatchCommandData();
      patchCommandData.setKey("posts/1");

      PatchRequest patchRequest1 = new PatchRequest();
      patchRequest1.setName("Comments");
      patchRequest1.setType(PatchCommandType.MODIFY);
      patchRequest1.setPosition(0);

      PatchRequest nestedPatchRequest = new PatchRequest();
      nestedPatchRequest.setName("AuthorId");
      nestedPatchRequest.setType(PatchCommandType.SET);
      nestedPatchRequest.setValue(new RavenJValue("authors/456"));

      patchRequest1.setNested(new PatchRequest[] { nestedPatchRequest });

      patchCommandData.setPatches(new PatchRequest[] { patchRequest1 } );

      store.getDatabaseCommands().batch(Arrays.<ICommandData> asList(patchCommandData));

      try (IDocumentSession session = store.openSession()) {
        assertEquals("authors/456", session.load(Post.class, "posts/1").getComments().get(0).getAuthorId());
      }
    }
  }

  @Test
  public void canAddValuesToList() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Post post = new Post();
        Comment comment = new Comment();
        comment.setAuthorId("authors/123");
        post.setComments(Arrays.asList(comment));
        session.store(post);
        session.saveChanges();
      }

      Comment comment = new Comment();
      comment.setAuthorId("authors/456");

      PatchRequest patch = new PatchRequest();
      patch.setType(PatchCommandType.ADD);
      patch.setName("Comments");
      patch.setValue(RavenJObject.fromObject(comment));

      PatchCommandData patchCommandData = new PatchCommandData();
      patchCommandData.setKey("posts/1");
      patchCommandData.setPatches(new PatchRequest[] { patch });

      store.getDatabaseCommands().batch(Arrays. <ICommandData> asList(patchCommandData));

      try (IDocumentSession session = store.openSession()) {
        List<Comment> comments = session.load(Post.class, "posts/1").getComments();
        assertEquals(2, comments.size());
        assertEquals("authors/456", comments.get(1).getAuthorId());
      }
    }
  }

  @Test
  public void canRemoveValuesFromList() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Post post = new Post();
        Comment comment = new Comment();
        comment.setAuthorId("authors/123");
        post.setComments(Arrays.asList(comment));
        session.store(post);
        session.saveChanges();
      }

      Comment comment = new Comment();
      comment.setAuthorId("authors/456");

      PatchRequest patch = new PatchRequest();
      patch.setType(PatchCommandType.REMOVE);
      patch.setName("Comments");
      patch.setPosition(0);

      PatchCommandData patchCommandData = new PatchCommandData();
      patchCommandData.setKey("posts/1");
      patchCommandData.setPatches(new PatchRequest[] { patch });

      store.getDatabaseCommands().batch(Arrays. <ICommandData> asList(patchCommandData));

      try (IDocumentSession session = store.openSession()) {
        List<Comment> comments = session.load(Post.class, "posts/1").getComments();
        assertEquals(0, comments.size());
      }
    }
  }

}

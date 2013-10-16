package net.ravendb.tests.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import net.ravendb.abstractions.json.linq.JTokenType;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;

import org.junit.Test;


public class RavenJObjectsTest {

  @Test
  public void canIgnoreUnassignedProperties()  {
    Blog blogOne = new Blog();
    blogOne.setTitle("one");
    blogOne.setCategory("Ravens");


    RavenJObject o = RavenJObject.fromObject(blogOne);

    assertTrue(o.containsKey("Title"));
    assertEquals("one", o.get("Title").value(String.class));

    assertTrue(o.containsKey("User"));
    assertTrue(o.get("User").getType() == JTokenType.NULL);

    assertFalse(o.containsKey("foo"));
    assertNull(o.get("foo"));
  }

  @Test
  public void canSerializeNestedObjectsCorrectly() {
    Blog blogOne = new Blog();
    blogOne.setTitle("one");
    blogOne.setCategory("Ravens");
    Tag tag1= new Tag();
    tag1.setName("birds");
    blogOne.setTags(new Tag[] { tag1});

    RavenJObject.fromObject(blogOne);
  }

  @Test
  public void can_read_array_into_ravenjobject() {
    String json = "[{\"Username\":\"user3\",\"Email\":\"user3@hotmail.com\",\"IsActive\":true,\"@metadata\":{\"Raven-Entity-Name\":\"UserDbs\"," +
        "\"Raven-Clr-Type\":\"Persistence.Models.UserDb, Infrastructure\"}}," +
        "{\"Username\":\"user4\",\"Email\":\"user3@hotmail.com\",\"IsActive\":true,\"@metadata\":{\"Raven-Entity-Name\":\"UserDbs\"," +
        "\"Raven-Clr-Type\":\"Persistence.Models.UserDb, Infrastructure\"}}]";

    RavenJToken obj = RavenJToken.parse(json);
    assertNotNull(obj);
  }


  public static class Blog {
    private User user;
    private String title;
    private Tag[] tags;
    private String category;
    public User getUser() {
      return user;
    }
    public void setUser(User user) {
      this.user = user;
    }
    public String getTitle() {
      return title;
    }
    public void setTitle(String title) {
      this.title = title;
    }
    public Tag[] getTags() {
      return tags;
    }
    public void setTags(Tag[] tags) {
      this.tags = tags;
    }
    public String getCategory() {
      return category;
    }
    public void setCategory(String category) {
      this.category = category;
    }

  }
  public static class Tag {
    private String name;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  public static class User {
    private String name;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

  }
}

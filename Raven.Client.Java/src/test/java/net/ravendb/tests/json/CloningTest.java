package net.ravendb.tests.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.abstractions.json.linq.RavenJValue;
import net.ravendb.tests.java.Parallel;
import net.ravendb.tests.java.Parallel.LoopBody;

import org.junit.Test;


public class CloningTest {

  public static class Blog {
    private String title;
    private String category;
    private BlogTag[] tags;
    public String getTitle() {
      return title;
    }
    public void setTitle(String title) {
      this.title = title;
    }
    public String getCategory() {
      return category;
    }
    public void setCategory(String category) {
      this.category = category;
    }
    public BlogTag[] getTags() {
      return tags;
    }
    public void setTags(BlogTag[] tags) {
      this.tags = tags;
    }


  }

  public static class BlogTag {
    private String name;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

  }

  @Test
  public void whenCloningWillRetainAllValues() {
    Blog newBlog = new Blog();
    BlogTag tag1 = new BlogTag();
    tag1.setName("SuperCallaFragalisticExpealadocious");
    newBlog.setTags(new BlogTag[] { tag1} );

    RavenJObject expected = RavenJObject.fromObject(newBlog);
    RavenJObject actual = new RavenJObject(expected);

    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void cloningTestsStoresValues() {

    RavenJObject f = new RavenJObject();
    f.add("test", new RavenJValue("Test"));
    f.add("2nd", new RavenJValue("second"));

    assertEquals(2, f.getCount());
  }

  @Test
  public void cloningTestsWorksCorrectly() {
    RavenJObject f = new RavenJObject();
    f.add("1", new RavenJValue(1));
    f.add("2", new RavenJValue(2));

    RavenJObject f1 = f.cloneToken();
    f1.add("2", new RavenJValue(3));

    RavenJValue val = (RavenJValue) f.get("2");
    assertEquals(2, val.getValue());
    val = (RavenJValue)f1.get("2");
    assertEquals(3, val.getValue());

    RavenJObject f2 = f1.cloneToken();
    val = (RavenJValue)f2.get("2");
    assertEquals(3, val.getValue());

    f.add("2", f2);
    f1 = f.cloneToken();
    f.remove("2");
    assertNull(f.get("2"));
    assertNotNull(f1.get("2"));
  }

  @Test
  public void changingValuesOfParent() {
    RavenJObject obj = RavenJObject.parse(" { 'Me': { 'ObjectID': 1} }");
    RavenJObject obj2 = obj.cloneToken();
    RavenJObject obj3 = obj.cloneToken();

    RavenJObject o = obj2.value(RavenJObject.class, "Me");
    o.add("ObjectID", new RavenJValue(2));

    obj3.value(RavenJObject.class, "Me").add("ObjectID", new RavenJValue(3));
    assertEquals(1, (int) obj.value(RavenJObject.class, "Me").value(Integer.TYPE, "ObjectID"));
    assertEquals(2, (int) obj2.value(RavenJObject.class, "Me").value(Integer.TYPE, "ObjectID"));
    assertEquals(3, (int) obj3.value(RavenJObject.class, "Me").value(Integer.TYPE, "ObjectID"));
  }

  @Test
  public void shouldNotFail() {
    RavenJObject root = new RavenJObject();
    RavenJObject current = root;
    for (int i = 0; i < 10000; i++)
    {
      RavenJObject temp = new RavenJObject();
      current.add("Inner", temp);
      current = temp;
    }

    RavenJObject anotherRoot = root.cloneToken();
    do
    {
      anotherRoot.add("Inner", new RavenJValue(0));
      RavenJToken jToken = anotherRoot.get("Inner");
      if (jToken instanceof RavenJObject) {
        anotherRoot = (RavenJObject) jToken;
      } else {
        anotherRoot = null;
      }
    } while (anotherRoot != null);
  }

  @Test
  public void shouldBehaveNicelyInMultithreaded() {
    RavenJObject obj = new RavenJObject();
    obj.add("prop1", new RavenJValue(2));
    obj.add("prop2", new RavenJValue("123"));

    final RavenJObject copy = obj.cloneToken();
    copy.add("@id", new RavenJValue("movies/1"));

    Parallel.For(0, 10000, new LoopBody<Integer>() {

      @Override
      public void run(Integer i) {
        assertTrue(copy.containsKey("@id"));
        RavenJObject foo = copy.cloneToken();
        assertTrue(foo.containsKey("@id"));
        assertTrue(copy.containsKey("@id"));

      }
    });
  }


}

package raven.tests.document;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;

//TODO: write generic test for find if all tests ends with "Test"
public class DocumentIdTest extends RemoteClientTest {

  @Test
  public void withSynchronousApiIdsAreGeneratedOnStore() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        TestObject obj = new TestObject();
        obj.setName("Test object");
        session.store(obj);
        assertNotNull(obj.getId());
      }
    }
  }

  public static class TestObject {
    private String id;
    private String name;
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

  }
}

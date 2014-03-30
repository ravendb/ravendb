package net.ravendb.tests.document;

import static org.junit.Assert.assertNotNull;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;

import org.junit.Test;


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

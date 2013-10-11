package raven.tests.issues;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import raven.abstractions.closure.Action1;
import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.BulkInsertOperation;
import raven.client.document.DocumentStore;


public class BulkInsertClientTest extends RemoteClientTest {
  @Test
  public void canCreateAndDisposeUsingBulk() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (BulkInsertOperation bulkInsert = store.bulkInsert()) {
        bulkInsert.setReport(new Action1<String>() {
          @Override
          public void apply(String msg) {
            System.out.println(msg);
          }
        });
        User user = new User();
        user.setName("Fitzchak");
        bulkInsert.store(user);
      }

      try (IDocumentSession session = store.openSession()) {
        User user = session.load(User.class, "users/1");
        assertNotNull(user);
        assertEquals("Fitzchak", user.getName());
      }
    }
  }

  @SuppressWarnings("unused")
  private static class User {
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
  //FIXME: other tests

}

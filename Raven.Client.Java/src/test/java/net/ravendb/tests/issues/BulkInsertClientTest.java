package net.ravendb.tests.issues;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import net.ravendb.abstractions.closure.Action1;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.BulkInsertOperation;
import net.ravendb.client.document.DocumentStore;

import org.junit.Test;



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

}

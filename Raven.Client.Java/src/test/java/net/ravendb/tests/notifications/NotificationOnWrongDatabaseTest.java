package net.ravendb.tests.notifications;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;

import org.junit.Test;



public class NotificationOnWrongDatabaseTest extends RemoteClientTest {

  @Test(expected = RuntimeException.class)
  public void shouldNotCrashServer() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        store.changes("does-not-exists");
      }
    }
  }
}

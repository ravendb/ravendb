package raven.tests.notifications;

import org.junit.Test;

import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;


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

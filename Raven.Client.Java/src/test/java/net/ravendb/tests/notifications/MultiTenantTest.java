package net.ravendb.tests.notifications;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.data.DocumentChangeNotification;
import net.ravendb.abstractions.data.DocumentChangeTypes;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.utils.Observers;

import org.junit.Test;


public class MultiTenantTest extends RemoteClientTest {

  @Test
  public void canGetNotificationsFromTenant_ExplicitDatabase() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      final BlockingQueue<DocumentChangeNotification> list = new ArrayBlockingQueue<>(20);

      store.changes(getDefaultDb()).forDocument("items/1").subscribe(Observers.create(new Action1<DocumentChangeNotification>() {
        @Override
        public void apply(DocumentChangeNotification value) {
          list.add(value);
        }
      }));

      try (IDocumentSession session = store.openSession()) {
       session.store(new ClientServerTest.Item());
       session.saveChanges();
      }

      DocumentChangeNotification documentChangeNotification = list.poll(15, TimeUnit.SECONDS);
      assertNotNull(documentChangeNotification);

      assertEquals("items/1", documentChangeNotification.getId());
      assertEquals(documentChangeNotification.getType(), DocumentChangeTypes.PUT);

    }
  }

  @Test
  public void canGetNotificationsFromTenant_AndNotFromAnother() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.getDatabaseCommands().getGlobalAdmin().ensureDatabaseExists("test");
      store.getDatabaseCommands().getGlobalAdmin().ensureDatabaseExists("another");
      final BlockingQueue<DocumentChangeNotification> list = new ArrayBlockingQueue<>(20);

      store.changes("test").forDocument("items/1").subscribe(Observers.create(new Action1<DocumentChangeNotification>() {
        @Override
        public void apply(DocumentChangeNotification change) {
          list.add(change);
        }
      }));

      try (IDocumentSession session = store.openSession("another")) {
        session.store(new ClientServerTest.Item(), "items/2");
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession("test")) {
        session.store(new ClientServerTest.Item(), "items/1");
        session.saveChanges();
      }

      DocumentChangeNotification documentChangeNotification = list.poll(15, TimeUnit.SECONDS);
      assertNotNull(documentChangeNotification);

      assertEquals("items/1", documentChangeNotification.getId());
      assertEquals(DocumentChangeTypes.PUT, documentChangeNotification.getType());

      documentChangeNotification = list.poll(1, TimeUnit.SECONDS);
      assertNull(documentChangeNotification);


    }
  }
}

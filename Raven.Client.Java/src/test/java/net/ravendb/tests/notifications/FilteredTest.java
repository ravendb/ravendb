package net.ravendb.tests.notifications;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.data.DocumentChangeNotification;
import net.ravendb.abstractions.data.DocumentChangeTypes;
import net.ravendb.abstractions.data.IndexChangeNotification;
import net.ravendb.abstractions.data.IndexChangeTypes;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.changes.IDatabaseChanges;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.utils.Observers;


public class FilteredTest extends RemoteClientTest {

  @Test
  public void canGetNotificationAboutIndexUpdate() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      final BlockingQueue<IndexChangeNotification> list = new ArrayBlockingQueue<>(20);

      IDatabaseChanges task = store.changes();
      task.forIndex("Raven/DocumentsByEntityName").subscribe(Observers.create(new Action1<IndexChangeNotification>() {
        @Override
        public void apply(IndexChangeNotification first) {
          list.add(first);
        }
      }));

      try (IDocumentSession session = store.openSession()) {
        session.store(new ClientServerTest.Item(), "items/1");
        session.saveChanges();
      }

      IndexChangeNotification indexChangeNotification = list.poll(2, TimeUnit.SECONDS);
      assertNotNull(indexChangeNotification);

      assertEquals("Raven/DocumentsByEntityName", indexChangeNotification.getName());
      assertEquals(IndexChangeTypes.MAP_COMPLETED, indexChangeNotification.getType());
    }
  }

  @Test
  public void canGetById() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      final BlockingQueue<DocumentChangeNotification> list = new ArrayBlockingQueue<>(20);

      IDatabaseChanges task = store.changes();
      task.forDocumentsStartingWith("items").subscribe(Observers.create(new Action1<DocumentChangeNotification>() {
        @Override
        public void apply(DocumentChangeNotification first) {
          list.add(first);
        }
      }));

      try (IDocumentSession session = store.openSession()) {
        session.store(new ClientServerTest.Item(), "seeks/1");
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        session.store(new ClientServerTest.Item(), "items/1");
        session.saveChanges();
      }

      DocumentChangeNotification documentChangeNotification = list.poll(2, TimeUnit.SECONDS);
      assertNotNull(documentChangeNotification);

      assertEquals("items/1", documentChangeNotification.getId());
      assertEquals(DocumentChangeTypes.PUT, documentChangeNotification.getType());

    }
  }
}

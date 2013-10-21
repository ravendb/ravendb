package net.ravendb.tests.notifications;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.closure.Predicate;
import net.ravendb.abstractions.data.DocumentChangeNotification;
import net.ravendb.abstractions.data.DocumentChangeTypes;
import net.ravendb.abstractions.data.IndexChangeNotification;
import net.ravendb.abstractions.data.IndexChangeTypes;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.changes.IDatabaseChanges;
import net.ravendb.client.changes.IObservable;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.document.FailoverBehavior;
import net.ravendb.client.document.FailoverBehaviorSet;
import net.ravendb.client.utils.Observers;

import org.junit.Test;



public class ClientServerTest extends RemoteClientTest {
  public static class Item {
    //empty by design
  }

  @Test
  public void canGetNotificationAboutDocumentPut() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.getConventions().setFailoverBehavior(new FailoverBehaviorSet(FailoverBehavior.FAIL_IMMEDIATELY));

      final BlockingQueue<DocumentChangeNotification> list = new ArrayBlockingQueue<>(20);
      IDatabaseChanges taskObservable = store.changes(getDefaultDb());
      IObservable<DocumentChangeNotification> observable = taskObservable.forDocument("items/1");
      observable.subscribe(Observers.create(new Action1<DocumentChangeNotification>() {
        @Override
        public void apply(DocumentChangeNotification msg) {
          list.add(msg);
        }
      }));
      try (IDocumentSession session = store.openSession()) {
        session.store(new Item());
        session.saveChanges();
      }

      DocumentChangeNotification documentChangeNotification = list.poll(15, TimeUnit.SECONDS);
      assertNotNull(documentChangeNotification);

      assertEquals("items/1", documentChangeNotification.getId());
      assertEquals(documentChangeNotification.getType(), DocumentChangeTypes.PUT);
    }
  }

  @Test
  public void canGetNotificationAboutDocumentDelete() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      final BlockingQueue<DocumentChangeNotification> list = new ArrayBlockingQueue<>(20);
      IDatabaseChanges taskObservable = store.changes(getDefaultDb());
      IObservable<DocumentChangeNotification> observable = taskObservable.forDocument("items/1");
      observable.where(new Predicate<DocumentChangeNotification>() {
        @Override
        public Boolean apply(DocumentChangeNotification input) {
          return input.getType().equals(DocumentChangeTypes.DELETE);
        }
      }).subscribe(Observers.create(new Action1<DocumentChangeNotification>() {
        @Override
        public void apply(DocumentChangeNotification msg) {
          list.add(msg);
        }
      }));

      try (IDocumentSession session = store.openSession()) {
        session.store(new Item());
        session.saveChanges();
      }

      store.getDatabaseCommands().delete("items/1", null);

      DocumentChangeNotification documentChangeNotification = list.poll(15, TimeUnit.SECONDS);
      assertNotNull(documentChangeNotification);

      assertEquals("items/1", documentChangeNotification.getId());
      assertEquals(documentChangeNotification.getType(), DocumentChangeTypes.DELETE);
    }
  }

  @Test
  public void canGetNotificationAboutDocumentIndexUpdate() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      final BlockingQueue<IndexChangeNotification> list = new ArrayBlockingQueue<>(20);
      IDatabaseChanges taskObservable = store.changes(getDefaultDb());
      IObservable<IndexChangeNotification> observable = taskObservable.forIndex("Raven/DocumentsByEntityName");
      observable.subscribe(Observers.create(new Action1<IndexChangeNotification>() {
        @Override
        public void apply(IndexChangeNotification msg) {
          list.add(msg);
        }
      }));

      try (IDocumentSession session = store.openSession()) {
        session.store(new Item());
        session.saveChanges();
      }

      IndexChangeNotification indexChangeNotification = list.poll(15, TimeUnit.SECONDS);
      assertNotNull(indexChangeNotification);

      assertEquals("Raven/DocumentsByEntityName", indexChangeNotification.getName());
      assertEquals(indexChangeNotification.getType(), IndexChangeTypes.MAP_COMPLETED);

    }

  }

}

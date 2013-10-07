package raven.tests.notifications;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.EnumSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import raven.abstractions.closure.Action1;
import raven.abstractions.closure.Predicate;
import raven.abstractions.data.DocumentChangeNotification;
import raven.abstractions.data.DocumentChangeTypes;
import raven.abstractions.data.IndexChangeNotification;
import raven.abstractions.data.IndexChangeTypes;
import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.changes.IDatabaseChanges;
import raven.client.changes.IObservable;
import raven.client.document.DocumentStore;
import raven.client.document.FailoverBehavior;
import raven.client.utils.Observers;


public class ClientServerTest extends RemoteClientTest {
  public static class Item {
    //empty by design
  }

  @Test
  public void canGetNotificationAboutDocumentPut() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.getConventions().setFailoverBehavior(EnumSet.of(FailoverBehavior.FAIL_IMMEDIATELY));

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

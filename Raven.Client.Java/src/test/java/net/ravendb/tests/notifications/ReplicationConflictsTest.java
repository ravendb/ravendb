package net.ravendb.tests.notifications;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.JsonDocument;
import net.ravendb.abstractions.data.ReplicationConflictNotification;
import net.ravendb.abstractions.data.ReplicationConflictTypes;
import net.ravendb.abstractions.data.ReplicationOperationTypes;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.changes.IDatabaseChanges;
import net.ravendb.client.changes.IObservable;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.document.FailoverBehavior;
import net.ravendb.client.document.FailoverBehaviorSet;
import net.ravendb.client.exceptions.ConflictException;
import net.ravendb.client.utils.Observers;
import net.ravendb.tests.bundles.replication.ReplicationBase;

import org.junit.Test;



public class ReplicationConflictsTest extends ReplicationBase {
  @Test
  public void canGetNotificationsAboutConflictedDocuments() throws Exception {
    try (IDocumentStore store1 = createStore();
      IDocumentStore store2 = createStore()) {

      store1.getDatabaseCommands().put("users/1", null,
        RavenJObject.parse("{\"Name\": \"Ayende\" }"),
        new RavenJObject());

      store2.getDatabaseCommands().put("users/1", null,
        RavenJObject.parse("{\"Name\": \"Rahien\" }"),
        new RavenJObject());

      final BlockingQueue<ReplicationConflictNotification> list = new ArrayBlockingQueue<>(20);

      IDatabaseChanges changes = store2.changes();
      IObservable<ReplicationConflictNotification> replicationConflicts = changes.forAllReplicationConflicts();
      replicationConflicts.subscribe(Observers.create(new Action1<ReplicationConflictNotification>()  {
        @Override
        public void apply(ReplicationConflictNotification notification) {
          list.add(notification);
        }
      }));

      tellFirstInstanceToReplicateToSecondInstance();

      ReplicationConflictNotification replicationConflictNotification = list.poll(15, TimeUnit.SECONDS);
      assertNotNull(replicationConflictNotification);

      assertEquals("users/1", replicationConflictNotification.getId());
      assertEquals(ReplicationConflictTypes.DOCUMENT_REPLICATION_CONFLICT, replicationConflictNotification.getItemType());
      assertEquals(2, replicationConflictNotification.getConflicts().length);
      assertEquals(ReplicationOperationTypes.PUT, replicationConflictNotification.getOperationType());

      Etag conflictedEtag = null;

      try {
        store2.getDatabaseCommands().get("users/1");
      } catch (ConflictException e) {
        conflictedEtag = e.getEtag();
      }
      assertEquals(conflictedEtag, replicationConflictNotification.getEtag());
    }
  }

  @Test
  public void canGetNotificationsAboutConflictedAttachements() throws Exception {
    try (IDocumentStore store1 = createStore();
      IDocumentStore store2 = createStore()) {
      store1.getDatabaseCommands().putAttachment("attachment/1", null, new ByteArrayInputStream(new byte[] { 1,2,3}), new RavenJObject());
      store2.getDatabaseCommands().putAttachment("attachment/1", null, new ByteArrayInputStream(new byte[] { 1,2,3}), new RavenJObject());


      final BlockingQueue<ReplicationConflictNotification> list = new ArrayBlockingQueue<>(20);

      IDatabaseChanges changes = store2.changes();
      IObservable<ReplicationConflictNotification> replicationConflicts = changes.forAllReplicationConflicts();
      replicationConflicts.subscribe(Observers.create(new Action1<ReplicationConflictNotification>()  {
        @Override
        public void apply(ReplicationConflictNotification notification) {
          list.add(notification);
        }
      }));

      tellFirstInstanceToReplicateToSecondInstance();

      ReplicationConflictNotification replicationConflictNotification = list.poll(15, TimeUnit.SECONDS);
      assertNotNull(replicationConflictNotification);

      assertEquals("attachment/1", replicationConflictNotification.getId());
      assertEquals(ReplicationConflictTypes.ATTACHMENT_REPLICATION_CONFLICT, replicationConflictNotification.getItemType());
      assertEquals(2, replicationConflictNotification.getConflicts().length);
      assertEquals(ReplicationOperationTypes.PUT, replicationConflictNotification.getOperationType());

    }
  }

  @Test
  public void notificationShouldContainAllConfictedIds() throws Exception {
    try (IDocumentStore store1 = createStore();
      IDocumentStore store2 = createStore();
      IDocumentStore store3 = createStore()) {
      store1.getDatabaseCommands().put("users/1", null,
        RavenJObject.parse("{\"Name\": \"Ayende\" }"),
        new RavenJObject());

      store2.getDatabaseCommands().put("users/1", null,
        RavenJObject.parse("{\"Name\": \"Rahien\" }"),
        new RavenJObject());

      store3.getDatabaseCommands().put("users/1", null,
        RavenJObject.parse("{\"Name\": \"Rahien\" }"),
        new RavenJObject());


      final BlockingQueue<ReplicationConflictNotification> list = new ArrayBlockingQueue<>(20);

      IDatabaseChanges changes = store3.changes();
      IObservable<ReplicationConflictNotification> replicationConflicts = changes.forAllReplicationConflicts();
      replicationConflicts.subscribe(Observers.create(new Action1<ReplicationConflictNotification>()  {
        @Override
        public void apply(ReplicationConflictNotification notification) {
          list.add(notification);
        }
      }));

      tellInstanceToReplicateToAnotherInstance(0, 2); // will create conflict on 3

      ReplicationConflictNotification replicationConflictNotification = list.poll(15, TimeUnit.SECONDS);
      assertNotNull(replicationConflictNotification);

      assertEquals("users/1", replicationConflictNotification.getId());
      assertEquals(ReplicationConflictTypes.DOCUMENT_REPLICATION_CONFLICT, replicationConflictNotification.getItemType());
      assertEquals(2, replicationConflictNotification.getConflicts().length);

      tellInstanceToReplicateToAnotherInstance(1, 2); // will add another conflicted document on 3
      replicationConflictNotification = list.poll(15, TimeUnit.SECONDS);
      assertNotNull(replicationConflictNotification);

      assertEquals("users/1", replicationConflictNotification.getId());
      assertEquals(ReplicationConflictTypes.DOCUMENT_REPLICATION_CONFLICT, replicationConflictNotification.getItemType());
      assertEquals(3, replicationConflictNotification.getConflicts().length);
      assertEquals(ReplicationOperationTypes.PUT, replicationConflictNotification.getOperationType());
    }
  }

  @Test
  public void canGetNotificationsWhenDeleteReplicationCausesConflict() throws Exception {
    try (IDocumentStore store1 = createStore();
      IDocumentStore store2 = createStore()) {

      store1.getDatabaseCommands().put("users/1", null,
        RavenJObject.parse("{\"Name\": \"Ayende\" }"),
        new RavenJObject());

      store2.getDatabaseCommands().put("users/1", null,
        RavenJObject.parse("{\"Name\": \"Rahien\" }"),
        new RavenJObject());

      store1.getDatabaseCommands().delete("users/1", null);

      final BlockingQueue<ReplicationConflictNotification> list = new ArrayBlockingQueue<>(20);

      IDatabaseChanges changes = store2.changes();
      IObservable<ReplicationConflictNotification> replicationConflicts = changes.forAllReplicationConflicts();
      replicationConflicts.subscribe(Observers.create(new Action1<ReplicationConflictNotification>()  {
        @Override
        public void apply(ReplicationConflictNotification notification) {
          list.add(notification);
        }
      }));

      tellFirstInstanceToReplicateToSecondInstance();

      ReplicationConflictNotification replicationConflictNotification = list.poll(15, TimeUnit.SECONDS);
      assertNotNull(replicationConflictNotification);

      assertEquals("users/1", replicationConflictNotification.getId());
      assertEquals(ReplicationConflictTypes.DOCUMENT_REPLICATION_CONFLICT, replicationConflictNotification.getItemType());
      assertEquals(2, replicationConflictNotification.getConflicts().length);
      assertEquals(ReplicationOperationTypes.DELETE, replicationConflictNotification.getOperationType());

    }
  }

  @Test
  public void notificationShouldContainAllConflictedIdsOfReplicatedDeletes() throws Exception {

    try (IDocumentStore store1 = createStore();
      IDocumentStore store2 = createStore();
      IDocumentStore store3 = createStore()) {
      store1.getDatabaseCommands().put("users/1", null,
        RavenJObject.parse("{\"Name\": \"Ayende\" }"),
        new RavenJObject());

      store2.getDatabaseCommands().put("users/1", null,
        RavenJObject.parse("{\"Name\": \"Rahien\" }"),
        new RavenJObject());

      store3.getDatabaseCommands().put("users/1", null,
        RavenJObject.parse("{\"Name\": \"Rahien\" }"),
        new RavenJObject());

      store1.getDatabaseCommands().delete("users/1", null);
      store2.getDatabaseCommands().delete("users/1", null);

      final BlockingQueue<ReplicationConflictNotification> list = new ArrayBlockingQueue<>(20);

      IDatabaseChanges changes = store3.changes();
      IObservable<ReplicationConflictNotification> replicationConflicts = changes.forAllReplicationConflicts();
      replicationConflicts.subscribe(Observers.create(new Action1<ReplicationConflictNotification>()  {
        @Override
        public void apply(ReplicationConflictNotification notification) {
          list.add(notification);
        }
      }));

      tellInstanceToReplicateToAnotherInstance(0, 2); // will create conflict on 3

      ReplicationConflictNotification replicationConflictNotification = list.poll(15, TimeUnit.SECONDS);
      assertNotNull(replicationConflictNotification);

      assertEquals("users/1", replicationConflictNotification.getId());
      assertEquals(ReplicationConflictTypes.DOCUMENT_REPLICATION_CONFLICT, replicationConflictNotification.getItemType());
      assertEquals(2, replicationConflictNotification.getConflicts().length);
      assertEquals(ReplicationOperationTypes.DELETE, replicationConflictNotification.getOperationType());

      tellInstanceToReplicateToAnotherInstance(1, 2); // will add another conflicted document on 3
      replicationConflictNotification = list.poll(15, TimeUnit.SECONDS);
      assertNotNull(replicationConflictNotification);

      assertEquals("users/1", replicationConflictNotification.getId());
      assertEquals(ReplicationConflictTypes.DOCUMENT_REPLICATION_CONFLICT, replicationConflictNotification.getItemType());
      assertEquals(3, replicationConflictNotification.getConflicts().length);
      assertEquals(ReplicationOperationTypes.DELETE, replicationConflictNotification.getOperationType());
    }
  }

  @Test
  public void conflictShouldBeResolvedByRegisiteredConflictListenerWhenNotificationArrives() throws Exception {
    try (IDocumentStore store1 = createStore();
      IDocumentStore store2 = createStore()) {

      store2.getConventions().setFailoverBehavior(new FailoverBehaviorSet(FailoverBehavior.FAIL_IMMEDIATELY));

      store1.getDatabaseCommands().put("users/1", null,
        RavenJObject.parse("{\"Name\": \"Ayende\" }"),
        new RavenJObject());

      store2.getDatabaseCommands().put("users/1", null,
        RavenJObject.parse("{\"Name\": \"Rahien\" }"),
        new RavenJObject());

      ((DocumentStore)store2).registerListener(new ClientSideConflictResolution());


      final BlockingQueue<ReplicationConflictNotification> list = new ArrayBlockingQueue<>(20);

      IDatabaseChanges changes = store2.changes();
      IObservable<ReplicationConflictNotification> replicationConflicts = changes.forAllReplicationConflicts();
      replicationConflicts.subscribe(Observers.create(new Action1<ReplicationConflictNotification>()  {
        @Override
        public void apply(ReplicationConflictNotification notification) {
          list.add(notification);
        }
      }));

      tellFirstInstanceToReplicateToSecondInstance();

      ReplicationConflictNotification replicationConflictNotification = list.poll(15, TimeUnit.SECONDS);
      assertNotNull(replicationConflictNotification);

      boolean conflictedDocumentsDeleted = false;

      for (int i = 0; i < retriesCount; i++) {
        JsonDocument document1 = store2.getDatabaseCommands().get(replicationConflictNotification.getConflicts()[0]);
        JsonDocument document2 = store2.getDatabaseCommands().get(replicationConflictNotification.getConflicts()[1]);

        if (document1 == null && document2 ==null) {
          conflictedDocumentsDeleted = true;
          break;
        }
        Thread.sleep(200);
      }

      assertTrue(conflictedDocumentsDeleted);

      JsonDocument jsonDocument = store2.getDatabaseCommands().get("users/1");
      assertEquals("Ayende Rahien", jsonDocument.getDataAsJson().value(String.class, "Name"));

    }
  }

}

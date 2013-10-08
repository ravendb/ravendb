package raven.tests.bundles.replication;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;

import org.junit.Assert;
import org.junit.Test;

import raven.abstractions.data.Attachment;
import raven.abstractions.json.linq.RavenJObject;
import raven.client.IDocumentStore;
import raven.client.connection.IDatabaseCommands;
import raven.client.exceptions.ConflictException;

public class AttachmentReplicationBugsTest extends ReplicationBase {

  @Test
  public void can_replicate_documents_between_two_external_instances() throws Exception {

    try (IDocumentStore store1 = createStore();
      IDocumentStore store2 = createStore()) {
      tellFirstInstanceToReplicateToSecondInstance();
      Thread.sleep(1000);
      IDatabaseCommands databaseCommands = store1.getDatabaseCommands();
      int documentCount = 20;
      for (int i = 0; i < documentCount; i++) {
        databaseCommands.putAttachment(i + "", null, new ByteArrayInputStream(new byte[] {(byte) i}), new RavenJObject());
      }

      boolean foundAll = false;
      for (int i = 0; i < retriesCount; i++) {
        int countFound = 0;
        for (int j = 0; j < documentCount; j++) {
          Attachment attachment = store2.getDatabaseCommands().getAttachment(j + "");
          if (attachment == null) break;
          countFound++;
        }
        foundAll = countFound == documentCount;
        if (foundAll) break;
        Thread.sleep(100);
      }
      Assert.assertTrue(foundAll);

    }
  }

  @Test
  public void can_resolve_conflict_with_delete() throws Exception {

    try (IDocumentStore store1 = createStore();
      IDocumentStore store2 = createStore()) {

      store1.getDatabaseCommands().putAttachment("static", null, new ByteArrayInputStream(new byte[] {(byte) 1}),
        new RavenJObject());
      store2.getDatabaseCommands().putAttachment("static", null, new ByteArrayInputStream(new byte[] {(byte) 1}),
        new RavenJObject());

      tellFirstInstanceToReplicateToSecondInstance();

      try {
        for (int i = 0; i < retriesCount; i++) {
          store2.getDatabaseCommands().getAttachment("static");
          Thread.sleep(100);
        }
        fail();
      } catch (ConflictException e) {
        store2.getDatabaseCommands().deleteAttachment("static", null);

        for (String conflictedVersionId : e.getConflictedVersionIds()) {
          Assert.assertNull(store2.getDatabaseCommands().getAttachment(conflictedVersionId));
        }
      }

    }
  }


}

package net.ravendb.tests.bundles.replication;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;

import net.ravendb.abstractions.data.Attachment;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.connection.IDatabaseCommands;
import net.ravendb.client.exceptions.ConflictException;

import org.junit.Assert;
import org.junit.Test;


public class AttachmentReplicationTest extends ReplicationBase {

  @SuppressWarnings("null")
  @Test
  public void can_replicate_between_two_instances() throws Exception {

    try (IDocumentStore store1 = createStore();
      IDocumentStore store2 = createStore()) {

      tellFirstInstanceToReplicateToSecondInstance();

      store1.getDatabaseCommands().putAttachment("ayende", null, new ByteArrayInputStream(new byte[] {1, 2, 3}),
        new RavenJObject());

      Attachment attachment = null;
      for (int i = 0; i < retriesCount; i++) {
        attachment = store2.getDatabaseCommands().getAttachment("ayende");
        if (attachment != null) break;
        Thread.sleep(100);
      }

      Assert.assertNotNull(attachment);
      Assert.assertArrayEquals(new byte[] {1, 2, 3}, attachment.getData());
    }
  }

  @Test
  public void can_replicate_large_number_of_documents_between_two_instances() throws Exception {

    try (IDocumentStore store1 = createStore();
      IDocumentStore store2 = createStore()) {

      tellFirstInstanceToReplicateToSecondInstance();

      IDatabaseCommands databaseCommands = store1.getDatabaseCommands();
      for (int i = 0; i < 150; i++) {
        databaseCommands.putAttachment(i + "", null, new ByteArrayInputStream(new byte[] {(byte) i}), new RavenJObject());
      }

      boolean foundAll = false;
      for (int i = 0; i < retriesCount; i++) {
        int countFound = 0;
        for (int j = 0; j < 150; j++) {
          Attachment attachment = store2.getDatabaseCommands().getAttachment(i + "");
          if (attachment == null) break;
          countFound++;
        }
        foundAll = countFound == 150;
        if (foundAll) break;
        Thread.sleep(100);
      }
      Assert.assertTrue(foundAll);
    }
  }

  @Test
  public void can_replicate_delete_between_two_instances() throws Exception {

    try (IDocumentStore store1 = createStore();
      IDocumentStore store2 = createStore()) {

      tellFirstInstanceToReplicateToSecondInstance();

      store1.getDatabaseCommands().putAttachment("ayende", null, new ByteArrayInputStream(new byte[] {2}),
        new RavenJObject());

      for (int i = 0; i < retriesCount; i++) {
        if (store2.getDatabaseCommands().getAttachment("ayende") != null) break;
        Thread.sleep(100);
      }
      Assert.assertNotNull(store2.getDatabaseCommands().getAttachment("ayende"));

      store1.getDatabaseCommands().deleteAttachment("ayende", null);

      for (int i = 0; i < retriesCount; i++) {
        if (store2.getDatabaseCommands().getAttachment("ayende") == null) break;
        Thread.sleep(100);
      }
      Assert.assertNull(store2.getDatabaseCommands().getAttachment("ayende"));
    }
  }

  @Test
  public void when_replicating_and_an_attachment_is_already_there_will_result_in_conflict() throws Exception {
    try (IDocumentStore store1 = createStore();
      IDocumentStore store2 = createStore()) {

      store1.getDatabaseCommands().putAttachment("ayende", null, new ByteArrayInputStream(new byte[] {2}),
        new RavenJObject());
      store2.getDatabaseCommands().putAttachment("ayende", null, new ByteArrayInputStream(new byte[] {3}),
        new RavenJObject());

      tellFirstInstanceToReplicateToSecondInstance();
      Thread.sleep(1000);

      try {
        for (int i = 0; i < retriesCount; i++) {
          store2.getDatabaseCommands().getAttachment("ayende");
          fail();

        }
      } catch (ConflictException e) {
        Assert.assertEquals(
          "Conflict detected on ayende, conflict must be resolved before the attachment will be accessible",
          e.getMessage());
      }
    }
  }

  @Test
  public void when_replicating_and_an_attachment_is_already_there_will_result_in_conflict_and_can_get_all_conflicts()
    throws Exception {

    try (IDocumentStore store1 = createStore();
      IDocumentStore store2 = createStore()) {
      store1.getDatabaseCommands().putAttachment("ayende", null, new ByteArrayInputStream(new byte[] {2}),
        new RavenJObject());
      store2.getDatabaseCommands().putAttachment("ayende", null, new ByteArrayInputStream(new byte[] {3}),
        new RavenJObject());

      tellFirstInstanceToReplicateToSecondInstance();
      Thread.sleep(1000);

      try {
        for (int i = 0; i < retriesCount; i++) {
          store2.getDatabaseCommands().getAttachment("ayende");
          fail();

        }
      } catch (ConflictException e) {
        Assert.assertEquals(
          "Conflict detected on ayende, conflict must be resolved before the attachment will be accessible",
          e.getMessage());

        Assert.assertTrue(e.getConflictedVersionIds()[0].startsWith("ayende/conflicts/"));
        Assert.assertTrue(e.getConflictedVersionIds()[1].startsWith("ayende/conflicts/"));
      }
    }
  }

}

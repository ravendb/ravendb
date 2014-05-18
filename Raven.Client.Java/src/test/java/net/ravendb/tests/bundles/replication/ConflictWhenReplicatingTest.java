package net.ravendb.tests.bundles.replication;

import static org.junit.Assert.fail;

import java.io.Serializable;

import net.ravendb.abstractions.data.Constants;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.exceptions.ConflictException;

import org.junit.Assert;
import org.junit.Test;


public class ConflictWhenReplicatingTest extends ReplicationBase {

  @Test
  public void when_replicating_and_a_document_is_already_there_will_result_in_conflict() throws Exception {

    try (IDocumentStore store1 = createStore();
      IDocumentStore store2 = createStore()) {

      try (IDocumentSession session1 = store1.openSession()) {
        session1.store(new Company());
        session1.saveChanges();
      }

      try (IDocumentSession session2 = store2.openSession()) {
        session2.store(new Company());
        session2.saveChanges();
      }

      tellFirstInstanceToReplicateToSecondInstance();

      Thread.sleep(1000);

      try {
        for (int i = 0; i < retriesCount; i++) {
          try (IDocumentSession session = store2.openSession()) {
            session.load(Company.class, "companies/1");
            Thread.sleep(100);
          }
        }
        fail();
      } catch (ConflictException e) {
        Assert.assertEquals(
          "Conflict detected on companies/1, conflict must be resolved before the document will be accessible",
          e.getMessage());
      }
    }

  }

  @Test
  public void can_resolve_conflict_by_deleting_conflicted_doc() throws Exception {
    try (
      IDocumentStore store1 = createStore();
      IDocumentStore store2 = createStore()) {

      try (IDocumentSession session = store1.openSession()) {
        session.store(new Company());
        session.saveChanges();
      }

      try (IDocumentSession session = store2.openSession()) {
        session.store(new Company());
        session.saveChanges();
      }

      tellFirstInstanceToReplicateToSecondInstance();

      try {
        for (int i = 0; i < retriesCount; i++) {
          try(IDocumentSession session = store2.openSession()) {
            session.load(Company.class, "companies/1");
            Thread.sleep(100);
          }
        }
        fail();
      } catch (ConflictException e) {
        store2.getDatabaseCommands().delete("companies/1", null);

        for (String conflictedVersionId : e.getConflictedVersionIds()) {
          Assert.assertNull(store2.getDatabaseCommands().get(conflictedVersionId));
        }
      }
    }
  }

  @Test
  public void when_replicating_from_two_different_source_different_documents() throws Exception {
    try (IDocumentStore store1 = createStore();
      IDocumentStore store2 = createStore();
      IDocumentStore store3 = createStore()) {

      try (IDocumentSession session1 = store1.openSession()) {
        session1.store(new Company());
        session1.saveChanges();
      }

      try (IDocumentSession session2 = store2.openSession()) {
        session2.store(new Company());
        session2.saveChanges();
      }

      tellInstanceToReplicateToAnotherInstance(0, 2);

      for (int i = 0; i < retriesCount; i++) { // wait for it to show up in the 3rd server
        try (IDocumentSession session = store3.openSession()) {
          if (session.load(Company.class, "companies/1") != null) {
            break;
          }
          Thread.sleep(100);
        }
      }

      tellInstanceToReplicateToAnotherInstance(1, 2);

      try {
        for (int i = 0; i < retriesCount; i++) {
          try (IDocumentSession session3 = store3.openSession()) {
            session3.load(Company.class, "companies/1");
            Thread.sleep(100);
          }
        }
        fail();
      } catch (ConflictException e) {
        Assert.assertEquals(
          "Conflict detected on companies/1, conflict must be resolved before the document will be accessible",
          e.getMessage());
      }
    }
  }

  @Test
  public void can_conflict_on_deletes_as_well() throws Exception {

    try (IDocumentStore store1 = createStore();
      IDocumentStore store2 = createStore();
      IDocumentStore store3 = createStore()) {

      try (IDocumentSession session1 = store1.openSession()) {
        session1.store(new Company());
        session1.saveChanges();
      }

      try (IDocumentSession session2 = store2.openSession()) {
        session2.store(new Company());
        session2.saveChanges();
      }

      tellInstanceToReplicateToAnotherInstance(0, 2);

      for (int i = 0; i < retriesCount; i++) { // wait for it to show up in the 3rd server

        try (IDocumentSession session3 = store3.openSession()) {
          if (session3.load(Company.class, "companies/1") != null) {
            break;
          }
          Thread.sleep(100);
        }
      }

      try (IDocumentSession session = store1.openSession()) {
        session.delete(session.load(Company.class, "companies/1"));
        session.saveChanges();
      }

      for (int i = 0; i < retriesCount; i++)  { // wait for it to NOT show up in the 3rd server
        try (IDocumentSession session = store3.openSession()) {
          if (session.load(Company.class, "companies/1") == null) {
            break;
          }
          Thread.sleep(100);
        }
      }

      tellInstanceToReplicateToAnotherInstance(1, 2);

      Thread.sleep(1000);
      try {
        for (int i = 0; i < retriesCount; i++) {
          try (IDocumentSession session = store3.openSession()) {
            session.load(Company.class, "companies/1");
            Thread.sleep(100);
          }
        }
        fail();
      } catch (ConflictException e) {
        Assert.assertEquals(
          "Conflict detected on companies/1, conflict must be resolved before the document will be accessible",
          e.getMessage());
      }
    }

  }

  public void tombstone_deleted_after_conflict_resolved() throws Exception  {
    try (
      IDocumentStore store1 = createStore();
      IDocumentStore store2 = createStore()
      ) {

      try (IDocumentSession session1 = store1.openSession()) {
        session1.store(new Company());
        session1.saveChanges();
      }

      tellFirstInstanceToReplicateToSecondInstance();
      Company company = null;
      for (int i = 0; i < retriesCount; i++) {
        try (IDocumentSession session2 = store2.openSession()) {
          company = session2.load(Company.class, "companies/1");
          if (company != null){
            break;
          }
          Thread.sleep(100);
        }
      }
      Assert.assertNotNull(company);

      //Stop replication
      store1.getDatabaseCommands().delete(Constants.RAVEN_REPLICATION_DESTINATIONS, null);
      Assert.assertNull(store1.getDatabaseCommands().get(Constants.RAVEN_REPLICATION_DESTINATIONS));

      try (IDocumentSession session1 = store1.openSession()) {
        company = session1.load(Company.class, "companies/1");
        company.setName("Raven");
        session1.saveChanges();
      }


      try (IDocumentSession session2 = store2.openSession()) {
        session2.delete(session2.load(Company.class, "companies/1"));
        session2.saveChanges();
      }
    }
  }

  public static class Company implements Serializable {

    public String name;
    public String id;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

  }

}

package net.ravendb.tests.multiget;

import static org.junit.Assert.assertEquals;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RavenQueryStatistics;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.bugs.User;

import org.junit.Test;


public class BugsTest extends RemoteClientTest {

  @Test
  public void canUseStats() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("Ayende");
        session.store(user1);

        User user2 = new User();
        user2.setName("Oren");
        session.store(user2);

        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        Reference<RavenQueryStatistics> stats = new Reference<>();
        session.query(User.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .statistics(stats)
          .lazily();

        session.advanced().eagerly().executeAllPendingLazyOperations();

        assertEquals(2, stats.value.getTotalResults());
      }
    }
  }
}

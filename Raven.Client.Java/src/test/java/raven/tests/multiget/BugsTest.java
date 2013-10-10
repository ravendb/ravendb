package raven.tests.multiget;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import raven.abstractions.basic.Reference;
import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RavenQueryStatistics;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentQueryCustomizationFactory;
import raven.client.document.DocumentStore;
import raven.tests.bugs.User;

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

package net.ravendb.tests.multiget;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import net.ravendb.abstractions.basic.Lazy;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.bugs.QUser;
import net.ravendb.tests.bugs.User;


public class MultiGetMultiTenantTest extends RemoteClientTest {


  @Test
  public void canUseLazyWithMultiTenancy() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("oren");
        User user2 = new User();
        user2.setName("ayende");
        session.store(user1);
        session.store(user2);
        session.store(new User());
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        Lazy<User> result1 = session.advanced().lazily().load(User.class, "users/1");
        Lazy<User> result2 = session.advanced().lazily().load(User.class, "users/2");
        assertNotNull(result1.getValue());
        assertNotNull(result2.getValue());
      }
    }
  }

  @Test
  public void canCacheLazyQueryResults() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("oren");
        User user2 = new User();
        user2.setName("ayende");
        session.store(user1);
        session.store(new User());
        session.store(user2);
        session.saveChanges();
      }

      QUser x = QUser.user;

      try (IDocumentSession session = store.openSession()) {
        session.query(User.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
        .where(x.name.eq("Test")).toList();
      }

      try (IDocumentSession session = store.openSession()) {
        Lazy<List<User>> result1 = session.query(User.class).where(x.name.eq("oren")).lazily();
        Lazy<List<User>> result2 = session.query(User.class).where(x.name.eq("ayende")).lazily();

        assertNotNull(result2.getValue());

        assertEquals(1, session.advanced().getNumberOfRequests());
        assertTrue(result1.getValue().size() > 0);
        assertEquals(1, session.advanced().getNumberOfRequests());
        assertEquals(0, store.getJsonRequestFactory().getNumOfCachedRequests());
      }

      try (IDocumentSession session = store.openSession()) {
        Lazy<List<User>> result1 = session.query(User.class).where(x.name.eq("oren")).lazily();
        Lazy<List<User>> result2 = session.query(User.class).where(x.name.eq("ayende")).lazily();

        assertNotNull(result2.getValue());

        assertEquals(1, session.advanced().getNumberOfRequests());
        assertTrue(result1.getValue().size() > 0);
        assertEquals(1, session.advanced().getNumberOfRequests());

        assertEquals(2, store.getJsonRequestFactory().getNumOfCachedRequests());
      }
    }
  }

  @Test
  @Ignore("waiting for RavenDB-1665 Support for admin statistics is broken in 3.0")
  public void canAggressivelyCacheLoads() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb())) {
      store.getConventions().setShouldAggressiveCacheTrackChanges(false);
      store.initialize();

      try (IDocumentSession session = store.openSession()) {
        session.store(new User());
        session.store(new User());
        session.saveChanges();
      }

      waitForAllRequestsToComplete();

      int prevValue = getNumberOfRequests();

      for (int i = 0; i < 5; i++) {
        try (IDocumentSession session = store.openSession()) {
          try (AutoCloseable aggresiveCache = session.advanced().getDocumentStore().aggressivelyCacheFor(5 * 60 * 1000)) {
            session.advanced().lazily().load(User.class, "users/1");
            session.advanced().lazily().load(User.class, "users/2");

            session.advanced().eagerly().executeAllPendingLazyOperations();
          }
        }
      }

      assertNumberOfRequests(1, prevValue);
    }
  }

}

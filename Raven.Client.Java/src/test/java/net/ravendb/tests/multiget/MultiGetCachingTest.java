package net.ravendb.tests.multiget;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;


import net.ravendb.abstractions.basic.Lazy;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentConvention;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.bugs.QUser;
import net.ravendb.tests.bugs.User;

import org.junit.Ignore;
import org.junit.Test;


public class MultiGetCachingTest extends RemoteClientTest {

  @Test
  @Ignore("waiting for RavenDB-1665 Support for admin statistics is broken in 3.0")
  public void canAggressivelyCacheLoads() throws Exception {

    DocumentConvention conventions = new DocumentConvention();
    conventions.setShouldAggressiveCacheTrackChanges(false);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).withConventions(conventions).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(new User());
        session.store(new User());
        session.saveChanges();
      }

      waitForAllRequestsToComplete();
      int requests = getNumberOfRequests();

      for (int i = 0; i < 5; i++) {
        try (IDocumentSession session = store.openSession()) {
          try (AutoCloseable context = session.advanced().getDocumentStore().aggressivelyCacheFor(5 * 60 * 1000)) {
            session.advanced().lazily().load(User.class, "users/1");
            session.advanced().lazily().load(User.class, "users/2");

            session.advanced().eagerly().executeAllPendingLazyOperations();
          }
        }
      }

      waitForAllRequestsToComplete();
      assertNumberOfRequests(1, requests);

    }
  }

  @Test
  @Ignore("waiting for RavenDB-1665 Support for admin statistics is broken in 3.0")
  public void canAggressivelyCachePartOfMultiGet_SimpleFirst() throws Exception {

    DocumentConvention conventions = new DocumentConvention();
    conventions.setShouldAggressiveCacheTrackChanges(false);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).withConventions(conventions).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(new User());
        session.store(new User());
        session.saveChanges();
      }

      waitForAllRequestsToComplete();
      int requests = getNumberOfRequests();

      try (IDocumentSession session = store.openSession()) {
        try (AutoCloseable context = session.advanced().getDocumentStore().aggressivelyCacheFor(5 * 60 * 1000)) {
          session.load(User.class, new String[] { "users/1" });
        }
      }

      try (IDocumentSession session = store.openSession()) {
        try (AutoCloseable context = session.advanced().getDocumentStore().aggressivelyCacheFor(5 * 60 * 1000)) {
          session.advanced().lazily().load(User.class, new String[] { "users/1" });
          session.advanced().lazily().load(User.class, "users/2");
          session.advanced().eagerly().executeAllPendingLazyOperations();
        }
      }

      waitForAllRequestsToComplete();
      assertNumberOfRequests(2, requests);
      assertEquals(1, store.getJsonRequestFactory().getNumOfCachedRequests());

    }
  }

  @Test
  @Ignore("waiting for RavenDB-1665 Support for admin statistics is broken in 3.0")
  public void canAggressivelyCachePartOfMultiGet_DirectLoad() throws Exception {

    DocumentConvention conventions = new DocumentConvention();
    conventions.setShouldAggressiveCacheTrackChanges(false);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).withConventions(conventions).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(new User());
        session.store(new User());
        session.saveChanges();
      }

      waitForAllRequestsToComplete();
      int requests = getNumberOfRequests();

      try (IDocumentSession session = store.openSession()) {
        try (AutoCloseable context = session.advanced().getDocumentStore().aggressivelyCacheFor(5 * 60 * 1000)) {
          session.load(User.class, "users/1");
        }
      }

      try (IDocumentSession session = store.openSession()) {
        try (AutoCloseable context = session.advanced().getDocumentStore().aggressivelyCacheFor(5 * 60 * 1000)) {
          session.advanced().lazily().load(User.class, "users/1");
          session.advanced().lazily().load(User.class, "users/2");
          session.advanced().eagerly().executeAllPendingLazyOperations();
        }
      }

      waitForAllRequestsToComplete();
      assertNumberOfRequests(2, requests);
      assertEquals(1, store.getJsonRequestFactory().getNumOfCachedRequests());

    }
  }

  @Test
  @Ignore("waiting for RavenDB-1665 Support for admin statistics is broken in 3.0")
  public void canAggressivelyCachePartOfMultiGet_BatchFirst() throws Exception {

    DocumentConvention conventions = new DocumentConvention();
    conventions.setShouldAggressiveCacheTrackChanges(false);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).withConventions(conventions).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(new User());
        session.store(new User());
        session.saveChanges();
      }

      waitForAllRequestsToComplete();
      int requests = getNumberOfRequests();

      try (IDocumentSession session = store.openSession()) {
        try (AutoCloseable context = session.advanced().getDocumentStore().aggressivelyCacheFor(5 * 60 * 1000)) {
          session.advanced().lazily().load(User.class, new String[] { "users/1" });
          session.advanced().eagerly().executeAllPendingLazyOperations();
        }
      }

      try (IDocumentSession session = store.openSession()) {
        try (AutoCloseable context = session.advanced().getDocumentStore().aggressivelyCacheFor(5 * 60 * 1000)) {
          session.load(User.class, new String[] { "users/1" });
        }
      }

      waitForAllRequestsToComplete();
      assertNumberOfRequests(1, requests);
      assertEquals(1, store.getJsonRequestFactory().getNumOfCachedRequests());

    }
  }


  @Test
  public void canCacheLazyQueryResults() throws Exception {

    DocumentConvention conventions = new DocumentConvention();
    conventions.setShouldAggressiveCacheTrackChanges(false);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).withConventions(conventions).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User u = new User();
        u.setName("oren");
        session.store(u);
        session.store(new User());
        u = new User();
        u.setName("ayende");
        session.store(u);
        session.store(new User());
        session.saveChanges();
      }

      QUser u = new QUser("u");

      try (IDocumentSession session = store.openSession()) {
        session.query(User.class)
        .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
        .where(u.name.eq("test")).toList();
      }

      try (IDocumentSession session = store.openSession()) {
        Lazy<List<User>> result1 = session.query(User.class).where(u.name.eq("oren")).lazily();
        Lazy<List<User>> result2 = session.query(User.class).where(u.name.eq("ayende")).lazily();
        assertNotNull(result2.getValue());

        assertEquals(1, session.advanced().getNumberOfRequests());
        assertNotNull(result1.getValue());
        assertEquals(1, session.advanced().getNumberOfRequests());
        assertEquals(0, store.getJsonRequestFactory().getNumOfCachedRequests());
      }

      try (IDocumentSession session = store.openSession()) {
        Lazy<List<User>> result1 = session.query(User.class).where(u.name.eq("oren")).lazily();
        Lazy<List<User>> result2 = session.query(User.class).where(u.name.eq("ayende")).lazily();
        assertNotNull(result2.getValue());

        assertEquals(1, session.advanced().getNumberOfRequests());
        assertNotNull(result1.getValue());
        assertEquals(1, session.advanced().getNumberOfRequests());
        assertEquals(2, store.getJsonRequestFactory().getNumOfCachedRequests());
      }
    }
  }

  @Test
  public void canCacheLazyQueryAndMultiLoadResults() throws Exception {
    DocumentConvention conventions = new DocumentConvention();
    conventions.setShouldAggressiveCacheTrackChanges(false);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).withConventions(conventions).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User u = new User();
        u.setName("oren");
        session.store(u);
        session.store(new User());
        u = new User();
        u.setName("ayende");
        session.store(u);
        session.store(new User());
        session.saveChanges();
      }

      QUser u = new QUser("u");

      try (IDocumentSession session = store.openSession()) {
        session.query(User.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .where(u.name.eq("test")).toList();
      }

      try (IDocumentSession session = store.openSession()) {
        Lazy<User[]> items = session.advanced().lazily().load(User.class, "users/2", "users/4");
        Lazy<List<User>> result1 = session.query(User.class).where(u.name.eq("oren")).lazily();
        Lazy<List<User>> result2 = session.query(User.class).where(u.name.eq("ayende")).lazily();
        assertNotNull(result2.getValue());
        assertNotNull(items.getValue());
        assertEquals(1, session.advanced().getNumberOfRequests());
        assertNotNull(result1.getValue());
        assertEquals(1, session.advanced().getNumberOfRequests());
        assertEquals(0, store.getJsonRequestFactory().getNumOfCachedRequests());
      }

      try (IDocumentSession session = store.openSession()) {
        Lazy<User[]> items = session.advanced().lazily().load(User.class, "users/2", "users/4");
        Lazy<List<User>> result1 = session.query(User.class).where(u.name.eq("oren")).lazily();
        Lazy<List<User>> result2 = session.query(User.class).where(u.name.eq("ayende")).lazily();
        assertNotNull(result2.getValue());
        assertNotNull(items.getValue());
        assertEquals(1, session.advanced().getNumberOfRequests());
        assertNotNull(result1.getValue());
        assertEquals(1, session.advanced().getNumberOfRequests());
        assertEquals(3, store.getJsonRequestFactory().getNumOfCachedRequests());
      }
    }
  }

  @Test
  public void canMixCachingForBatchAndNonBatched_BatchedFirst() throws Exception {
    DocumentConvention conventions = new DocumentConvention();
    conventions.setShouldAggressiveCacheTrackChanges(false);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).withConventions(conventions).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User u = new User();
        u.setName("oren");
        session.store(u);
        session.store(new User());
        u = new User();
        u.setName("ayende");
        session.store(u);
        session.store(new User());
        session.saveChanges();
      }

      QUser u = new QUser("u");

      try (IDocumentSession session = store.openSession()) {
        session.query(User.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .where(u.name.eq("test")).toList();
      }


      try (IDocumentSession session = store.openSession()) {
        Lazy<List<User>> result1 = session.query(User.class).where(u.name.eq("oren")).lazily();
        Lazy<List<User>> result2 = session.query(User.class).where(u.name.eq("ayende")).lazily();
        assertNotNull(result2.getValue());
        assertEquals(1, session.advanced().getNumberOfRequests());
        assertNotNull(result1.getValue());
        assertEquals(1, session.advanced().getNumberOfRequests());
      }

      try (IDocumentSession session = store.openSession()) {
        session.query(User.class).where(u.name.eq("oren")).toList();
        session.query(User.class).where(u.name.eq("ayende")).toList();

        assertEquals(2, store.getJsonRequestFactory().getNumOfCachedRequests());
      }
    }
  }

  @Test
  public void canMixCachingForBatchAndNonBatched_IndividualFirst() throws Exception {
    DocumentConvention conventions = new DocumentConvention();
    conventions.setShouldAggressiveCacheTrackChanges(false);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).withConventions(conventions).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User u = new User();
        u.setName("oren");
        session.store(u);
        session.store(new User());
        u = new User();
        u.setName("ayende");
        session.store(u);
        session.store(new User());
        session.saveChanges();
      }

      QUser u = new QUser("u");

      try (IDocumentSession session = store.openSession()) {
        session.query(User.class)
         .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
         .where(u.name.eq("test")).toList();
      }

      try (IDocumentSession session = store.openSession()) {
        session.query(User.class).where(u.name.eq("oren")).toList();
        session.query(User.class).where(u.name.eq("ayende")).toList();
      }

      try (IDocumentSession session = store.openSession()) {
        Lazy<List<User>> result1 = session.query(User.class).where(u.name.eq("oren")).lazily();
        Lazy<List<User>> result2 = session.query(User.class).where(u.name.eq("ayende")).lazily();
        assertNotNull(result2.getValue());
        assertEquals(1, session.advanced().getNumberOfRequests());
        assertNotNull(result1.getValue());
        assertEquals(1, session.advanced().getNumberOfRequests());
        assertEquals(2, store.getJsonRequestFactory().getNumOfCachedRequests());
      }
    }
  }


}

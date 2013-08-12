package raven.tests.multiget;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentConvention;
import raven.client.document.DocumentStore;
import raven.tests.bugs.User;

public class MultiGetCachingTest extends RemoteClientTest {

  @Test
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
            session.advanced().lazily().lazyLoad(User.class, "users/1");
            session.advanced().lazily().lazyLoad(User.class, "users/2");

            session.advanced().eagerly().executeAllPendingLazyOperations();
          }
        }
      }

      waitForAllRequestsToComplete();
      assertNumberOfRequests(1, requests);

    }
  }

  @Test
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
          session.advanced().lazily().lazyLoad(User.class, new String[] { "users/1" });
          session.advanced().lazily().lazyLoad(User.class, "users/2");
          session.advanced().eagerly().executeAllPendingLazyOperations();
        }
      }

      waitForAllRequestsToComplete();
      assertNumberOfRequests(2, requests);
      assertEquals(1, store.getJsonRequestFactory().getNumOfCachedRequests());

    }
  }

  @Test
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
          session.advanced().lazily().lazyLoad(User.class, "users/1");
          session.advanced().lazily().lazyLoad(User.class, "users/2");
          session.advanced().eagerly().executeAllPendingLazyOperations();
        }
      }

      waitForAllRequestsToComplete();
      assertNumberOfRequests(2, requests);
      assertEquals(1, store.getJsonRequestFactory().getNumOfCachedRequests());

    }
  }

  @Test
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
          session.advanced().lazily().lazyLoad(User.class, new String[] { "users/1" });
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





  //TODO: other tests
}

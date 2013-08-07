package raven.tests.multiget;

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

      //TODO: WaitForAllRequestsToComplete(server);
      //TODO: server.Server.ResetNumberOfRequests();

      for (int i = 0; i < 5; i++) {
        try (IDocumentSession session = store.openSession()) {
          try (AutoCloseable context = session.advanced().getDocumentStore().aggressivelyCacheFor(5 * 60 * 1000)) {
            session.advanced().lazily().lazyLoad(User.class, "users/1");
            session.advanced().lazily().lazyLoad(User.class, "users/2");

            session.advanced().eagerly().executeAllPendingLazyOperations();
          }
        }
      }

      //TODO: WaitForAllRequestsToComplete(server);
      //TODO: Assert.Equal(1, server.Server.NumberOfRequests);


    }
  }

  //TODO: other tests
}

package raven.tests.multiget;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.UUID;

import org.junit.Ignore;
import org.junit.Test;

import raven.abstractions.data.GetResponse;
import raven.abstractions.extensions.JsonExtensions;
import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.connection.profiling.ProfilingInformation;
import raven.client.document.DocumentQueryCustomizationFactory;
import raven.client.document.DocumentSession;
import raven.client.document.DocumentStore;
import raven.client.indexes.RavenDocumentsByEntityName;
import raven.tests.bugs.QUser;
import raven.tests.bugs.User;

public class MultiGetProfilingTest extends RemoteClientTest {

  @Test
  public void canProfileLazyRequests() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      ((DocumentStore) store).initializeProfiling();
      try (IDocumentSession session = store.openSession()) {
        // handle the initial request for replication information
      }
      UUID id = null;
      try (IDocumentSession session = store.openSession()) {
        id = ((DocumentSession)session).getDatabaseCommands().getProfilingInformation().getId();
        session.advanced().lazily().load(User.class, "users/1");
        session.advanced().lazily().load(User.class, "users/2");
        session.advanced().lazily().load(User.class, "users/3");

        session.advanced().eagerly().executeAllPendingLazyOperations();
      }

      ProfilingInformation profilingInformation = ((DocumentStore)store).getProfilingInformationFor(id);
      assertEquals(1, profilingInformation.getRequests().size());

      GetResponse[] responses = JsonExtensions.getDefaultObjectMapper().readValue(profilingInformation.getRequests().get(0).getResult(), GetResponse[].class);
      assertEquals(3, responses.length);

      for (GetResponse response : responses) {
        assertEquals(404, response.getStatus());
      }
    }
  }

  @Test
  public void canProfilePartiallyCachedLazyRequest() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      ((DocumentStore) store).initializeProfiling();
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("oren");
        session.store(user1);

        User user2 = new User();
        user2.setName("ayende");
        session.store(user2);

        session.saveChanges();
      }

      QUser u = QUser.user;

      try (IDocumentSession session = store.openSession()) {
        session.query(User.class).where(u.name.eq("oren"))
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .toList();
      }

      UUID id = null;
      try (IDocumentSession session = store.openSession()) {
        id = ((DocumentSession)session).getDatabaseCommands().getProfilingInformation().getId();
        session.query(User.class).where(u.name.eq("oren")).lazily();
        session.query(User.class).where(u.name.eq("ayende")).lazily();

        session.advanced().eagerly().executeAllPendingLazyOperations();
      }

      ProfilingInformation profilingInformation = ((DocumentStore) store).getProfilingInformationFor(id);
      assertEquals(1, profilingInformation.getRequests().size());

      GetResponse[] responses = JsonExtensions.getDefaultObjectMapper().readValue(profilingInformation.getRequests().get(0).getResult(), GetResponse[].class);
      assertEquals(304, responses[0].getStatus());
      assertTrue(responses[0].getResult().toString().contains("oren"));

      assertEquals(200, responses[1].getStatus());
      assertTrue("ayende", responses[1].getResult().toString().contains("ayende"));

    }
  }

  @Test
  public void canProfileFullyCached () throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      ((DocumentStore) store).initializeProfiling();
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("oren");
        session.store(user1);

        User user2 = new User();
        user2.setName("ayende");
        session.store(user2);

        session.saveChanges();
      }

      QUser u = QUser.user;

      try (IDocumentSession session = store.openSession()) {
        session.query(User.class).where(u.name.eq("oren")).toList();
        session.query(User.class).where(u.name.eq("ayende")).toList();
      }

      UUID id = null;
      try (IDocumentSession session = store.openSession()) {
        id = ((DocumentSession)session).getDatabaseCommands().getProfilingInformation().getId();
        session.query(User.class).where(u.name.eq("oren")).lazily();
        session.query(User.class).where(u.name.eq("ayende")).lazily();

        session.advanced().eagerly().executeAllPendingLazyOperations();
      }

      ProfilingInformation profilingInformation = ((DocumentStore) store).getProfilingInformationFor(id);
      assertEquals(1, profilingInformation.getRequests().size());

      GetResponse[] responses = JsonExtensions.getDefaultObjectMapper().readValue(profilingInformation.getRequests().get(0).getResult(), GetResponse[].class);
      assertEquals(304, responses[0].getStatus());
      assertTrue(responses[0].getResult().toString().contains("oren"));

      assertEquals(304, responses[1].getStatus());
      assertTrue("ayende", responses[1].getResult().toString().contains("ayende"));

    }
  }

  @Test
  public void canProfilePartiallyAggressivelyCached() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      ((DocumentStore) store).initializeProfiling();
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("oren");
        session.store(user1);

        User user2 = new User();
        user2.setName("ayende");
        session.store(user2);

        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        try (AutoCloseable scope = session.advanced().getDocumentStore().aggressivelyCacheFor(5 * 60 * 1000)) {
          session.load(User.class, "users/1");
        }
      }

      UUID id = null;
      try (IDocumentSession session = store.openSession()) {
        id = ((DocumentSession)session).getDatabaseCommands().getProfilingInformation().getId();

        try (AutoCloseable scope = session.advanced().getDocumentStore().aggressivelyCacheFor(5 * 60 * 1000)) {
          session.advanced().lazily().load(User.class, "users/1");
          session.advanced().lazily().load(User.class, "users/2");

          session.advanced().eagerly().executeAllPendingLazyOperations();
        }
      }

      ProfilingInformation profilingInformation = ((DocumentStore) store).getProfilingInformationFor(id);
      assertEquals(1, profilingInformation.getRequests().size());

      GetResponse[] responses = JsonExtensions.getDefaultObjectMapper().readValue(profilingInformation.getRequests().get(0).getResult(), GetResponse[].class);
      assertEquals(0, responses[0].getStatus());
      assertTrue(responses[0].getResult().toString().contains("oren"));

      assertEquals(200, responses[1].getStatus());
      assertTrue(responses[1].getResult().toString().contains("ayende"));

    }
  }

  @Test
  public void canProfileFullyAggressivelyCached() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      ((DocumentStore) store).initializeProfiling();
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("oren");
        session.store(user1);

        User user2 = new User();
        user2.setName("ayende");
        session.store(user2);

        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        try (AutoCloseable scope = session.advanced().getDocumentStore().aggressivelyCacheFor(5 * 60 * 1000)) {
          session.load(User.class, "users/1");
          session.load(User.class, "users/2");
        }
      }

      UUID id = null;
      try (IDocumentSession session = store.openSession()) {
        id = ((DocumentSession)session).getDatabaseCommands().getProfilingInformation().getId();

        try (AutoCloseable scope = session.advanced().getDocumentStore().aggressivelyCacheFor(5 * 60 * 1000)) {
          session.advanced().lazily().load(User.class, "users/1");
          session.advanced().lazily().load(User.class, "users/2");

          session.advanced().eagerly().executeAllPendingLazyOperations();
        }
      }

      ProfilingInformation profilingInformation = ((DocumentStore) store).getProfilingInformationFor(id);
      assertEquals(1, profilingInformation.getRequests().size());

      GetResponse[] responses = JsonExtensions.getDefaultObjectMapper().readValue(profilingInformation.getRequests().get(0).getResult(), GetResponse[].class);
      assertEquals(0, responses[0].getStatus());
      assertTrue(responses[0].getResult().toString().contains("oren"));

      assertEquals(0, responses[1].getStatus());
      assertTrue(responses[1].getResult().toString().contains("ayende"));

    }
  }

  @Test
  public void canProfileErrors() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      ((DocumentStore) store).initializeProfiling();
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("oren");
        session.store(user1);

        User user2 = new User();
        user2.setName("ayende");
        session.store(user2);

        session.saveChanges();
      }

      UUID id = null;
      try (IDocumentSession session = store.openSession()) {
        id = ((DocumentSession)session).getDatabaseCommands().getProfilingInformation().getId();

        session.advanced().luceneQuery(Object.class, RavenDocumentsByEntityName.class).whereEquals("Not", "There").lazily();
        try {
          session.advanced().eagerly().executeAllPendingLazyOperations();
          fail();
        } catch (RuntimeException e) {
          //ok
        }
      }

      ProfilingInformation profilingInformation = ((DocumentStore) store).getProfilingInformationFor(id);
      assertEquals(1, profilingInformation.getRequests().size());

      GetResponse[] responses = JsonExtensions.getDefaultObjectMapper().readValue(profilingInformation.getRequests().get(0).getResult(), GetResponse[].class);
      assertEquals(500, responses[0].getStatus());
      assertTrue(responses[0].getResult().toString().contains("The field 'Not' is not indexed, cannot query on fields that are not indexed"));


    }
  }

}

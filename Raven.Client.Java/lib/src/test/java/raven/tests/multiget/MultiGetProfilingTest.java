package raven.tests.multiget;

import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.junit.Test;

import raven.abstractions.data.GetResponse;
import raven.abstractions.extensions.JsonExtensions;
import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.connection.profiling.ProfilingInformation;
import raven.client.document.DocumentSession;
import raven.client.document.DocumentStore;
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

  //TODO: other tests
}

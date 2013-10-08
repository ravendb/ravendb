package raven.tests.bundles.replication;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;

import raven.abstractions.replication.ReplicationDestination;
import raven.abstractions.replication.ReplicationDestination.TransitiveReplicationOptions;
import raven.abstractions.replication.ReplicationDocument;
import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RavenDBAwareTests;
import raven.client.document.DocumentStore;

public class ReplicationBase extends RavenDBAwareTests {

  private List<IDocumentStore> stores = new ArrayList<>();

  protected int retriesCount = 500;

  public IDocumentStore createStore() {
    int port = DEFAULT_SERVER_PORT_1 + stores.size();
    try {
      startServer(port);
      createDbAtPort(getDbName(), port);
      IDocumentStore store = new DocumentStore("http://" + DEFAULT_HOST + ":" + port, getDbName()).initialize();
      stores.add(store);
      return store;
    } catch (Exception e) {
      Assert.fail("Couldn't create the store" + e);
    }
    return null;
  }

  public void tellFirstInstanceToReplicateToSecondInstance() {
    tellInstanceToReplicateToAnotherInstance(0, 1);
  }

  public void tellInstanceToReplicateToAnotherInstance(int src, int dest) {
    ReplicationDocument repDoc = createReplicationDocument("http://" + DEFAULT_HOST + ":"
      + (DEFAULT_SERVER_PORT_1 + dest), getDbName());
    try (IDocumentSession s = stores.get(src).openSession()) {
      s.store(repDoc, "Raven/Replication/Destinations");
      s.saveChanges();
    } catch (Exception e) {
      Assert.fail("Can not add replication document");
    }
  }


  @After
  public void afterTest() {
    //TODO: dispose http client!
    try {
      for (int i = 0; i < stores.size(); i++) {
        stopServer(DEFAULT_SERVER_PORT_1 + i);
      }
    } catch (Exception e) {
      Assert.fail("Can not stop servers");
    }
    stores = new ArrayList<>();
  }

  protected ReplicationDocument createReplicationDocument(String url, String database) {
    ReplicationDestination rep = new ReplicationDestination();
    rep.setUrl(url);
    rep.setDatabase(database);
    rep.setTransitiveReplicationBehavior(TransitiveReplicationOptions.NONE);
    rep.setIgnoredClient(Boolean.FALSE);
    rep.setDisabled(Boolean.FALSE);
    ReplicationDocument repDoc = new ReplicationDocument();
    repDoc.getDestinations().add(rep);
    return repDoc;
  }

}

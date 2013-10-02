package raven.client.connection;

import java.util.UUID;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import raven.abstractions.closure.Functions;
import raven.abstractions.replication.ReplicationDestination;
import raven.abstractions.replication.ReplicationDocument;
import raven.abstractions.replication.ReplicationDestination.TransitiveReplicationOptions;
import raven.client.RavenDBAwareTests;
import raven.client.listeners.IDocumentConflictListener;


public abstract class AbstractReplicationTest extends RavenDBAwareTests{

  protected ServerClient serverClient2;
  protected static final String SOURCE = "source";
  protected static final String TARGET = "target";


  @Before
  @Override
  public void init() {
    super.init();

    serverClient2 = new ServerClient(DEFAULT_SERVER_URL_2, convention,
      new Functions.StaticFunction1<String, ReplicationInformer>(replicationInformer), null, factory,
      UUID.randomUUID(), new IDocumentConflictListener[0]);

  }


  @BeforeClass
  public static void startServerBefore() throws Exception {
    startServer(DEFAULT_SERVER_PORT_1);
    startServer(DEFAULT_SERVER_PORT_2);
  }

  @AfterClass
  public static void stopServerAfter() throws Exception {
    stopServer(DEFAULT_SERVER_PORT_1);
    stopServer(DEFAULT_SERVER_PORT_2);
  }

  protected ReplicationDocument createReplicationDocument() {
    return createReplicationDocument(DEFAULT_SERVER_URL_2, TARGET);
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

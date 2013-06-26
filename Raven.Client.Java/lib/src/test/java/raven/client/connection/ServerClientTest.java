package raven.client.connection;

import static org.junit.Assert.assertNull;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import raven.abstractions.closure.Function1;
import raven.abstractions.closure.Functions;
import raven.abstractions.data.JsonDocument;
import raven.client.RavenDBAwareTests;
import raven.client.document.DocumentConvention;
import raven.client.listeners.IDocumentConflictListener;

public class ServerClientTest extends RavenDBAwareTests {

  private DocumentConvention convention;
  private HttpJsonRequestFactory factory;
  private ReplicationInformer replicationInformer;
  private ServerClient serverClient;


  @Before
  public void init() {
    convention = new DocumentConvention();
    factory = new HttpJsonRequestFactory(10);
    replicationInformer = new ReplicationInformer();

    serverClient = new ServerClient(DEFAULT_SERVER_URL, convention, null,
        new Functions.StaticFunction1<String, ReplicationInformer>(replicationInformer), null, factory, UUID.randomUUID(), new IDocumentConflictListener[0]);
  }

  @Test
  public void testGet() {
    IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
    JsonDocument jsonDocument = db1Commands.get("noSuchElement");
    //TODO: asserts etc
  }

}

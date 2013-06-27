package raven.client.connection;

import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import raven.abstractions.closure.Functions;
import raven.abstractions.data.Etag;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.data.PutResult;
import raven.abstractions.data.UuidType;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJToken;
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
      new Functions.StaticFunction1<String, ReplicationInformer>(replicationInformer), null, factory,
      UUID.randomUUID(), new IDocumentConflictListener[0]);
  }

  //@Test
  public void testPutGet() throws Exception {
    IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
    try {
      createDb("db1");

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());
      RavenJToken t = RavenJToken.fromObject("String");
      RavenJObject o = RavenJObject.parse("{ \"key\" : \"val\"}");
      PutResult result = db1Commands.put("testVal", etag, o, new RavenJObject());
      Assert.assertNotNull(result);
      JsonDocument jsonDocument = db1Commands.get("testVal");
      Assert.assertEquals("val", jsonDocument.getDataAsJson().value(String.class, "key"));

    } finally {
      deleteDb("db1");
    }
  }

  @Test
  public void testgetDatabaseNames() throws Exception {
    IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
    try {
      createDb("db1");

      List<String> result = db1Commands.getDatabaseNames(2);

      System.out.print(result.toArray());


    } finally {
      deleteDb("db1");
    }
  }

}

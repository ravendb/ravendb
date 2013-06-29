package raven.client.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.omg.CORBA.OMGVMCID;

import raven.abstractions.closure.Functions;
import raven.abstractions.data.Attachment;
import raven.abstractions.data.Etag;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.data.JsonDocumentMetadata;
import raven.abstractions.data.PutResult;
import raven.abstractions.data.UuidType;
import raven.abstractions.indexing.IndexDefinition;
import raven.abstractions.json.linq.RavenJObject;
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

  @Test
  public void testPutGet() throws Exception {
    IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
    try {
      createDb("db1");

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());
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
  public void testGetDatabaseNames() throws Exception {
    IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
    try {
      createDb("db1");
      createDb("db2");

      List<String> result = serverClient.getDatabaseNames(2);

      Assert.assertEquals(2, result.size());
      Assert.assertTrue(result.contains("db1"));

    } finally {
      deleteDb("db1");
      createDb("db2");
    }
  }

  @Test
  public void testGetDocuments() throws Exception {
    IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
    try {
      createDb("db1");

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());

      PutResult result = db1Commands.put("testVal1", etag, RavenJObject.parse("{ \"key\" : \"val1\"}"),
        new RavenJObject());
      result = db1Commands.put("testVal2", etag, RavenJObject.parse("{ \"key\" : \"val2\"}"), new RavenJObject());
      result = db1Commands.put("testVal3", etag, RavenJObject.parse("{ \"key\" : \"val3\"}"), new RavenJObject());
      result = db1Commands.put("testVal4", etag, RavenJObject.parse("{ \"key\" : \"val4\"}"), new RavenJObject());

      Assert.assertNotNull(result);

      List<JsonDocument> jsonDocumentList = db1Commands.getDocuments(0, 4);
      Assert.assertEquals(4, jsonDocumentList.size());
      Assert.assertTrue(jsonDocumentList.get(0).getDataAsJson().containsKey("key"));
      Assert.assertTrue(jsonDocumentList.get(1).getDataAsJson().containsKey("key"));
      Assert.assertTrue(jsonDocumentList.get(2).getDataAsJson().containsKey("key"));
      Assert.assertTrue(jsonDocumentList.get(3).getDataAsJson().containsKey("key"));

      jsonDocumentList = db1Commands.getDocuments(0, 2);
      Assert.assertEquals(2, jsonDocumentList.size());

      jsonDocumentList = db1Commands.getDocuments(0, 10);
      Assert.assertEquals(4, jsonDocumentList.size());

      jsonDocumentList = db1Commands.getDocuments(2, 10);
      Assert.assertEquals(2, jsonDocumentList.size());

    } finally {
      deleteDb("db1");
    }
  }

  @Test
  public void testStartsWith() throws Exception {
    IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
    try {
      createDb("db1");

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());

      PutResult result = db1Commands.put("tests/val1a", etag, RavenJObject.parse("{ \"key\" : \"val1\"}"),
        new RavenJObject());
      result = db1Commands.put("tests/val2a", etag, RavenJObject.parse("{ \"key\" : \"val2\"}"), new RavenJObject());
      result = db1Commands.put("tests/val3a", etag, RavenJObject.parse("{ \"key\" : \"val3\"}"), new RavenJObject());
      result = db1Commands.put("tests/aval4", etag, RavenJObject.parse("{ \"key\" : \"val4\"}"), new RavenJObject());

      Assert.assertNotNull(result);

      List<JsonDocument> jsonDocumentList = db1Commands.startsWith("tests/", "", 0, 5);
      Assert.assertEquals(4, jsonDocumentList.size());
      Assert.assertTrue(jsonDocumentList.get(0).getDataAsJson().containsKey("key"));
      Assert.assertTrue(jsonDocumentList.get(1).getDataAsJson().containsKey("key"));
      Assert.assertTrue(jsonDocumentList.get(2).getDataAsJson().containsKey("key"));
      Assert.assertTrue(jsonDocumentList.get(3).getDataAsJson().containsKey("key"));

      jsonDocumentList = db1Commands.startsWith("tests/", "val1a", 0, 5);
      Assert.assertEquals(1, jsonDocumentList.size());

      jsonDocumentList = db1Commands.startsWith("tests/", "val*", 0, 5);
      Assert.assertEquals(3, jsonDocumentList.size());

      jsonDocumentList = db1Commands.startsWith("tests/", "val*a", 0, 5);
      Assert.assertEquals(3, jsonDocumentList.size());

      jsonDocumentList = db1Commands.startsWith("tests/", "*val*", 0, 5);
      Assert.assertEquals(4, jsonDocumentList.size());

      jsonDocumentList = db1Commands.startsWith("tests/v", "*2a", 0, 5);
      Assert.assertEquals(1, jsonDocumentList.size());
      Assert.assertEquals("val2", jsonDocumentList.get(0).getDataAsJson().value(String.class, "key"));

    } finally {
      deleteDb("db1");
    }
  }

  @Test
  public void testUrlFor() throws Exception {
    IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
    try {
      createDb("db1");

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());

      PutResult result = db1Commands.put("tests/val1a", etag, RavenJObject.parse("{ \"key\" : \"val1\"}"),
        new RavenJObject());

      Assert.assertNotNull(result);

      String url = db1Commands.urlFor("tests/val1a");

      Assert.assertTrue(url.endsWith("db1/docs/tests/val1a"));

    } finally {
      deleteDb("db1");
    }
  }

  @Test
  public void testDelete() throws Exception {
    IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
    try {
      createDb("db1");

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());

      PutResult result = db1Commands.put("tests/val1a", etag, RavenJObject.parse("{ \"key\" : \"val1\"}"),
        new RavenJObject());
      result = db1Commands.put("tests/val2a", etag, RavenJObject.parse("{ \"key\" : \"val2\"}"), new RavenJObject());
      result = db1Commands.put("tests/val3a", etag, RavenJObject.parse("{ \"key\" : \"val3\"}"), new RavenJObject());
      result = db1Commands.put("tests/aval4", etag, RavenJObject.parse("{ \"key\" : \"val4\"}"), new RavenJObject());
      Assert.assertNotNull(result);

      List<JsonDocument> jsonDocumentList = db1Commands.getDocuments(0, 5);
      Assert.assertEquals(4, jsonDocumentList.size());

      JsonDocument jsonDocument = db1Commands.get("tests/val1a");

      db1Commands.delete(jsonDocument.getKey(), jsonDocument.getEtag());

      jsonDocumentList = db1Commands.getDocuments(0, 5);
      Assert.assertEquals(3, jsonDocumentList.size());

      jsonDocument = db1Commands.get("tests/val2a");
      db1Commands.delete(jsonDocument.getKey(), jsonDocument.getEtag());
      jsonDocumentList = db1Commands.getDocuments(0, 5);
      Assert.assertEquals(2, jsonDocumentList.size());

      jsonDocument = db1Commands.get("tests/val3a");
      db1Commands.delete(jsonDocument.getKey(), jsonDocument.getEtag());
      jsonDocumentList = db1Commands.getDocuments(0, 5);
      Assert.assertEquals(1, jsonDocumentList.size());

      jsonDocument = db1Commands.get("tests/aval4");
      db1Commands.delete(jsonDocument.getKey(), jsonDocument.getEtag());
      jsonDocumentList = db1Commands.getDocuments(0, 5);
      Assert.assertEquals(0, jsonDocumentList.size());

    } finally {
      deleteDb("db1");
    }
  }

  @Test
  public void testAttachments() throws Exception {
    IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
    try {
      createDb("db1");

      String key = "test/at1";

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());

      InputStream is  = new ByteArrayInputStream("Test test test".getBytes());
      db1Commands.putAttachment(key, etag, is, new RavenJObject());
      is.close();

      Attachment a = db1Commands.getAttachment(key);

      Assert.assertEquals("Test test test", new String(a.getData()));

      List<Attachment> list = db1Commands.getAttachmentHeadersStartingWith("test/", 0, 5);

      db1Commands.deleteAttachment(key, a.getEtag());
      String url = db1Commands.urlFor(key);
      System.out.println(url);

      a = db1Commands.getAttachment(key);
      Assert.assertNull(a);

    } finally {
      deleteDb("db1");
    }
  }

  //@Test
  public void testHead() throws Exception {
    IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
    try {
      createDb("db1");

      String key = "test/at1";

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());

      RavenJObject o = RavenJObject.parse("{ \"key\" : \"val\"}");
      PutResult result = db1Commands.put("testVal", etag, o, new RavenJObject());

      //head method does not work
      JsonDocumentMetadata meta = db1Commands.head("testVal");

      //System.out.print(meta.getLastModified());

    } finally {
      deleteDb("db1");
    }
  }

  @Test
  public void testIndexes() throws Exception {

    IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
    try {
      createDb("db1");

      IndexDefinition index1 = new IndexDefinition();
      index1.setMap("from company in docs.Companies from partner in company.Partners select new { Partner = partner }");

      db1Commands.putIndex("firstIndex", index1);

      assertNotNull(db1Commands.getIndex("firstIndex"));

      db1Commands.resetIndex("firstIndex");

      Collection<String> indexNames = db1Commands.getIndexNames(0, 10);
      List<String> expectedIndexNames = Arrays.asList("firstIndex");
      assertEquals(expectedIndexNames, indexNames);

      Collection<IndexDefinition> collection = db1Commands.getIndexes(0, 10);
      assertEquals(1, collection.size());

      db1Commands.deleteIndex("firstIndex");


      IndexDefinition complexIndex = new IndexDefinition();
      //TODO: set all fields create index, get index and compare properties

      assertEquals(new ArrayList<String>(), db1Commands.getIndexNames(0, 10));


    } finally {
      deleteDb("db1");
    }

  }

  //@Test
  public void test() throws Exception {
    IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
    try {
      createDb("db1");

      String key = "test/at1";

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());

      RavenJObject o = RavenJObject.parse("{ \"key\" : \"val\"}");
      PutResult result = db1Commands.put("testVal", etag, o, new RavenJObject());

      //head method does not work
      Long l = db1Commands.nextIdentityFor("test");

      System.out.print(l);

    } finally {
      deleteDb("db1");
    }
  }

}

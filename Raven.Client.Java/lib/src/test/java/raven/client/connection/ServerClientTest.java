package raven.client.connection;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import raven.abstractions.closure.Functions;
import raven.abstractions.data.Attachment;
import raven.abstractions.data.Etag;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.data.JsonDocumentMetadata;
import raven.abstractions.data.PutResult;
import raven.abstractions.data.UuidType;
import raven.abstractions.extensions.JsonExtensions;
import raven.abstractions.indexing.IndexDefinition;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.RavenDBAwareTests;
import raven.client.document.DocumentConvention;
import raven.client.listeners.IDocumentConflictListener;
import raven.samples.Developer;

public class ServerClientTest extends RavenDBAwareTests {

  private DocumentConvention convention;
  private HttpJsonRequestFactory factory;
  private ReplicationInformer replicationInformer;
  private ServerClient serverClient;

  @Before
  public void init() {
    System.setProperty("java.net.preferIPv4Stack" , "true");
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
      assertNotNull(result);
      JsonDocument jsonDocument = db1Commands.get("testVal");
      assertEquals("val", jsonDocument.getDataAsJson().value(String.class, "key"));


      Developer d1 = new Developer();
      d1.setNick("john");
      d1.setId(5l);

      String longKey = StringUtils.repeat("a", 256);
      db1Commands.put(longKey, null, RavenJObject.fromObject(d1), new RavenJObject());

      JsonDocument developerDocument = db1Commands.get(longKey);
      Developer readDeveloper = JsonExtensions.getDefaultObjectMapper().readValue(developerDocument.getDataAsJson().toString(), Developer.class);
      assertEquals("john", readDeveloper.getNick());

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

      assertEquals(2, result.size());
      assertTrue(result.contains("db1"));

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

      assertNotNull(result);

      List<JsonDocument> jsonDocumentList = db1Commands.getDocuments(0, 4);
      assertEquals(4, jsonDocumentList.size());
      assertTrue(jsonDocumentList.get(0).getDataAsJson().containsKey("key"));
      assertTrue(jsonDocumentList.get(1).getDataAsJson().containsKey("key"));
      assertTrue(jsonDocumentList.get(2).getDataAsJson().containsKey("key"));
      assertTrue(jsonDocumentList.get(3).getDataAsJson().containsKey("key"));

      jsonDocumentList = db1Commands.getDocuments(0, 2);
      assertEquals(2, jsonDocumentList.size());

      jsonDocumentList = db1Commands.getDocuments(0, 10);
      assertEquals(4, jsonDocumentList.size());

      jsonDocumentList = db1Commands.getDocuments(2, 10);
      assertEquals(2, jsonDocumentList.size());

      List<JsonDocument> metaOnly = db1Commands.getDocuments(0, 100, true);
      assertEquals(4, metaOnly.size());
      assertEquals(0, metaOnly.get(0).getDataAsJson().getCount());


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

      assertNotNull(result);

      List<JsonDocument> jsonDocumentList = db1Commands.startsWith("tests/", "", 0, 5);
      assertEquals(4, jsonDocumentList.size());
      assertTrue(jsonDocumentList.get(0).getDataAsJson().containsKey("key"));
      assertTrue(jsonDocumentList.get(1).getDataAsJson().containsKey("key"));
      assertTrue(jsonDocumentList.get(2).getDataAsJson().containsKey("key"));
      assertTrue(jsonDocumentList.get(3).getDataAsJson().containsKey("key"));

      jsonDocumentList = db1Commands.startsWith("tests/", "val1a", 0, 5);
      assertEquals(1, jsonDocumentList.size());

      jsonDocumentList = db1Commands.startsWith("tests/", "val*", 0, 5);
      assertEquals(3, jsonDocumentList.size());

      jsonDocumentList = db1Commands.startsWith("tests/", "val*a", 0, 5);
      assertEquals(3, jsonDocumentList.size());

      jsonDocumentList = db1Commands.startsWith("tests/", "*val*", 0, 5);
      assertEquals(4, jsonDocumentList.size());

      jsonDocumentList = db1Commands.startsWith("tests/v", "*2a", 0, 5);
      assertEquals(1, jsonDocumentList.size());
      assertEquals("val2", jsonDocumentList.get(0).getDataAsJson().value(String.class, "key"));

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

      assertNotNull(result);

      String url = db1Commands.urlFor("tests/val1a");

      assertTrue(url.endsWith("db1/docs/tests/val1a"));

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
      assertNotNull(result);

      List<JsonDocument> jsonDocumentList = db1Commands.getDocuments(0, 5);
      assertEquals(4, jsonDocumentList.size());

      JsonDocument jsonDocument = db1Commands.get("tests/val1a");

      db1Commands.delete(jsonDocument.getKey(), jsonDocument.getEtag());

      jsonDocumentList = db1Commands.getDocuments(0, 5);
      assertEquals(3, jsonDocumentList.size());

      jsonDocument = db1Commands.get("tests/val2a");
      db1Commands.delete(jsonDocument.getKey(), jsonDocument.getEtag());
      jsonDocumentList = db1Commands.getDocuments(0, 5);
      assertEquals(2, jsonDocumentList.size());

      jsonDocument = db1Commands.get("tests/val3a");
      db1Commands.delete(jsonDocument.getKey(), jsonDocument.getEtag());
      jsonDocumentList = db1Commands.getDocuments(0, 5);
      assertEquals(1, jsonDocumentList.size());

      jsonDocument = db1Commands.get("tests/aval4");
      db1Commands.delete(jsonDocument.getKey(), jsonDocument.getEtag());
      jsonDocumentList = db1Commands.getDocuments(0, 5);
      assertEquals(0, jsonDocumentList.size());

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

      assertEquals("Test test test", new String(a.getData()));

      List<Attachment> list = db1Commands.getAttachmentHeadersStartingWith("test/", 0, 5);

      Attachment ah = db1Commands.headAttachment(key);

      db1Commands.deleteAttachment(key, a.getEtag());
      String url = db1Commands.urlFor(key);

      a = db1Commands.getAttachment(key);
      assertNull(a);

    } finally {
      deleteDb("db1");
    }
  }

  @Test
  public void testHead() throws Exception {
    IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
    try {
      createDb("db1");

      String key = "testVal";

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());

      RavenJObject o = RavenJObject.parse("{ \"key\" : \"val\"}");
      PutResult result = db1Commands.put(key, etag, o, new RavenJObject());

      //head method does not work
      JsonDocumentMetadata meta = db1Commands.head(key);

      assertNotNull(meta);
      assertNotNull(meta.getLastModified());
      assertEquals(key, meta.getKey());

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

  @Test
  public void testNextIdentityFor() throws Exception {
    IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
    try {
      createDb("db1");

      String key = "test";

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());

      RavenJObject o = RavenJObject.parse("{ \"key\" : \"val\"}");
      PutResult result = db1Commands.put(key, etag, o, new RavenJObject());

      //head method does not work
      Long l = db1Commands.nextIdentityFor(key);

      assertEquals(new Long(1), l);

      JsonDocument doc = db1Commands.get(key);

      doc.getDataAsJson().add("key2", RavenJToken.fromObject("val2"));

      result = db1Commands.put(key, doc.getEtag(), doc.getDataAsJson(), new RavenJObject());

      l = db1Commands.nextIdentityFor(key);

      assertEquals(new Long(2), l);

    } finally {
      deleteDb("db1");
    }
  }

}

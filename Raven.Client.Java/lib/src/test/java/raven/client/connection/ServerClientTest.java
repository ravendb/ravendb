package raven.client.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;

import raven.abstractions.closure.Functions;
import raven.abstractions.data.Attachment;
import raven.abstractions.data.Etag;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.data.JsonDocumentMetadata;
import raven.abstractions.data.PutResult;
import raven.abstractions.data.UuidType;
import raven.abstractions.exceptions.ServerClientException;
import raven.abstractions.extensions.JsonExtensions;
import raven.abstractions.indexing.FieldIndexing;
import raven.abstractions.indexing.FieldStorage;
import raven.abstractions.indexing.FieldTermVector;
import raven.abstractions.indexing.IndexDefinition;
import raven.abstractions.indexing.SortOptions;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJToken;
import raven.abstractions.json.linq.RavenJValue;
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
      RavenJObject o = RavenJObject.parse("{ \"key\" : \"val\"}");
      PutResult result = db1Commands.put("testVal", etag, o, new RavenJObject());
      assertNotNull(result);
      try {
        db1Commands.delete("testVal", result.getEtag().incrementBy(10000));
        fail();
      } catch (Exception e) {
        //ok
      }

      JsonDocument jsonDocument = db1Commands.get("testVal");
      assertEquals("val", jsonDocument.getDataAsJson().value(String.class, "key"));
      assertNull("Can't get document with long key", db1Commands.get(StringUtils.repeat("a", 256)));
      assertNull("This document does not exist!", db1Commands.get("NoSuch"));

      db1Commands.delete("noSuchKey", null);

      Developer d1 = new Developer();
      d1.setNick("john");
      d1.setId(5l);

      String longKey = StringUtils.repeat("a", 256);
      db1Commands.put(longKey, null, RavenJObject.fromObject(d1), new RavenJObject());

      JsonDocument developerDocument = db1Commands.get(longKey);
      Developer readDeveloper = JsonExtensions.getDefaultObjectMapper().readValue(developerDocument.getDataAsJson().toString(), Developer.class);
      assertEquals("john", readDeveloper.getNick());

      RavenJObject objectWithOutKey = new RavenJObject();
      objectWithOutKey.add("Name",  new RavenJValue("Anonymous"));
      PutResult putResult = db1Commands.put(null, null, objectWithOutKey , null);
      assertNotNull(putResult);
      String docKey = putResult.getKey();
      assertNotNull(db1Commands.get(docKey));

    } finally {
      deleteDb("db1");
    }
  }

  @Test
  public void testGetDatabaseNames() throws Exception {
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

      jsonDocumentList = db1Commands.startsWith("tests/", "val1a", 0, 5, true);
      assertEquals(1, jsonDocumentList.size());
      assertEquals("We requested metadata only", 0, jsonDocumentList.get(0).getDataAsJson().getCount());

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
  public void testTransformers() {

  }

  @Test
  public void testAttachments() throws Exception {
    IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
    try {
      createDb("db1");

      assertNull("No such attachment", db1Commands.getAttachment("noSuchLKey"));

      String key = "test/at1";

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());

      InputStream is  = new ByteArrayInputStream("Test test test".getBytes());
      db1Commands.putAttachment(key, etag, is, new RavenJObject());
      is.close();

      Attachment a = db1Commands.getAttachment(key);

      RavenJObject meta = new RavenJObject();
      meta.add("Content-Type", new RavenJValue("text/plain"));
      db1Commands.updateAttachmentMetadata(key, a.getEtag(), meta);

      a = db1Commands.getAttachment(key);
      assertEquals("text/plain", a.getMetadata().get("Content-Type").value(String.class));

      // can update attachment metadata

      RavenJObject metadata = a.getMetadata();
      metadata.add("test", new RavenJValue("yes"));
      db1Commands.updateAttachmentMetadata(key, a.getEtag(), metadata);

      a = db1Commands.getAttachment(key);
      metadata = new RavenJObject();
      metadata.add("test", new RavenJValue("no"));
      db1Commands.updateAttachmentMetadata(key, a.getEtag(), metadata);
      a = db1Commands.getAttachment(key);

      assertEquals("no", a.getMetadata().get("Test").value(String.class));

      metadata = new RavenJObject();
      meta.add("test", new RavenJValue("etag"));
      try {
        db1Commands.updateAttachmentMetadata(key, a.getEtag().incrementBy(10000), metadata);
        fail();
      } catch (ServerClientException e) {
        //ok
      }

      assertEquals("Test test test", new String(a.getData()));

      List<Attachment> list = db1Commands.getAttachmentHeadersStartingWith("test/", 0, 5);
      assertEquals(1, list.size());

      Attachment ah = db1Commands.headAttachment(key);
      assertNotNull(ah.getMetadata());

      db1Commands.deleteAttachment(key, a.getEtag());
      String url = db1Commands.urlFor(key);
      assertEquals("http://localhost:8123/databases/db1/docs/test/at1", url);

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
      db1Commands.put(key, etag, o, new RavenJObject());

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
      complexIndex.setMap("docs.Companies.SelectMany(c => c.Employees).Select(x => new {Name = x.Name,Count = 1})");
      complexIndex.setReduce("results.GroupBy(x => x.Name).Select(x => new {Name = x.Key,Count = Enumerable.Sum(x, y => ((int) y.Count))})");
      complexIndex.getStores().put("Name", FieldStorage.YES);
      complexIndex.getStores().put("Count", FieldStorage.NO);
      complexIndex.getIndexes().put("Name", FieldIndexing.ANALYZED);
      complexIndex.getIndexes().put("Count", FieldIndexing.NOT_ANALYZED);
      complexIndex.getSortOptions().put("Name", SortOptions.STRING_VAL);
      complexIndex.getSortOptions().put("Count", SortOptions.FLOAT);
      complexIndex.getTermVectors().put("Name", FieldTermVector.WITH_POSITIONS_AND_OFFSETS);
      complexIndex.getAnalyzers().put("Name", "Raven.Database.Indexing.Collation.Cultures.SvCollationAnalyzer, Raven.Database");

      db1Commands.putIndex("ComplexIndex", complexIndex);

      IndexDefinition complexReturn = db1Commands.getIndex("ComplexIndex");
      db1Commands.deleteIndex("ComplexIndex");

      assertEquals(FieldStorage.YES, complexReturn.getStores().get("Name"));
      assertNull("It should be null since, No is default value", complexReturn.getStores().get("Count"));
      assertEquals(FieldIndexing.ANALYZED, complexReturn.getIndexes().get("Name"));
      assertEquals(FieldIndexing.NOT_ANALYZED, complexReturn.getIndexes().get("Count"));
      assertEquals(SortOptions.STRING_VAL, complexReturn.getSortOptions().get("Name"));
      assertEquals(SortOptions.FLOAT, complexReturn.getSortOptions().get("Count"));
      assertEquals(FieldTermVector.WITH_POSITIONS_AND_OFFSETS, complexReturn.getTermVectors().get("Name"));
      assertEquals("Raven.Database.Indexing.Collation.Cultures.SvCollationAnalyzer, Raven.Database", complexReturn.getAnalyzers().get("Name"));

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
      db1Commands.put(key, etag, o, new RavenJObject());

      //head method does not work
      Long l = db1Commands.nextIdentityFor(key);

      assertEquals(new Long(1), l);

      JsonDocument doc = db1Commands.get(key);

      doc.getDataAsJson().add("key2", RavenJToken.fromObject("val2"));

      db1Commands.put(key, doc.getEtag(), doc.getDataAsJson(), new RavenJObject());

      l = db1Commands.nextIdentityFor(key);

      assertEquals(new Long(2), l);

    } finally {
      deleteDb("db1");
    }
  }

}

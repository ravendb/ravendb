package net.ravendb.client.connection;

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

import net.ravendb.abstractions.commands.ICommandData;
import net.ravendb.abstractions.commands.PatchCommandData;
import net.ravendb.abstractions.commands.PutCommandData;
import net.ravendb.abstractions.data.Attachment;
import net.ravendb.abstractions.data.BatchResult;
import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.data.DatabaseDocument;
import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.JsonDocument;
import net.ravendb.abstractions.data.JsonDocumentMetadata;
import net.ravendb.abstractions.data.PatchCommandType;
import net.ravendb.abstractions.data.PatchRequest;
import net.ravendb.abstractions.data.PutResult;
import net.ravendb.abstractions.data.UuidType;
import net.ravendb.abstractions.exceptions.ServerClientException;
import net.ravendb.abstractions.extensions.JsonExtensions;
import net.ravendb.abstractions.indexing.FieldIndexing;
import net.ravendb.abstractions.indexing.FieldStorage;
import net.ravendb.abstractions.indexing.FieldTermVector;
import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.abstractions.indexing.SortOptions;
import net.ravendb.abstractions.json.linq.RavenJArray;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.abstractions.json.linq.RavenJValue;
import net.ravendb.client.RavenDBAwareTests;
import net.ravendb.client.connection.IDatabaseCommands;
import net.ravendb.samples.Developer;

import org.apache.commons.lang.StringUtils;
import org.junit.Ignore;
import org.junit.Test;


public class ServerClientTest extends RavenDBAwareTests {

  @Test
  public void testCreateDb() throws Exception {
    try {
      IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
      DatabaseDocument databaseDocument = new DatabaseDocument();
      databaseDocument.setId("testingDb");
      databaseDocument.getSettings().put("Raven/DataDir", "~\\Databases\\testingDb");
      dbCommands.getGlobalAdmin().createDatabase(databaseDocument);
    } finally {
      deleteDb("testingDb");
    }
  }



  @Test
  public void testPutGet() throws Exception {
    IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
    try {
      createDb();

      Etag etag = new Etag();
      RavenJObject o = RavenJObject.parse("{ \"key\" : \"val\"}");
      PutResult result = dbCommands.put("testVal", etag, o, new RavenJObject());
      assertNotNull(result);
      try {
        dbCommands.delete("testVal", result.getEtag().incrementBy(10000));
        fail();
      } catch (Exception e) {
        //ok
      }

      JsonDocument jsonDocument = dbCommands.get("testVal");
      assertEquals("val", jsonDocument.getDataAsJson().value(String.class, "key"));
      assertNull("Can't get document with long key", dbCommands.get(StringUtils.repeat("a", 256)));
      assertNull("This document does not exist!", dbCommands.get("NoSuch"));

      dbCommands.delete("noSuchKey", null);

      Developer d1 = new Developer();
      d1.setNick("john");
      d1.setId(5l);

      String longKey = StringUtils.repeat("a", 256);
      dbCommands.put(longKey, null, RavenJObject.fromObject(d1), new RavenJObject());

      JsonDocument developerDocument = dbCommands.get(longKey);
      Developer readDeveloper = JsonExtensions.createDefaultJsonSerializer().readValue(developerDocument.getDataAsJson().toString(), Developer.class);
      assertEquals("john", readDeveloper.getNick());

      RavenJObject objectWithOutKey = new RavenJObject();
      objectWithOutKey.add("Name",  new RavenJValue("Anonymous"));
      PutResult putResult = dbCommands.put(null, null, objectWithOutKey , null);
      assertNotNull(putResult);
      String docKey = putResult.getKey();
      assertNotNull(dbCommands.get(docKey));

    } finally {
      deleteDb();
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
      deleteDb("db2");
    }
  }

  @Test
  public void testGetDocuments() throws Exception {
    IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
    try {
      createDb();

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());

      PutResult result = dbCommands.put("testVal1", etag, RavenJObject.parse("{ \"key\" : \"val1\"}"),
          new RavenJObject());
      result = dbCommands.put("testVal2", etag, RavenJObject.parse("{ \"key\" : \"val2\"}"), new RavenJObject());
      result = dbCommands.put("testVal3", etag, RavenJObject.parse("{ \"key\" : \"val3\"}"), new RavenJObject());
      result = dbCommands.put("testVal4", etag, RavenJObject.parse("{ \"key\" : \"val4\"}"), new RavenJObject());

      assertNotNull(result);

      List<JsonDocument> jsonDocumentList = dbCommands.getDocuments(0, 4);
      assertEquals(4, jsonDocumentList.size());
      assertTrue(jsonDocumentList.get(0).getDataAsJson().containsKey("key"));
      assertTrue(jsonDocumentList.get(1).getDataAsJson().containsKey("key"));
      assertTrue(jsonDocumentList.get(2).getDataAsJson().containsKey("key"));
      assertTrue(jsonDocumentList.get(3).getDataAsJson().containsKey("key"));

      jsonDocumentList = dbCommands.getDocuments(0, 2);
      assertEquals(2, jsonDocumentList.size());

      jsonDocumentList = dbCommands.getDocuments(0, 10);
      assertEquals(5, jsonDocumentList.size());

      jsonDocumentList = dbCommands.getDocuments(2, 10);
      assertEquals(3, jsonDocumentList.size());

      List<JsonDocument> metaOnly = dbCommands.getDocuments(0, 100, true);
      assertEquals(5, metaOnly.size());
      assertEquals(0, metaOnly.get(0).getDataAsJson().getCount());


    } finally {
      deleteDb();
    }
  }

  @Test
  public void testStartsWith() throws Exception {
    IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
    try {
      createDb();

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());

      PutResult result = dbCommands.put("tests/val1a", etag, RavenJObject.parse("{ \"key\" : \"val1\"}"),
          new RavenJObject());
      result = dbCommands.put("tests/val2a", etag, RavenJObject.parse("{ \"key\" : \"val2\"}"), new RavenJObject());
      result = dbCommands.put("tests/val3a", etag, RavenJObject.parse("{ \"key\" : \"val3\"}"), new RavenJObject());
      result = dbCommands.put("tests/aval4", etag, RavenJObject.parse("{ \"key\" : \"val4\"}"), new RavenJObject());

      assertNotNull(result);

      List<JsonDocument> jsonDocumentList = dbCommands.startsWith("tests/", "", 0, 5);
      assertEquals(4, jsonDocumentList.size());
      assertTrue(jsonDocumentList.get(0).getDataAsJson().containsKey("key"));
      assertTrue(jsonDocumentList.get(1).getDataAsJson().containsKey("key"));
      assertTrue(jsonDocumentList.get(2).getDataAsJson().containsKey("key"));
      assertTrue(jsonDocumentList.get(3).getDataAsJson().containsKey("key"));

      jsonDocumentList = dbCommands.startsWith("tests/", "val1a", 0, 5);
      assertEquals(1, jsonDocumentList.size());

      jsonDocumentList = dbCommands.startsWith("tests/", "val*", 0, 5);
      assertEquals(3, jsonDocumentList.size());

      jsonDocumentList = dbCommands.startsWith("tests/", "val*a", 0, 5);
      assertEquals(3, jsonDocumentList.size());

      jsonDocumentList = dbCommands.startsWith("tests/", "*val*", 0, 5);
      assertEquals(4, jsonDocumentList.size());

      jsonDocumentList = dbCommands.startsWith("tests/v", "*2a", 0, 5);
      assertEquals(1, jsonDocumentList.size());
      assertEquals("val2", jsonDocumentList.get(0).getDataAsJson().value(String.class, "key"));

      jsonDocumentList = dbCommands.startsWith("tests/", "val1a", 0, 5, true);
      assertEquals(1, jsonDocumentList.size());
      assertEquals("We requested metadata only", 0, jsonDocumentList.get(0).getDataAsJson().getCount());

    } finally {
      deleteDb();
    }
  }

  @Test
  public void testUrlFor() throws Exception {
    IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
    try {
      createDb();

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());

      PutResult result = dbCommands.put("tests/val1a", etag, RavenJObject.parse("{ \"key\" : \"val1\"}"),
          new RavenJObject());

      assertNotNull(result);

      String url = dbCommands.urlFor("tests/val1a");

      assertTrue(url.endsWith(getDbName() + "/docs/tests/val1a"));

    } finally {
      deleteDb();
    }
  }

  @Test
  public void testDelete() throws Exception {
    IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
    try {
      createDb();

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());

      PutResult result = dbCommands.put("tests/val1a", etag, RavenJObject.parse("{ \"key\" : \"val1\"}"),
          new RavenJObject());
      result = dbCommands.put("tests/val2a", etag, RavenJObject.parse("{ \"key\" : \"val2\"}"), new RavenJObject());
      result = dbCommands.put("tests/val3a", etag, RavenJObject.parse("{ \"key\" : \"val3\"}"), new RavenJObject());
      result = dbCommands.put("tests/aval4", etag, RavenJObject.parse("{ \"key\" : \"val4\"}"), new RavenJObject());
      assertNotNull(result);

      List<JsonDocument> jsonDocumentList = dbCommands.getDocuments(0, 5);
      assertEquals(5, jsonDocumentList.size());

      JsonDocument jsonDocument = dbCommands.get("tests/val1a");

      dbCommands.delete(jsonDocument.getKey(), jsonDocument.getEtag());

      jsonDocumentList = dbCommands.getDocuments(0, 5);
      assertEquals(4, jsonDocumentList.size());

      jsonDocument = dbCommands.get("tests/val2a");
      dbCommands.delete(jsonDocument.getKey(), jsonDocument.getEtag());
      jsonDocumentList = dbCommands.getDocuments(0, 5);
      assertEquals(3, jsonDocumentList.size());

      jsonDocument = dbCommands.get("tests/val3a");
      dbCommands.delete(jsonDocument.getKey(), jsonDocument.getEtag());
      jsonDocumentList = dbCommands.getDocuments(0, 5);
      assertEquals(2, jsonDocumentList.size());

      jsonDocument = dbCommands.get("tests/aval4");
      dbCommands.delete(jsonDocument.getKey(), jsonDocument.getEtag());
      jsonDocumentList = dbCommands.getDocuments(0, 5);
      assertEquals(1, jsonDocumentList.size());

    } finally {
      deleteDb();
    }
  }


  @Test
  public void testBatch() throws Exception {
    try {
      createDb();
      IDatabaseCommands commands = serverClient.forDatabase(getDbName());

      RavenJObject postMeta = new RavenJObject();
      postMeta.add(Constants.RAVEN_ENTITY_NAME, new RavenJValue("posts"));

      RavenJObject firstComment = new RavenJObject();
      firstComment.add("AuthorId", new RavenJValue("authors/123"));

      RavenJObject post = new RavenJObject();
      post.add("Comments", new RavenJArray(firstComment));

      PutCommandData createPost = new PutCommandData();
      createPost.setKey("posts/1");
      createPost.setMetadata(postMeta);
      createPost.setDocument(post);

      RavenJObject secondComment = new RavenJObject();
      secondComment.add("AuthorId", new RavenJValue("authors/456"));

      PatchCommandData addAnotherComment = new PatchCommandData();
      addAnotherComment.setKey("posts/1");
      PatchRequest patchRequest = new PatchRequest();
      addAnotherComment.setPatches(new PatchRequest[] { patchRequest});
      patchRequest.setType(PatchCommandType.ADD);
      patchRequest.setName("Comments");
      patchRequest.setValue(secondComment);

      BatchResult[] batchResults = commands.batch(Arrays.<ICommandData> asList(createPost, addAnotherComment));
      assertEquals(2, batchResults.length);

      JsonDocument fetchedPost = commands.get("posts/1");
      assertNotNull(fetchedPost);
      assertEquals(2, fetchedPost.getDataAsJson().value(RavenJArray.class, "Comments").size());


    } finally {
      deleteDb();
    }
  }

  @Test
  public void testAttachments() throws Exception {
    IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
    try {
      createDb();

      assertNull("No such attachment", dbCommands.getAttachment("noSuchLKey"));

      String key = "test/at1";

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());

      InputStream is  = new ByteArrayInputStream("Test test test".getBytes());
      dbCommands.putAttachment(key, etag, is, new RavenJObject());
      is.close();

      assertEquals(1l, dbCommands.getStatistics().getCountOfAttachments());

      Attachment a = dbCommands.getAttachment(key);

      RavenJObject meta = new RavenJObject();
      meta.add("Content-Type", new RavenJValue("text/plain"));
      dbCommands.updateAttachmentMetadata(key, a.getEtag(), meta);

      a = dbCommands.getAttachment(key);
      assertEquals("text/plain", a.getMetadata().get("Content-Type").value(String.class));

      // can update attachment metadata

      RavenJObject metadata = a.getMetadata();
      metadata.add("test", new RavenJValue("yes"));
      dbCommands.updateAttachmentMetadata(key, a.getEtag(), metadata);

      a = dbCommands.getAttachment(key);
      metadata = new RavenJObject();
      metadata.add("test", new RavenJValue("no"));
      dbCommands.updateAttachmentMetadata(key, a.getEtag(), metadata);
      a = dbCommands.getAttachment(key);

      assertEquals("no", a.getMetadata().get("Test").value(String.class));

      metadata = new RavenJObject();
      meta.add("test", new RavenJValue("etag"));
      try {
        dbCommands.updateAttachmentMetadata(key, a.getEtag().incrementBy(10000), metadata);
        fail();
      } catch (ServerClientException e) {
        //ok
      }

      assertEquals("Test test test", new String(a.getData()));

      List<Attachment> list = dbCommands.getAttachmentHeadersStartingWith("test/", 0, 5);
      assertEquals(1, list.size());

      Attachment ah = dbCommands.headAttachment(key);
      assertNotNull(ah.getMetadata());

      dbCommands.deleteAttachment(key, a.getEtag());
      String url = dbCommands.urlFor(key);
      assertEquals("http://" + DEFAULT_HOST + ":8123/databases/" + getDbName() + "/docs/test/at1", url);

      a = dbCommands.getAttachment(key);
      assertNull(a);

    } finally {
      deleteDb();
    }
  }

  @Test
  public void testHead() throws Exception {
    IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
    try {
      createDb();

      String key = "testVal";

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());

      RavenJObject o = RavenJObject.parse("{ \"key\" : \"val\"}");
      dbCommands.put(key, etag, o, new RavenJObject());

      //head method does not work
      JsonDocumentMetadata meta = dbCommands.head(key);

      assertNotNull(meta);
      assertNotNull(meta.getLastModified());
      assertEquals(key, meta.getKey());

    } finally {
      deleteDb();
    }
  }

  @Test
  public void testIndexes() throws Exception {

    IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
    try {
      createDb();

      IndexDefinition index1 = new IndexDefinition();
      index1.setMap("from company in docs.Companies from partner in company.Partners select new { Partner = partner }");

      dbCommands.putIndex("firstIndex", index1);

      assertNotNull(dbCommands.getIndex("firstIndex"));

      dbCommands.resetIndex("firstIndex");

      Collection<String> indexNames = dbCommands.getIndexNames(0, 10);
      List<String> expectedIndexNames = Arrays.asList("firstIndex");
      assertEquals(expectedIndexNames, indexNames);

      Collection<IndexDefinition> collection = dbCommands.getIndexes(0, 10);
      assertEquals(1, collection.size());

      dbCommands.deleteIndex("firstIndex");

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

      dbCommands.putIndex("ComplexIndex", complexIndex);

      IndexDefinition complexReturn = dbCommands.getIndex("ComplexIndex");
      dbCommands.deleteIndex("ComplexIndex");

      assertEquals(FieldStorage.YES, complexReturn.getStores().get("Name"));
      assertNull("It should be null since, No is default value", complexReturn.getStores().get("Count"));
      assertEquals(FieldIndexing.ANALYZED, complexReturn.getIndexes().get("Name"));
      assertEquals(FieldIndexing.NOT_ANALYZED, complexReturn.getIndexes().get("Count"));
      assertEquals(SortOptions.STRING_VAL, complexReturn.getSortOptions().get("Name"));
      assertEquals(SortOptions.FLOAT, complexReturn.getSortOptions().get("Count"));
      assertEquals(FieldTermVector.WITH_POSITIONS_AND_OFFSETS, complexReturn.getTermVectors().get("Name"));
      assertEquals("Raven.Database.Indexing.Collation.Cultures.SvCollationAnalyzer, Raven.Database", complexReturn.getAnalyzers().get("Name"));

      assertEquals(new ArrayList<String>(), dbCommands.getIndexNames(0, 10));


    } finally {
      deleteDb();
    }

  }

  @Test
  public void testNextIdentityFor() throws Exception {
    IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
    try {
      createDb();

      String key = "test";

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());

      RavenJObject o = RavenJObject.parse("{ \"key\" : \"val\"}");
      dbCommands.put(key, etag, o, new RavenJObject());

      //head method does not work
      Long l = dbCommands.nextIdentityFor(key);

      assertEquals(new Long(1), l);

      JsonDocument doc = dbCommands.get(key);

      doc.getDataAsJson().add("key2", RavenJToken.fromObject("val2"));

      dbCommands.put(key, doc.getEtag(), doc.getDataAsJson(), new RavenJObject());

      l = dbCommands.nextIdentityFor(key);

      assertEquals(new Long(2), l);

    } finally {
      deleteDb();
    }
  }

}

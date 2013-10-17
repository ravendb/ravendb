package net.ravendb.tests.patching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.ravendb.abstractions.data.IndexQuery;
import net.ravendb.abstractions.data.JsonDocument;
import net.ravendb.abstractions.data.ScriptedPatchRequest;
import net.ravendb.abstractions.exceptions.ConcurrencyException;
import net.ravendb.abstractions.extensions.JsonExtensions;
import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;

import org.junit.Test;


public class AdvancedPatchingTest extends RemoteClientTest {

  private CustomType test = new CustomType("someId", "bob", 12143, Arrays.asList("one", "two", "seven"));

  //splice(2, 1) will remove 1 elements from position 2 onwards (zero-based)
  String sampleScript = "this.Comments.splice(2, 1); " +
      "this.Id = 'Something new';  " +
      "this.Value++; " +
      "this.newValue = \"err!!\"; " +
      "this.Comments.Map(function(comment) { " +
      "  return (comment == \"one\") ? comment + \" test\" : comment; " +
      "});";


  @Test
  public void canPerformAdvancedPatching_Remotely() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      executeTest(store);
    }
  }

  @Test
  public void canPerformAdvancedWithSetBasedUpdates_Remotely() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      executeSetBasedTest(store);
    }
  }

  @Test
  public void canUpdateBasedOnAnotherDocumentProperty() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        CustomType c1 = new CustomType();
        c1.setValue(2);
        session.store(c1);

        CustomType c2 = new CustomType();
        c2.setValue(1);
        session.store(c2);
        session.saveChanges();
      }
      ScriptedPatchRequest patchRequest = new ScriptedPatchRequest();
      patchRequest.setScript("var another = LoadDocument(anotherId); this.Value = another.Value; ");
      Map<String, Object> values = new HashMap<>();
      values.put("anotherId", "CustomTypes/2");
      patchRequest.setValues(values);
      store.getDatabaseCommands().patch("CustomTypes/1", patchRequest);

      JsonDocument resultDoc = store.getDatabaseCommands().get("CustomTypes/1");
      RavenJObject resultJson = resultDoc.getDataAsJson();
      CustomType result = JsonExtensions.createDefaultJsonSerializer().readValue(resultJson.toString(), CustomType.class);

      assertEquals(1, result.getValue());
    }
  }

  @Test
  public void canPatchMetadata() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        CustomType c1 = new CustomType();
        c1.setValue(2);
        session.store(c1);

        CustomType c2 = new CustomType();
        c2.setValue(1);
        session.store(c2);
        session.saveChanges();
      }
      ScriptedPatchRequest patchRequest = new ScriptedPatchRequest();
      patchRequest.setScript("this.Owner = this['@metadata']['Raven-Clr-Type']; this['@metadata']['Raven-Entity-Name'] = 'New-Entity'; ");
      store.getDatabaseCommands().patch("CustomTypes/1", patchRequest);

      JsonDocument resultDoc = store.getDatabaseCommands().get("CustomTypes/1");
      RavenJObject resultJson = resultDoc.getDataAsJson();
      CustomType result = JsonExtensions.createDefaultJsonSerializer().readValue(resultJson.toString(), CustomType.class);
      RavenJObject metadata = resultDoc.getMetadata();

      assertEquals(metadata.get("Raven-Clr-Type").toString(), result.getOwner());
      assertEquals("New-Entity", metadata.get("Raven-Entity-Name").toString());
    }
  }

  @Test
  public void canUpdateOnMissingProperty() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        RavenJObject obj1 = new RavenJObject();
        obj1.add("Name", "Ayende");
        session.store(obj1, "products/1");
        session.saveChanges();
      }

      ScriptedPatchRequest patchRequest = new ScriptedPatchRequest();
      patchRequest.setScript("this.Test = 'a';");
      store.getDatabaseCommands().patch("products/1", patchRequest);

      JsonDocument resultDoc = store.getDatabaseCommands().get("products/1");

      assertEquals("Ayende", resultDoc.getDataAsJson().value(String.class, "Name"));
      assertEquals("a", resultDoc.getDataAsJson().value(String.class, "Test"));
    }
  }

  @Test
  public void willNotErrorOnMissingDocument() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      ScriptedPatchRequest patchRequest = new ScriptedPatchRequest();
      patchRequest.setScript("this.Test = 'a';");
      store.getDatabaseCommands().patch("products/1", patchRequest);
    }
  }

  // some tests were omitted as we don't want to test server here!

  @Test
  public void shouldThrowConcurrencyExceptionIfNonCurrentEtagWasSpecified() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        CustomType c1 = new CustomType();
        c1.setValue(10);
        session.store(c1);
        session.saveChanges();
      }

      ScriptedPatchRequest patch = new ScriptedPatchRequest();
      patch.setScript("PutDocument('Items/1', {'Property': 'Value'}, {'@etag' : '01000000-0000-0003-0000-0000000000A0'} );");
      try {
        store.getDatabaseCommands().patch("CustomTypes/1", patch);
        fail();
      } catch (ConcurrencyException e) {
        //ok
        assertTrue(e.getMessage().contains("PUT attempted on document 'Items/1' using a non current etag (document deleted)"));
      }
    }
  }

  @Test
  public void canCreateDocumentsIfPatchingAppliedByIndex() throws Exception {
    CustomType item1 = new CustomType();
    item1.setId("Item/1");
    item1.setValue(1);

    CustomType item2 = new CustomType();
    item2.setId("Item/2");
    item2.setValue(2);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(item1);
        session.store(item2);
        session.saveChanges();
      }

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from doc in docs select new { doc.Value }");

      store.getDatabaseCommands().putIndex("TestIndex", indexDefinition);

      store.openSession().advanced().luceneQuery(CustomType.class, "TestIndex")
      .waitForNonStaleResults().toList();

      ScriptedPatchRequest patchRequest = new ScriptedPatchRequest();
      patchRequest.setScript("PutDocument('NewItem/3', {'CopiedValue': this.Value});");

      store.getDatabaseCommands().updateByIndex("TestIndex", new IndexQuery("Value:1"), patchRequest).waitForCompletion();

      List<JsonDocument> jsonDocuments = store.getDatabaseCommands().getDocuments(0, 10);
      assertEquals(3, jsonDocuments.size());

      JsonDocument jsonDocument = store.getDatabaseCommands().get("NewItem/3");
      assertEquals(new Integer(1), jsonDocument.getDataAsJson().value(int.class, "CopiedValue"));

    }
  }

  public void executeTest(IDocumentStore store) throws Exception {
    try (IDocumentSession s = store.openSession()) {
      s.store(test);
      s.saveChanges();
    }

    store.getDatabaseCommands().patch(test.getId(), new ScriptedPatchRequest(sampleScript));

    JsonDocument resultDoc = store.getDatabaseCommands().get(test.getId());
    RavenJObject resultJson = resultDoc.getDataAsJson();
    CustomType result = JsonExtensions.createDefaultJsonSerializer().readValue(resultJson.toString(), CustomType.class);

    assertNotEquals("Something new", resultDoc.getMetadata().value(String.class, "@id"));
    assertEquals(2, result.getComments().size());
    assertEquals("one test", result.getComments().get(0));
    assertEquals("two", result.getComments().get(1));
    assertEquals(12144, result.getValue());
    assertEquals("err!!", resultJson.get("newValue").toString());
  }

  private void executeSetBasedTest(IDocumentStore store) throws Exception {
    CustomType item1 = new CustomType();
    item1.setId("someId/");
    item1.setOwner("bob");
    item1.setValue(12143);
    item1.setComments(Arrays.asList("one", "two", "seven"));

    CustomType item2 = new CustomType();
    item2.setId("someId/");
    item2.setOwner("NOT bob");
    item2.setValue(9999);
    item2.setComments(Arrays.asList("one", "two", "seven"));

    try (IDocumentSession session = store.openSession()) {
      session.store(item1);
      session.store(item2);
      session.saveChanges();
    }
    IndexDefinition indexDefinition = new IndexDefinition();
    indexDefinition.setMap("from doc in docs select new { doc.Owner} ");
    store.getDatabaseCommands().putIndex("TestIndex", indexDefinition);

    store.openSession().advanced().luceneQuery(CustomType.class, "TestIndex").waitForNonStaleResults().toList();

    store.getDatabaseCommands().updateByIndex("TestIndex", new IndexQuery("Owner:Bob"), new ScriptedPatchRequest(sampleScript)).waitForCompletion();

    RavenJObject item1ResultJson = store.getDatabaseCommands().get(item1.getId()).getDataAsJson();
    CustomType item1Result = JsonExtensions.createDefaultJsonSerializer().readValue(item1ResultJson.toString(), CustomType.class);
    assertEquals(2, item1Result.getComments().size());
    assertEquals("one test", item1Result.getComments().get(0));
    assertEquals("two", item1Result.getComments().get(1));
    assertEquals(12144, item1Result.getValue());
    assertEquals("err!!", item1ResultJson.get("newValue").toString());

    RavenJObject item2ResultJson = store.getDatabaseCommands().get(item2.getId()).getDataAsJson();
    CustomType item2Result = JsonExtensions.createDefaultJsonSerializer().readValue(item2ResultJson.toString(), CustomType.class);
    assertEquals(9999, item2Result.getValue());
    assertEquals(3, item2Result.getComments().size());
    assertEquals("one", item2Result.getComments().get(0));
    assertEquals("two", item2Result.getComments().get(1));
    assertEquals("seven", item2Result.getComments().get(2));

  }

  public static class CustomType {
    private String id;
    private String owner;
    private int value;
    private List<String> comments;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public String getOwner() {
      return owner;
    }
    public void setOwner(String owner) {
      this.owner = owner;
    }
    public int getValue() {
      return value;
    }
    public void setValue(int value) {
      this.value = value;
    }
    public List<String> getComments() {
      return comments;
    }
    public void setComments(List<String> comments) {
      this.comments = comments;
    }
    public CustomType(String id, String owner, int value, List<String> comments) {
      super();
      this.id = id;
      this.owner = owner;
      this.value = value;
      this.comments = comments;
    }
    public CustomType() {
      super();
    }


  }
}

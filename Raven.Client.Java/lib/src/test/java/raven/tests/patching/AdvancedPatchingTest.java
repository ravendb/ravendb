package raven.tests.patching;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import raven.abstractions.data.JsonDocument;
import raven.abstractions.data.PatchRequest;
import raven.abstractions.data.ScriptedPatchRequest;
import raven.abstractions.exceptions.ConcurrencyException;
import raven.abstractions.extensions.JsonExtensions;
import raven.abstractions.json.linq.RavenJObject;
import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;

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
      CustomType result = JsonExtensions.getDefaultObjectMapper().readValue(resultJson.toString(), CustomType.class);

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
      CustomType result = JsonExtensions.getDefaultObjectMapper().readValue(resultJson.toString(), CustomType.class);
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
        //TOOD: finish me
      }
    }
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

package raven.tests.patching;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import raven.abstractions.data.IndexQuery;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.data.PatchCommandType;
import raven.abstractions.data.PatchRequest;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJValue;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;

public class MetadataPatchingTest extends RemoteClientTest {
  @Test
  public void changeRavenEntityName() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.getDatabaseCommands().put("foos/1", null, RavenJObject.parse("{'Something':'something'}"), RavenJObject.parse("{'Raven-Entity-Name': 'Foos'}"));

      PatchRequest patchRequest = new PatchRequest();
      patchRequest.setType(PatchCommandType.MODIFY);
      patchRequest.setName("@metadata");

      PatchRequest subRequest = new PatchRequest();
      subRequest.setType(PatchCommandType.SET);
      subRequest.setName("Raven-Entity-Name");
      subRequest.setValue(new RavenJValue("Bars"));

      patchRequest.setNested(new PatchRequest[] { subRequest } );

      store.getDatabaseCommands().updateByIndex("Raven/DocumentsByEntityName", new IndexQuery(), new PatchRequest[] {
        patchRequest
      }, false).waitForCompletion();
      waitForNonStaleIndexes(store.getDatabaseCommands());

      JsonDocument jsonDocument = store.getDatabaseCommands().get("foos/1");
      assertEquals("Bars", jsonDocument.getMetadata().value(String.class, "Raven-Entity-Name"));

    }
  }
}

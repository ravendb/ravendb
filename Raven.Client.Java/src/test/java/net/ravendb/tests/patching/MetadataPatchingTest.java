package net.ravendb.tests.patching;

import static org.junit.Assert.assertEquals;

import net.ravendb.abstractions.data.IndexQuery;
import net.ravendb.abstractions.data.JsonDocument;
import net.ravendb.abstractions.data.PatchCommandType;
import net.ravendb.abstractions.data.PatchRequest;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJValue;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;

import org.junit.Test;


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

      waitForNonStaleIndexes(store.getDatabaseCommands());

      store.getDatabaseCommands().updateByIndex("Raven/DocumentsByEntityName", new IndexQuery(), new PatchRequest[] {
        patchRequest
      }, false).waitForCompletion();
      waitForNonStaleIndexes(store.getDatabaseCommands());

      JsonDocument jsonDocument = store.getDatabaseCommands().get("foos/1");
      assertEquals("Bars", jsonDocument.getMetadata().value(String.class, "Raven-Entity-Name"));

    }
  }
}

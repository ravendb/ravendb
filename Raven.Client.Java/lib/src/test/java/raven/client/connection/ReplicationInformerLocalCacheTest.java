package raven.client.connection;

import java.util.Date;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import raven.abstractions.data.Etag;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJValue;


public class ReplicationInformerLocalCacheTest {

  @Test
  public void simpleCacheTest(){

    Date now = new Date();

    RavenJObject doc = new RavenJObject();
    doc.add("date", new RavenJValue(now));
    RavenJObject meta = new RavenJObject();
    meta.add("Content-Type", new RavenJValue("application/json"));

    JsonDocument document = new JsonDocument(doc, meta, "test/1", true, Etag.empty(), now);

    ReplicationInformerLocalCache.trySavingReplicationInformationToLocalCache("serverHash", document);

    JsonDocument result = ReplicationInformerLocalCache.tryLoadReplicationInformationFromLocalCache("serverHash");

    assertEquals(now, result.getDataAsJson().get("date").value(Date.class));
  }

}

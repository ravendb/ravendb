package net.ravendb.client.connection;

import java.util.Calendar;
import java.util.Date;

import static org.junit.Assert.assertEquals;

import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.JsonDocument;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJValue;
import net.ravendb.client.connection.ReplicationInformerLocalCache;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Test;



public class ReplicationInformerLocalCacheTest {

  @Test
  public void simpleCacheTest() {

    Date now = DateUtils.truncate(new Date(), Calendar.SECOND);

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

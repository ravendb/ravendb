package net.ravendb.client.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.JsonDocument;
import net.ravendb.abstractions.data.UuidType;
import net.ravendb.abstractions.json.linq.RavenJArray;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.replication.ReplicationDocument;
import net.ravendb.client.connection.IDatabaseCommands;

import org.junit.Test;



public class ReplicationTest extends AbstractReplicationTest {

  @Test
  public void testCreateDb() throws Exception {
    try {
      createDb(SOURCE, 1);
      createDb(TARGET, 2);

      List<String> result = serverClient.getDatabaseNames(2);

      assertEquals(1, result.size());
      assertTrue(result.contains(SOURCE));

      IDatabaseCommands source = serverClient.forDatabase(SOURCE);
      IDatabaseCommands target = serverClient2.forDatabase(TARGET);

      ReplicationDocument repDoc = createReplicationDocument();

      RavenJObject o = new RavenJObject();
      List<String> destinations = new ArrayList<>();
      destinations.add(DEFAULT_SERVER_URL_2);
      o.add("Destinations", new RavenJArray(destinations));

      source.put("Raven/Replication/Destinations", null, RavenJObject.fromObject(repDoc), new RavenJObject());

      Etag etag = new Etag();
      etag.setup(UuidType.DOCUMENTS, System.currentTimeMillis());

      source.put("testVal1", etag, RavenJObject.parse("{ \"key\" : \"val1\"}"), new RavenJObject());
      assertNotNull(result);

      JsonDocument jsonDocument = source.get("testVal1");
      assertEquals("val1", jsonDocument.getDataAsJson().get("key").value(String.class));

      for (int i = 0; i < 500; i++) {
        Thread.sleep(250);
        jsonDocument = target.get("testVal1");
        if (jsonDocument == null) {
          continue;
        }
        assertEquals("val1", jsonDocument.getDataAsJson().get("key").value(String.class));
        break;
      }

    } finally {
      deleteDb(SOURCE, 1);
      deleteDb(TARGET, 2);
    }
  }




}

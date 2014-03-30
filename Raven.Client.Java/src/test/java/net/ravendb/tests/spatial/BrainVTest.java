package net.ravendb.tests.spatial;

import static org.junit.Assert.assertEquals;

import java.util.List;

import net.ravendb.abstractions.indexing.FieldIndexing;
import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;

import org.junit.Test;



public class BrainVTest extends RemoteClientTest {

  @Test
  public void canPerformSpatialSearchWithNulls() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from e in docs.Events select new { Tag = \"Event\", _ = SpatialGenerate(e.Latitude, e.Longitude) }");
      indexDefinition.getIndexes().put("Tag", FieldIndexing.NOT_ANALYZED);

      store.getDatabaseCommands().putIndex("eventsByLatLng", indexDefinition);

      store.getDatabaseCommands().put("Events/1", null, RavenJObject.parse("{\"Venue\": \"Jimmy's Old Town Tavern\", \"Latitude\": null, \"Longitude\": null }"),
        RavenJObject.parse("{'Raven-Entity-Name': 'Events'}"));

      try (IDocumentSession session = store.openSession()) {
        List<Object> objects = session.query(Object.class, "eventsByLatLng").customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults()).toList();
        assertEquals(0, store.getDatabaseCommands().getStatistics().getErrors().length);
        assertEquals(1, objects.size());
      }
    }
  }

  @Test
  public void canUseNullCoalescingOperator() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from e in docs.Events select new { Tag = \"Event\", _ = SpatialGenerate(e.Latitude ?? 38.9103000, e.Longitude ?? -77.3942) }");
      indexDefinition.getIndexes().put("Tag", FieldIndexing.NOT_ANALYZED);

      store.getDatabaseCommands().putIndex("eventsByLatLng", indexDefinition);

      store.getDatabaseCommands().put("Events/1", null, RavenJObject.parse("{\"Venue\": \"Jimmy's Old Town Tavern\", \"Latitude\": null, \"Longitude\": null }"),
        RavenJObject.parse("{'Raven-Entity-Name': 'Events'}"));

      try (IDocumentSession session = store.openSession()) {
        List<Object> objects = session.query(Object.class, "eventsByLatLng")
          .customize(new DocumentQueryCustomizationFactory().withinRadiusOf(6, 38.9103000, -77.3942))
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults()).toList();
        assertEquals(0, store.getDatabaseCommands().getStatistics().getErrors().length);
        assertEquals(1, objects.size());
      }
    }
  }

}

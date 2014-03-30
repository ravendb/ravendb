package net.ravendb.tests.spatial;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import net.ravendb.abstractions.indexing.SpatialOptions.SpatialRelation;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;


public class TwoLocationsTest extends RemoteClientTest {

  public static class Event {
    private String name;
    private List<Location> locations;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public List<Location> getLocations() {
      return locations;
    }

    public void setLocations(List<Location> locations) {
      this.locations = locations;
    }

  }

  public static class Location {
    private double lng;
    private double lat;

    public double getLng() {
      return lng;
    }

    public void setLng(double lng) {
      this.lng = lng;
    }

    public double getLat() {
      return lat;
    }

    public void setLat(double lat) {
      this.lat = lat;
    }
  }

  private static void setup(IDocumentStore store) throws Exception {
    try (IDocumentSession session = store.openSession()) {
      Location loc1 = new Location();
      loc1.setLat(32.1067536);
      loc1.setLng(34.8357353);

      Location loc2 = new Location();
      loc2.setLat(32.0624912);
      loc2.setLng(34.7700725);

      Event event = new Event();
      event.setName("Trial");
      event.setLocations(Arrays.asList(loc1, loc2));

      session.store(event);
      session.saveChanges();
    }
  }

  @Test
  public void canQueryByMultipleLocations() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new MultiLocations().execute(store);
      setup(store);

      try (IDocumentSession session = store.openSession()) {
        List<Event> list = session.query(Event.class, MultiLocations.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .customize(new DocumentQueryCustomizationFactory().withinRadiusOf(1, 32.0590291, 34.7707401))
          .toList();

        assertEquals(0, store.getDatabaseCommands().getStatistics().getErrors().length);
        assertTrue(list.size() > 0);
      }

      try (IDocumentSession session = store.openSession()) {
        List<Event> list = session.query(Event.class, MultiLocations.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .customize(new DocumentQueryCustomizationFactory().withinRadiusOf(1, 32.1104641, 34.8417456))
          .toList();

        assertEquals(0, store.getDatabaseCommands().getStatistics().getErrors().length);
        assertTrue(list.size() > 0);
      }
    }
  }

  @Test
  public void canQueryByMultipleLocations2() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new MultiLocationsCustomFieldName().execute(store);
      setup(store);

      try (IDocumentSession session = store.openSession()) {
        List<Event> list = session.query(Event.class, MultiLocationsCustomFieldName.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .customize(new DocumentQueryCustomizationFactory().withinRadiusOf("someField", 1, 32.0590291, 34.7707401))
          .toList();

        assertEquals(0, store.getDatabaseCommands().getStatistics().getErrors().length);
        assertTrue(list.size() > 0);
      }

      try (IDocumentSession session = store.openSession()) {
        List<Event> list = session.query(Event.class, MultiLocationsCustomFieldName.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .customize(new DocumentQueryCustomizationFactory().withinRadiusOf("someField", 1, 32.1104641, 34.8417456))
          .toList();

        assertEquals(0, store.getDatabaseCommands().getStatistics().getErrors().length);
        assertTrue(list.size() > 0);
      }
    }
  }

  @Test
  public void canQueryByMultipleLocationsRawOverHttp() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      new MultiLocationsCustomFieldName().execute(store);

      setup(store);

      try (IDocumentSession session = store.openSession()) {
        List<Event> list = session.query(Event.class, MultiLocationsCustomFieldName.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .customize(new DocumentQueryCustomizationFactory().relatesToShape("someField", "Circle(34.770740 32.059029 d=1.000000)", SpatialRelation.WITHIN))
          .toList();
        assertTrue(list.size() > 0);
      }

      try (IDocumentSession session = store.openSession()) {
        List<Event> list = session.query(Event.class, MultiLocationsCustomFieldName.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .customize(new DocumentQueryCustomizationFactory().relatesToShape("someField", "Circle(34.770740 32.059029 d=1.000000)", SpatialRelation.WITHIN))
          .toList();
        assertTrue(list.size() > 0);
      }
    }
  }

  public static class MultiLocations extends AbstractIndexCreationTask {
    public MultiLocations() {
      map = " from e in docs.Events select new { e.Name, _ = e.Locations.Select(x => SpatialGenerate(x.Lat, x.Lng)) } " ;
    }
  }

  public static class MultiLocationsCustomFieldName extends AbstractIndexCreationTask {
    public MultiLocationsCustomFieldName() {
      map = " from e in docs.Events select new { e.Name, _ = e.Locations.Select(x => SpatialGenerate(\"someField\", x.Lat, x.Lng)) } ";
    }
  }

}

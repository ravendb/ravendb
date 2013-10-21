package net.ravendb.tests.spatial;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.abstractions.indexing.SpatialOptions.SpatialUnits;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;

import org.junit.Test;



public class SpatialQueriesTest extends RemoteClientTest {
  public static class Listing {
    private String classCodes;
    private long latitude;
    private long longitude;

    public String getClassCodes() {
      return classCodes;
    }

    public void setClassCodes(String classCodes) {
      this.classCodes = classCodes;
    }

    public long getLatitude() {
      return latitude;
    }

    public void setLatitude(long latitude) {
      this.latitude = latitude;
    }

    public long getLongitude() {
      return longitude;
    }

    public void setLongitude(long longitude) {
      this.longitude = longitude;
    }
  }

  public static class SpatialQueriesInMemoryTestIdx extends AbstractIndexCreationTask {
    public SpatialQueriesInMemoryTestIdx() {
      map = "from listingItem in docs.Listings select new  { "
        + "  listingItem.ClassCodes, "
        + "  listingItem.Latitude,"
        + "  listingItem.Longitude, "
        + "  _ = SpatialGenerate(listingItem.Latitude, listingItem.Longitude) "
        + " }";
    }
  }

  @Test
  public void canRunSpatialQueriesInMemory() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new SpatialQueriesInMemoryTestIdx().execute(store);
    }
  }

  //Failing test from http://groups.google.com/group/ravendb/browse_thread/thread/7a93f37036297d48/
  @Test
  public void canSuccessfullyDoSpatialQueryOfNearbyLocations() throws Exception {
    // These items is in a radius of 4 miles (approx 6,5 km)
    DummyGeoDoc areaOneDocOne = new DummyGeoDoc(55.6880508001, 13.5717346673);
    DummyGeoDoc areaOneDocTwo = new DummyGeoDoc(55.6821978456, 13.6076183965);
    DummyGeoDoc areaOneDocThree = new DummyGeoDoc(55.673251569, 13.5946697607);

    // This item is 12 miles (approx 19 km) from the closest in areaOne
    DummyGeoDoc closeButOutsideAreaOne = new DummyGeoDoc(55.8634157297, 13.5497731987);

    // This item is about 3900 miles from areaOne
    DummyGeoDoc newYork = new DummyGeoDoc(40.7137578228, -74.0126901936);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(areaOneDocOne);
        session.store(areaOneDocTwo);
        session.store(areaOneDocThree);
        session.store(closeButOutsideAreaOne);
        session.store(newYork);
        session.saveChanges();

        IndexDefinition indexDefinition = new IndexDefinition();
        indexDefinition.setMap("from doc in docs select new { _ = SpatialGenerate(doc.Latitude, doc.Longitude) }");

        store.getDatabaseCommands().putIndex("FindByLatLng", indexDefinition);

        // wait until the index is built
        session.advanced().luceneQuery(DummyGeoDoc.class, "FindByLatLng")
        .waitForNonStaleResults()
        .toList();

        double lat = 55.6836422426, lng = 13.5871808352; // in the middle of AreaOne
        double radius = 5.0;

        // Expected is that 5.0 will return 3 results
        List<DummyGeoDoc> nearbyDocs = session.advanced().luceneQuery(DummyGeoDoc.class, "FindByLatLng")
          .withinRadiusOf(radius, lat, lng)
          .waitForNonStaleResults()
          .toList();

        assertNotNull(nearbyDocs);
        assertEquals(3, nearbyDocs.size());
      }
    }
  }

  @Test
  public void canSuccessfullyQueryByMiles() throws Exception {
    DummyGeoDoc myHouse = new DummyGeoDoc(44.757767, -93.355322);

    // The gym is about 7.32 miles (11.79 kilometers) from my house.
    DummyGeoDoc gym = new DummyGeoDoc(44.682861, -93.25);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(myHouse);
        session.store(gym);
        session.saveChanges();

        IndexDefinition indexDefinition = new IndexDefinition();
        indexDefinition.setMap("from doc in docs select new { _ = SpatialGenerate(doc.Latitude, doc.Longitude) }");

        store.getDatabaseCommands().putIndex("FindByLatLng", indexDefinition);
        // wait until the index is built
        session.advanced().luceneQuery(DummyGeoDoc.class, "FindByLatLng")
        .waitForNonStaleResults()
        .toList();

        double radius = 8;

        // Find within 8 miles.
        // We should find both my house and the gym.
        List<DummyGeoDoc> matchesWithinMiles = session.advanced().luceneQuery(DummyGeoDoc.class, "FindByLatLng")
          .withinRadiusOf(radius, myHouse.getLatitude(), myHouse.getLongitude(), SpatialUnits.MILES)
          .waitForNonStaleResults()
          .toList();
        assertNotNull(matchesWithinMiles);
        assertEquals(2, matchesWithinMiles.size());

        // Find within 8 kilometers.
        // We should find only my house, since the gym is ~11 kilometers out.
        List<DummyGeoDoc> matchesWithinKilometers =  session.advanced().luceneQuery(DummyGeoDoc.class, "FindByLatLng")
          .withinRadiusOf(radius, myHouse.getLatitude(), myHouse.getLongitude(), SpatialUnits.KILOMETERS)
          .waitForNonStaleResults()
          .toList();
        assertNotNull(matchesWithinKilometers);
        assertEquals(1, matchesWithinKilometers.size());


      }
    }


  }

  public static class DummyGeoDoc {
    private String id;
    private double latitude;
    private double longitude;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public double getLatitude() {
      return latitude;
    }

    public void setLatitude(double latitude) {
      this.latitude = latitude;
    }

    public double getLongitude() {
      return longitude;
    }

    public void setLongitude(double longitude) {
      this.longitude = longitude;
    }

    public DummyGeoDoc(double latitude, double longitude) {
      super();
      this.latitude = latitude;
      this.longitude = longitude;
    }


  }

}

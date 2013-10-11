package raven.tests.spatial;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;

import raven.abstractions.data.FacetResults;
import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;
import raven.client.indexes.AbstractIndexCreationTask;


public class ClusteringTest extends RemoteClientTest {

  @QueryEntity
  public static class Location {
    private double lng;
    private double lat;
    private String name;

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

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  public static class LocationClustering extends AbstractIndexCreationTask {
    public LocationClustering() {
      map = "from location in docs.Locations "
        + "select new  { "
        + "    _ = SpatialGenerate(location.Lat, location.Lng), "
        + "    __ = SpatialClustering(\"Cluster\", location.Lat, location.Lng) "
        + " }";
    }
  }

  @Test
  public void canClusterData() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      new LocationClustering().execute(store);

      try (IDocumentSession session = store.openSession()) {
        Location location1 = new Location();
        location1.setLat(32.44611);
        location1.setLng(34.91098);
        location1.setName("Office");
        session.store(location1);

        Location location2 = new Location();
        location2.setLat(32.43734);
        location2.setLng(34.92110);
        location2.setName("Mall");
        session.store(location2);

        Location location3 = new Location();
        location3.setLat(32.43921);
        location3.setLng(34.90127);
        location3.setName("Rails");
        session.store(location3);

        session.saveChanges();
      }

      waitForNonStaleIndexes(store.getDatabaseCommands());

      QClusteringTest_Location x = QClusteringTest_Location.location;

      try (IDocumentSession s = store.openSession()) {
        FacetResults results = s.query(Location.class, LocationClustering.class)
          .aggregateBy("Cluster_5")
          .countOn(x.name)
          .andAggregateOn("Cluster_8")
          .countOn(x.name)
          .toList();

        assertEquals(1, results.getResults().get("Cluster_5").getValues().size());
        assertEquals(3, results.getResults().get("Cluster_8").getValues().size());
      }
    }
  }

}

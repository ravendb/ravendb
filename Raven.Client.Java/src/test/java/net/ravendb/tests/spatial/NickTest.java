package net.ravendb.tests.spatial;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.data.SpatialIndexQuery;
import net.ravendb.abstractions.indexing.SpatialOptions.SpatialRelation;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;

import org.junit.Test;



public class NickTest extends RemoteClientTest {

  public static class MySpatialDocument {
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

  }

  public static class MySpatialIndex extends AbstractIndexCreationTask {
    public MySpatialIndex() {
      map = "from entity in docs.MySpatialDocuments select new { _ = SpatialGenerate(entity.Latitude, entity.Longitude) }";
    }
  }

  @Test
  public void test() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        MySpatialDocument mySpatialDocument = new MySpatialDocument();
        mySpatialDocument.setId("spatials/1");
        mySpatialDocument.setLatitude(48.3708044);
        mySpatialDocument.setLongitude(2.8028712999999925);
        session.store(mySpatialDocument);
        session.saveChanges();
      }

      new MySpatialIndex().execute(store);

      waitForNonStaleIndexes(store.getDatabaseCommands());

      // Distance between the 2 tested points is 35.75km
      // (can be checked here: http://www.movable-type.co.uk/scripts/latlong.html

      try (IDocumentSession session = store.openSession()) {
        MySpatialDocument result = session.advanced().luceneQuery(MySpatialDocument.class, MySpatialIndex.class)
          // 1.025 is for the 2.5% uncertainty at the circle circumference
          .withinRadiusOf(35.75* 1.025,  48.6003516, 2.4632387000000335)
          .singleOrDefault();

        assertNotNull(result);  // A location should be returned.

        result = session.advanced().luceneQuery(MySpatialDocument.class, MySpatialIndex.class)
          .withinRadiusOf(30,  48.6003516, 2.4632387000000335)
          .singleOrDefault();

        assertNull(result); // No result should be returned.

        result = session.advanced().luceneQuery(MySpatialDocument.class, MySpatialIndex.class)
          .withinRadiusOf(33,  48.6003516, 2.4632387000000335)
          .singleOrDefault();

        assertNull(result);  // A location should be returned.

        String shape = SpatialIndexQuery.getQueryShapeFromLatLon(48.6003516, 2.4632387000000335, 33);
        result = session.advanced().luceneQuery(MySpatialDocument.class, MySpatialIndex.class)
          .relatesToShape(Constants.DEFAULT_SPATIAL_FIELD_NAME, shape, SpatialRelation.INTERSECTS, 0)
          .singleOrDefault();

        assertNull(result);  // A location should be returned.
      }



    }
  }
}

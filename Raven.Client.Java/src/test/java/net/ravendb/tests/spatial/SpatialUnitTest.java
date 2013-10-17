package net.ravendb.tests.spatial;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;

import net.ravendb.abstractions.indexing.SpatialOptions.SpatialUnits;
import net.ravendb.abstractions.indexing.SpatialOptionsFactory;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.client.spatial.SpatialCriteriaFactory;

import com.mysema.query.annotations.QueryEntity;


public class SpatialUnitTest extends RemoteClientTest {

  @Test
  public void test() throws Exception {
    DummyGeoDoc myHouse = new DummyGeoDoc(44.757767, -93.355322);
    // The gym is about 7.32 miles (11.79 kilometers) from my house.
    DummyGeoDoc gym = new DummyGeoDoc(44.682861, -93.25);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      store.executeIndex(new KmGeoIndex());
      store.executeIndex(new MilesGeoIndex());

      try (IDocumentSession session = store.openSession()) {
        session.store(myHouse);
        session.store(gym);
        session.saveChanges();
      }

      waitForNonStaleIndexes(store.getDatabaseCommands());

      QSpatialUnitTest_DummyGeoDoc x = QSpatialUnitTest_DummyGeoDoc.dummyGeoDoc;

      try (IDocumentSession session = store.openSession()) {
        int km = session.query(DummyGeoDoc.class, KmGeoIndex.class)
          .spatial(x.location, new SpatialCriteriaFactory().withinRadiusOf(8, myHouse.getLongitude(), myHouse.getLatitude()))
          .count();

        assertEquals(1, km);

        int miles = session.query(DummyGeoDoc.class, MilesGeoIndex.class)
          .spatial(x.location, new SpatialCriteriaFactory().withinRadiusOf(8, myHouse.getLongitude(), myHouse.getLatitude()))
          .count();

        assertEquals(2, miles);
      }

      try (IDocumentSession session = store.openSession()) {
        int km = session.query(DummyGeoDoc.class, KmGeoIndex.class)
          .customize(new DocumentQueryCustomizationFactory().withinRadiusOf("Location", 8, myHouse.getLatitude(), myHouse.getLongitude()))
          .count();

        assertEquals(1, km);

        int miles = session.query(DummyGeoDoc.class, MilesGeoIndex.class)
          .customize(new DocumentQueryCustomizationFactory().withinRadiusOf("Location", 8, myHouse.getLatitude(), myHouse.getLongitude()))
          .count();

        assertEquals(2, miles);
      }

      try (IDocumentSession session = store.openSession()) {
        int km = session.query(DummyGeoDoc.class, KmGeoIndex.class)
          .customize(new DocumentQueryCustomizationFactory().withinRadiusOf("Location", 8, myHouse.getLatitude(), myHouse.getLongitude(), SpatialUnits.KILOMETERS))
          .count();

        assertEquals(1, km);

        int miles = session.query(DummyGeoDoc.class, MilesGeoIndex.class)
          .customize(new DocumentQueryCustomizationFactory().withinRadiusOf("Location", 8, myHouse.getLatitude(), myHouse.getLongitude(), SpatialUnits.MILES))
          .count();

        assertEquals(2, miles);
      }

    }
  }

  public static class KmGeoIndex extends AbstractIndexCreationTask {
    public KmGeoIndex() {
      map = "from doc in docs.DummyGeoDocs select new { doc.Location } ";
      QSpatialUnitTest_DummyGeoDoc x = QSpatialUnitTest_DummyGeoDoc.dummyGeoDoc;
      spatial(x.location, new SpatialOptionsFactory().getGeography().defaultOptions(SpatialUnits.KILOMETERS));
    }
  }

  public static class MilesGeoIndex extends AbstractIndexCreationTask {
    public MilesGeoIndex() {
      map = "from doc in docs.DummyGeoDocs select new { doc.Location } ";
      QSpatialUnitTest_DummyGeoDoc x = QSpatialUnitTest_DummyGeoDoc.dummyGeoDoc;
      spatial(x.location, new SpatialOptionsFactory().getGeography().defaultOptions(SpatialUnits.MILES));
    }
  }

  @QueryEntity
  public static class DummyGeoDoc {
    private String id;
    private double[] location;
    private double latitude;
    private double longitude;

    public DummyGeoDoc(double lat, double lng) {
      latitude = lat;
      longitude = lng;
      location = new double[] { lng, lat};
    }


    public String getId() {
      return id;
    }


    public void setId(String id) {
      this.id = id;
    }


    public double[] getLocation() {
      return location;
    }


    public void setLocation(double[] location) {
      this.location = location;
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
}

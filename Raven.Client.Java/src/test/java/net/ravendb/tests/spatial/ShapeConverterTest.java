package net.ravendb.tests.spatial;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import net.ravendb.abstractions.indexing.SpatialOptionsFactory;
import net.ravendb.abstractions.indexing.SpatialOptionsFactory.SpatialBounds;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.client.spatial.SpatialCriteriaFactory;
import net.ravendb.tests.spatial.QShapeConverterTest_SpatialDoc;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;



public class ShapeConverterTest extends RemoteClientTest {

  @Test
  public void points() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      store.executeIndex(new CartesianIndex());

      try (IDocumentSession session = store.openSession()) {
        SpatialDoc spatialDoc = new SpatialDoc();
        spatialDoc.setGeometry("POLYGON ((1850 1850, 1950 1850, 1950 1950,1850 1950, 1850 1850))");
        session.store(spatialDoc);
        session.saveChanges();
      }

      waitForNonStaleIndexes(store.getDatabaseCommands());

      QShapeConverterTest_SpatialDoc x = QShapeConverterTest_SpatialDoc.spatialDoc;

      try (IDocumentSession session = store.openSession()) {
        Map<String, Object> map = new HashMap<>();
        map.put("type", "Point");
        map.put("coordinates", new int[] { 1900, 1900});
        boolean matches1 = session.query(SpatialDoc.class, CartesianIndex.class)
          .spatial(x.geometry, new SpatialCriteriaFactory().intersects(map))
          .any();

        assertTrue(matches1);

        boolean matches2 = session.query(SpatialDoc.class, CartesianIndex.class)
          .spatial(x.geometry, new SpatialCriteriaFactory().intersects(new int[] { 1900, 1900}))
          .any();
        assertTrue(matches2);


        map = new HashMap<>();
        map.put("Latitude", 1900);
        map.put("Longitude", 1900);
        boolean matches3 = session.query(SpatialDoc.class, CartesianIndex.class)
          .spatial(x.geometry, new SpatialCriteriaFactory().intersects(map))
          .any();

        assertTrue(matches3);


        map = new HashMap<>();
        map.put("X", 1900);
        map.put("Y", 1900);
        boolean matches4 = session.query(SpatialDoc.class, CartesianIndex.class)
          .spatial(x.geometry, new SpatialCriteriaFactory().intersects(map))
          .any();

        assertTrue(matches4);

        map = new HashMap<>();
        map.put("lat", 1900);
        map.put("lng", 1900);
        boolean matches5 = session.query(SpatialDoc.class, CartesianIndex.class)
          .spatial(x.geometry, new SpatialCriteriaFactory().intersects(map))
          .any();

        assertTrue(matches5);

        map = new HashMap<>();
        map.put("lat", 1900);
        map.put("Long", 1900);
        boolean matches6 = session.query(SpatialDoc.class, CartesianIndex.class)
          .spatial(x.geometry, new SpatialCriteriaFactory().intersects(map))
          .any();

        assertTrue(matches6);

        map = new HashMap<>();
        map.put("lat", 1900);
        map.put("lon", 1900);
        boolean matches7 = session.query(SpatialDoc.class, CartesianIndex.class)
          .spatial(x.geometry, new SpatialCriteriaFactory().intersects(map))
          .any();

        assertTrue(matches7);

        boolean matches8 = session.query(SpatialDoc.class, CartesianIndex.class)
          .spatial(x.geometry, new SpatialCriteriaFactory().intersects(new double[] { 1900.0, 1900.0}))
          .any();
        assertTrue(matches8);

      }
    }
  }

  @QueryEntity
  public static class SpatialDoc {
    private String id;
    private String geometry;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getGeometry() {
      return geometry;
    }

    public void setGeometry(String geometry) {
      this.geometry = geometry;
    }
  }

  public static class CartesianIndex extends AbstractIndexCreationTask {
    public CartesianIndex() {
      map = "from doc in docs.SpatialDocs select new { doc.Geometry }";
      QShapeConverterTest_SpatialDoc x = QShapeConverterTest_SpatialDoc.spatialDoc;
      spatial(x.geometry, new SpatialOptionsFactory().getCartesian().quadPrefixTreeIndex(12, new SpatialBounds(0, 0, 2000, 2000)));

    }
  }


}

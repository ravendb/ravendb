package net.ravendb.tests.spatial;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import net.ravendb.abstractions.indexing.SpatialOptionsFactory;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.tests.spatial.QGeoJsonTest_SpatialDoc;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;



public class GeoJsonTest extends RemoteClientTest {

  @QueryEntity
  public static class SpatialDoc {
    private String id;
    private Object geoJson;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public Object getGeoJson() {
      return geoJson;
    }

    public void setGeoJson(Object geoJson) {
      this.geoJson = geoJson;
    }
  }

  public static class GeoJsonIndex extends AbstractIndexCreationTask {
    public GeoJsonIndex() {
      QGeoJsonTest_SpatialDoc x = QGeoJsonTest_SpatialDoc.spatialDoc;
      map = "from doc in docs.SpatialDocs select new { doc.GeoJson }";
      spatial(x.geoJson, new SpatialOptionsFactory().getGeography().defaultOptions());
    }
  }

  @Test
  public void pointTest() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      store.executeIndex(new GeoJsonIndex());

      try (IDocumentSession session = store.openSession()) {
        SpatialDoc spatialDoc = new SpatialDoc();
        Map<String, Object> map = new HashMap<>();
        map.put("type", "Point");
        map.put("coordinates", new double[] { 45.0, 45.0 });
        spatialDoc.setGeoJson(map);

        session.store(spatialDoc);
        session.saveChanges();
      }

      waitForNonStaleIndexes(store.getDatabaseCommands());

      try (IDocumentSession session = store.openSession()) {
        boolean matches = session.query(SpatialDoc.class, GeoJsonIndex.class)
          .customize(new DocumentQueryCustomizationFactory().withinRadiusOf("GeoJson", 700, 40, 40))
          .any();
        assertTrue(matches);
      }
    }
  }

}

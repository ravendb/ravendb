package net.ravendb.tests.spatial;

import static org.junit.Assert.assertTrue;

import net.ravendb.abstractions.indexing.SpatialOptionsFactory;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.client.spatial.SpatialCriteriaFactory;
import net.ravendb.tests.spatial.QGeoUriTest_SpatialDoc;

import org.junit.Test;


import com.mysema.query.annotations.QueryEntity;


public class GeoUriTest extends RemoteClientTest {

  @QueryEntity
  public static class SpatialDoc {
    private String id;
    private Object point;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public Object getPoint() {
      return point;
    }

    public void setPoint(Object point) {
      this.point = point;
    }

  }

  public static class PointIndex extends AbstractIndexCreationTask {
    public PointIndex() {
      map = "from doc in docs.SpatialDocs select new  { doc.Point } ";
      QGeoUriTest_SpatialDoc x = QGeoUriTest_SpatialDoc.spatialDoc;
      spatial(x.point, new SpatialOptionsFactory().getGeography().defaultOptions());
    }
  }

  @Test
  public void pointTest() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.executeIndex(new PointIndex());
      try (IDocumentSession session = store.openSession()) {
        SpatialDoc spatialDoc = new SpatialDoc();
        spatialDoc.setPoint("geo:45.0,45.0,-78.4");
        session.store(spatialDoc);
        session.saveChanges();
      }

      waitForNonStaleIndexes(store.getDatabaseCommands());

      try (IDocumentSession session = store.openSession()) {
        QGeoUriTest_SpatialDoc x = QGeoUriTest_SpatialDoc.spatialDoc;
        boolean matches = session.query(SpatialDoc.class, PointIndex.class)
          .spatial(x.point, new SpatialCriteriaFactory().within("geo:45.0,45.0,-78.4;u=100.0"))
          .any();
        assertTrue(matches);
      }
    }
  }

}

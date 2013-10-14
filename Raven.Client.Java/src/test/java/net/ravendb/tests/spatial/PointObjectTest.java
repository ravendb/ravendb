package net.ravendb.tests.spatial;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import net.ravendb.abstractions.indexing.SpatialOptionsFactory;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.client.spatial.SpatialCriteriaFactory;
import net.ravendb.tests.spatial.QPointObjectTest_SpatialDoc;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;



public class PointObjectTest extends RemoteClientTest {

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
      map = "from doc in docs.SpatialDocs select new { doc.Point } ";
      QPointObjectTest_SpatialDoc x = QPointObjectTest_SpatialDoc.spatialDoc;
      spatial(x.point, new SpatialOptionsFactory().getGeography().defaultOptions());
    }
  }

  @Test
  public void pointTest() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.executeIndex(new PointIndex());

      try (IDocumentSession session = store.openSession()) {
        SpatialDoc doc1 = new SpatialDoc();
        doc1.setPoint(null);
        session.store(doc1);

        SpatialDoc doc2 = new SpatialDoc();
        doc2.setPoint(new double[] { 45.0, 45.0});
        session.store(doc2);

        SpatialDoc doc3 = new SpatialDoc();
        Map<String, Object> map = new HashMap<>();
        doc3.setPoint(map);
        map.put("X", 45.0);
        map.put("Y", 45.0);
        session.store(doc3);

        SpatialDoc doc4 = new SpatialDoc();
        map = new HashMap<>();
        doc4.setPoint(map);
        map.put("Latitude", 45.0);
        map.put("Longitude", 45.0);
        session.store(doc4);

        SpatialDoc doc5 = new SpatialDoc();
        map = new HashMap<>();
        doc5.setPoint(map);
        map.put("lat", 45.0);
        map.put("lon", 45.0);
        session.store(doc5);

        SpatialDoc doc6 = new SpatialDoc();
        map = new HashMap<>();
        doc6.setPoint(map);
        map.put("lat", 45.0);
        map.put("lng", 45.0);
        session.store(doc6);

        SpatialDoc doc7 = new SpatialDoc();
        map = new HashMap<>();
        doc7.setPoint(map);
        map.put("Lat", 45.0);
        map.put("Long", 45.0);
        session.store(doc7);

        SpatialDoc doc8 = new SpatialDoc();
        doc8.setPoint("geo:45.0,45.0,-78.4");
        session.store(doc8);

        SpatialDoc doc9 = new SpatialDoc();
        doc9.setPoint("geo:45.0,45.0,-78.4;u=0.2");
        session.store(doc9);


        session.saveChanges();
      }

      waitForNonStaleIndexes(store.getDatabaseCommands());

      try (IDocumentSession session = store.openSession()) {
        QPointObjectTest_SpatialDoc x = QPointObjectTest_SpatialDoc.spatialDoc;

        int matches = session.query(SpatialDoc.class, PointIndex.class)
          .spatial(x.point, new SpatialCriteriaFactory().withinRadiusOf(700, 40, 40))
          .count();

        assertEquals(8, matches);
      }
    }
  }

}

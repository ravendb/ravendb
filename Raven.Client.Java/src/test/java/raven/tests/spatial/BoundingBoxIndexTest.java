package raven.tests.spatial;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;

import raven.abstractions.indexing.SpatialOptionsFactory;
import raven.abstractions.indexing.SpatialOptionsFactory.SpatialBounds;
import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;
import raven.client.indexes.AbstractIndexCreationTask;
import raven.client.spatial.SpatialCriteriaFactory;


public class BoundingBoxIndexTest extends RemoteClientTest {
  @Test
  public void testBoudingBox() throws Exception {
    // Y YYY Y
    // Y YYY Y
    // Y YYY Y
    // Y     Y
    // YYYYYYY

    String polygon = "POLYGON ((0 0, 0 5, 1 5, 1 1, 5 1, 5 5, 6 5, 6 0, 0 0))";
    String rectangle1 = "2 2 4 4";
    String rectangle2 = "6 6 10 10";
    String rectangle3 = "0 0 6 6";

    QBoundingBoxIndexTest_SpatialDoc x = QBoundingBoxIndexTest_SpatialDoc.spatialDoc;

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      new BBoxIndex().execute(store);
      new QuadTreeIndex().execute(store);

      try (IDocumentSession session = store.openSession()) {
        SpatialDoc spatialDoc = new SpatialDoc();
        spatialDoc.setShape(polygon);
        session.store(spatialDoc);
        session.saveChanges();
      }

      waitForNonStaleIndexes(store.getDatabaseCommands());

      try (IDocumentSession session = store.openSession()) {
        int result = session.query(SpatialDoc.class).count();
        assertEquals(1, result);
      }

      try (IDocumentSession session = store.openSession()) {
        int result = session.query(SpatialDoc.class, BBoxIndex.class)
          .spatial(x.shape, new SpatialCriteriaFactory().intersects(rectangle1)).count();
        assertEquals(1, result);
      }

      try (IDocumentSession session = store.openSession()) {
        int result = session.query(SpatialDoc.class, BBoxIndex.class)
          .spatial(x.shape, new SpatialCriteriaFactory().intersects(rectangle2)).count();
        assertEquals(0, result);
      }

      try (IDocumentSession session = store.openSession()) {
        int result = session.query(SpatialDoc.class, BBoxIndex.class)
          .spatial(x.shape, new SpatialCriteriaFactory().disjoint(rectangle2)).count();
        assertEquals(1, result);
      }

      try (IDocumentSession session = store.openSession()) {
        int result = session.query(SpatialDoc.class, BBoxIndex.class)
          .spatial(x.shape, new SpatialCriteriaFactory().within(rectangle3)).count();
        assertEquals(1, result);
      }

      try (IDocumentSession session = store.openSession()) {
        int result = session.query(SpatialDoc.class, QuadTreeIndex.class)
          .spatial(x.shape, new SpatialCriteriaFactory().intersects(rectangle2)).count();
        assertEquals(0, result);
      }

      try (IDocumentSession session = store.openSession()) {
        int result = session.query(SpatialDoc.class, QuadTreeIndex.class)
          .spatial(x.shape, new SpatialCriteriaFactory().intersects(rectangle1)).count();
        assertEquals(0, result);
      }

    }
  }


  @QueryEntity
  public static class SpatialDoc {
    private String id;
    private String shape;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getShape() {
      return shape;
    }

    public void setShape(String shape) {
      this.shape = shape;
    }

  }

  public static class BBoxIndex extends AbstractIndexCreationTask {
    public BBoxIndex() {
      QBoundingBoxIndexTest_SpatialDoc x = QBoundingBoxIndexTest_SpatialDoc.spatialDoc;
      map = "from doc in docs.SpatialDocs select new  { doc.Shape }";
      spatial(x.shape, new SpatialOptionsFactory().getCartesian().boundingBoxIndex());
    }
  }

  public static class QuadTreeIndex extends AbstractIndexCreationTask {
    public QuadTreeIndex() {
      QBoundingBoxIndexTest_SpatialDoc x = QBoundingBoxIndexTest_SpatialDoc.spatialDoc;
      map = "from doc in docs.SpatialDocs select new  { doc.Shape }";
      spatial(x.shape, new SpatialOptionsFactory().getCartesian().quadPrefixTreeIndex(6, new SpatialBounds(0, 0, 16, 16)));
    }
  }

}

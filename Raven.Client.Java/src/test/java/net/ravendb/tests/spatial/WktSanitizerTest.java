package net.ravendb.tests.spatial;

import static org.junit.Assert.assertEquals;

import net.ravendb.abstractions.indexing.SpatialOptionsFactory;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.spatial.WktSanitizer;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.tests.spatial.QWktSanitizerTest_SpatialDoc;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;



public class WktSanitizerTest extends RemoteClientTest {

  @Test
  public void rectangle() {
    WktSanitizer wkt = new WktSanitizer();
    assertEquals("10.8 34.9 89.0 78.2", wkt.sanitize("10.8 34.9 89.0 78.2"));
  }

  @Test
  public void points() {
    WktSanitizer wkt = new WktSanitizer();
    assertEquals("POINT (0 0)", wkt.sanitize("POINT (0 0)"));
    assertEquals("POINT (0 0)", wkt.sanitize("POINT (0 0 0)"));
    assertEquals("POINT (0 0)", wkt.sanitize("POINT (0 0 0 0)"));
    assertEquals("POINT (0 0)", wkt.sanitize("POINT Z (0 0 0)"));
    assertEquals("POINT (0 0)", wkt.sanitize("POINT M (0 0 0)"));
    assertEquals("POINT (0 0)", wkt.sanitize("POINT ZM (0 0 0 0)"));
  }

  @Test
  public void lineStrings() {
    WktSanitizer wkt = new WktSanitizer();

    assertEquals("LINESTRING (0 0, 1 1)", wkt.sanitize("LINESTRING (0 0, 1 1)"));
    assertEquals("LINESTRING (0 0, 1 1)", wkt.sanitize("LINESTRING (0 0 0, 1 1 1)"));
    assertEquals("LINESTRING (0 0, 1 1)", wkt.sanitize("LINESTRING (0 0 0 0, 1 1 1 1)"));
    assertEquals("LINESTRING (0 0, 1 1)", wkt.sanitize("LINESTRING Z (0 0 0, 1 1 1)"));
    assertEquals("LINESTRING (0 0, 1 1)", wkt.sanitize("LINESTRING M (0 0 0, 1 1 1)"));
    assertEquals("LINESTRING (0 0, 1 1)", wkt.sanitize("LINESTRING ZM (0 0 0 0, 1 1 1 1)"));
  }

  @Test
  public void polygons() {
    WktSanitizer wkt = new WktSanitizer();

    assertEquals("POLYGON ((0 0, 1 1, 2 2, 0 0))", wkt.sanitize("POLYGON ((0 0, 1 1, 2 2, 0 0))"));
    assertEquals("POLYGON ((0 0, 1 1, 2 2, 0 0))", wkt.sanitize("POLYGON ((0 0 0, 1 1 1, 2 2 2, 0 0 0))"));
    assertEquals("POLYGON ((0 0, 1 1, 2 2, 0 0))", wkt.sanitize("POLYGON ((0 0 0 0, 1 1 1 1, 2 2 2 2, 0 0 0 0))"));
    assertEquals("POLYGON ((0 0, 1 1, 2 2, 0 0))", wkt.sanitize("POLYGON Z ((0 0 0, 1 1 1, 2 2 2, 0 0 0))"));
    assertEquals("POLYGON ((0 0, 1 1, 2 2, 0 0))", wkt.sanitize("POLYGON M ((0 0 0, 1 1 1, 2 2 2, 0 0 0))"));
    assertEquals("POLYGON ((0 0, 1 1, 2 2, 0 0))", wkt.sanitize("POLYGON ZM ((0 0 0 0, 1 1 1 1, 2 2 2 2, 0 0 0 0))"));
  }

  @Test
  public void integration() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.executeIndex(new SpatialIndex());

      try (IDocumentSession session = store.openSession()) {
        session.store(new SpatialDoc("POINT (50.8 50.8)"));
        session.store(new SpatialDoc("POINT (50.8 50.8 50.8)"));
        session.store(new SpatialDoc("POINT (50.8 50.8 50.8 50.8)"));
        session.store(new SpatialDoc("POINT Z (50.8 50.8 50.8)"));
        session.store(new SpatialDoc("POINT M (50.8 50.8 50.8)"));
        session.store(new SpatialDoc("POINT ZM (50.8 50.8 50.8 50.8)"));
        session.saveChanges();
      }

      waitForNonStaleIndexes(store.getDatabaseCommands());

      try (IDocumentSession session  = store.openSession()) {
        int matches = session.query(RavenJObject.class, SpatialIndex.class)
          .customize(new DocumentQueryCustomizationFactory().withinRadiusOf("Wkt", 150, 50.8, 50.8))
          .count();
        assertEquals(6, matches);
      }

    }

  }

  @QueryEntity
  public static class SpatialDoc {
    private String id;
    private String wkt;

    public SpatialDoc() {
      super();
    }

    public SpatialDoc(String wkt) {
      super();
      this.wkt = wkt;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getWkt() {
      return wkt;
    }

    public void setWkt(String wkt) {
      this.wkt = wkt;
    }

  }

  public static class SpatialIndex extends AbstractIndexCreationTask {
    public SpatialIndex() {
      QWktSanitizerTest_SpatialDoc x = QWktSanitizerTest_SpatialDoc.spatialDoc;
      map = "from doc in docs.SpatialDocs select new { doc.Wkt}";
      spatial(x.wkt, new SpatialOptionsFactory().getGeography().defaultOptions());
    }
  }

}

package net.ravendb.tests.spatial;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.indexing.FieldStorage;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RavenQueryStatistics;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.tests.spatial.QSpatialTest_MyProjection;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;



public class SpatialTest extends RemoteClientTest {

  public static class MyDocumentItem {
    private Date date;
    private Double latitude;
    private Double longitude;

    public Date getDate() {
      return date;
    }

    public void setDate(Date date) {
      this.date = date;
    }

    public Double getLatitude() {
      return latitude;
    }

    public void setLatitude(Double latitude) {
      this.latitude = latitude;
    }

    public Double getLongitude() {
      return longitude;
    }

    public void setLongitude(Double longitude) {
      this.longitude = longitude;
    }
  }

  @QueryEntity
  public static class MyDocument {
    private String id;
    private List<MyDocumentItem> items;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public List<MyDocumentItem> getItems() {
      return items;
    }

    public void setItems(List<MyDocumentItem> items) {
      this.items = items;
    }
  }

  @QueryEntity
  public static class MyProjection {
    private String id;
    private Date date;
    private double latitude;
    private double longitude;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public Date getDate() {
      return date;
    }

    public void setDate(Date date) {
      this.date = date;
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

  public static class MyIndex extends AbstractIndexCreationTask {
    public MyIndex() {
      map = "from doc in docs.MyDocuments "
        + " from item in doc.Items "
        + " let lat = item.Latitude ?? 0 "
        + " let lng = item.Longitude ?? 0 "
        + " select new  { "
        + "  Id = doc.Id, "
        + "  Date = item.Date, "
        + "  Latitude = lat,"
        + "  Longitude = lng,"
        + "  _ = SpatialGenerate(lat, lng)"
        + " }; ";

      QSpatialTest_MyProjection x = QSpatialTest_MyProjection.myProjection;
      store(x.id, FieldStorage.YES);
      store(x.date, FieldStorage.YES);

      store(x.latitude, FieldStorage.YES);
      store(x.longitude, FieldStorage.YES);
    }
  }

  @Test
  public void weirdSpatialResults() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {

        MyDocument myDoc = new MyDocument();
        myDoc.setId("First");

        MyDocumentItem docItem = new MyDocumentItem();
        docItem.setDate(mkDate(2011, 1, 1));
        docItem.setLatitude(10.0);
        docItem.setLongitude(10.0);

        myDoc.setItems(Arrays.asList(docItem));
        session.store(myDoc);
        session.saveChanges();
      }

      new MyIndex().execute(store);

      try (IDocumentSession session = store.openSession()) {
        Reference<RavenQueryStatistics> statsRef = new Reference<>();
        List<MyProjection> result = session.advanced()
          .luceneQuery(MyDocument.class, MyIndex.class)
          .waitForNonStaleResults()
          .withinRadiusOf(0, 12.3456789f, 12.3456789f)
          .statistics(statsRef)
          .selectFields(MyProjection.class, "Id", "Latitude", "Longitude")
          .take(50)
          .toList();

        assertEquals(0, statsRef.value.getTotalResults());
        assertEquals(0, result.size());
      }
    }
  }

  @Test
  public void matchSpatialResults() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {

        MyDocument myDoc = new MyDocument();
        myDoc.setId("First");

        MyDocumentItem docItem = new MyDocumentItem();
        docItem.setDate(mkDate(2011, 1, 1));
        docItem.setLatitude(10.0);
        docItem.setLongitude(10.0);

        myDoc.setItems(Arrays.asList(docItem));
        session.store(myDoc);
        session.saveChanges();
      }

      new MyIndex().execute(store);

      try (IDocumentSession session = store.openSession()) {
        Reference<RavenQueryStatistics> statsRef = new Reference<>();
        List<MyProjection> result = session.advanced()
          .luceneQuery(MyDocument.class, MyIndex.class)
          .waitForNonStaleResults()
          .withinRadiusOf(1, 10, 10)
          .statistics(statsRef)
          .selectFields(MyProjection.class, "Id", "Latitude", "Longitude")
          .take(50)
          .toList();

        assertEquals(1, statsRef.value.getTotalResults());
        assertEquals(1, result.size());
      }
    }
  }

  public static class MySpatialIndex extends AbstractIndexCreationTask {
    public MySpatialIndex() {
      map = "from doc in docs.MySpatialDocuments select new { _ = SpatialGenerate(doc.Latitude, doc.Longitude) };";
    }
  }

  public static class MySpatialDocument {
    private double latitude;
    private double longitude;

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

  @Test
  public void weirdSpatialResults2() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        MySpatialDocument spatialDoc = new MySpatialDocument();
        spatialDoc.setLatitude(12.3456789f);
        spatialDoc.setLongitude(12.3456789f);
        session.store(spatialDoc);
        session.saveChanges();
      }

      new MySpatialIndex().execute(store);

      try (IDocumentSession session = store.openSession()) {
        Reference<RavenQueryStatistics> statsRef = new Reference<>();
        List<MySpatialDocument> result = session.advanced()
          .luceneQuery(MySpatialDocument.class, MySpatialIndex.class)
          .waitForNonStaleResults()
          .withinRadiusOf(200, 12.3456789f, 12.3456789f)
          .statistics(statsRef)
          .take(50)
          .toList();

        assertEquals(1, statsRef.value.getTotalResults());
        assertEquals(1, result.size());
      }
    }
  }

}

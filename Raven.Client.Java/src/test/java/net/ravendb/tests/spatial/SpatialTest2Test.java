package net.ravendb.tests.spatial;

import static org.junit.Assert.assertEquals;

import java.util.List;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;

import org.junit.Test;



public class SpatialTest2Test extends RemoteClientTest {
  public static class Entity {
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

  public static class EntitiesByLocation extends AbstractIndexCreationTask {
    public EntitiesByLocation() {
      map = " from entity in docs.Entities select new { _ = SpatialGenerate(entity.Latitude, entity.Longitude) } ";
    }
  }

  @Test
  public void weirdSpatialResults() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Entity entity = new Entity();
        entity.setLatitude(45.829507799999988);
        entity.setLongitude(-73.800524699999983);

        session.store(entity);
        session.saveChanges();
      }

      new EntitiesByLocation().execute(store);

      try (IDocumentSession session = store.openSession()) {
        session.query(Entity.class, EntitiesByLocation.class)
        .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
        .toList();

        // Let's search within a 150km radius
        List<Entity> results = session.advanced().documentQuery(Entity.class, EntitiesByLocation.class)
          .withinRadiusOf(150000 * 0.000621, 45.831909, -73.810322)
          // This is less than 1km from the entity
          .sortByDistance()
          .toList();

        // This works
        assertEquals(1, results.size());

        // Let's search within a 15km radius
        results = session.advanced().documentQuery(Entity.class, EntitiesByLocation.class)
          .withinRadiusOf(15000 * 0.000621, 45.831909, -73.810322)
          // This is less than 1km from the entity
          .sortByDistance()
          .toList();

        // This fails
        assertEquals(1, results.size());

        // Let's search within a 1.5km radius
        results = session.advanced().documentQuery(Entity.class, EntitiesByLocation.class)
          .withinRadiusOf(1500 * 0.000621, 45.831909, -73.810322)
          // This is less than 1km from the entity
          .sortByDistance()
          .toList();

        // This fails
        assertEquals(1, results.size());


      }
    }
  }

}

package net.ravendb.tests.spatial;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.data.Facet;
import net.ravendb.abstractions.data.FacetResults;
import net.ravendb.abstractions.data.FacetSetup;
import net.ravendb.abstractions.data.SpatialIndexQuery;
import net.ravendb.abstractions.indexing.SpatialOptions.SpatialRelation;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RavenQueryStatistics;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.tests.spatial.QAfifTest_Vehicle;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;



public class AfifTest extends RemoteClientTest {

  public static class ByVehicle extends AbstractIndexCreationTask {
    public ByVehicle() {
      map = "from vehicle in docs.vehicles select new { vehicle.Model, vehicle.Make, _ = SpatialGenerate(vehicle.Latitude, vehicle.Longitude) };";
    }
  }

  @QueryEntity
  public static class Vehicle {
    private String id;
    private String model;
    private String make;
    private double latitude;
    private double longitude;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getModel() {
      return model;
    }

    public void setModel(String model) {
      this.model = model;
    }

    public String getMake() {
      return make;
    }

    public void setMake(String make) {
      this.make = make;
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

  public static class HawthornEast extends Location  {
    public HawthornEast() {
      super(145.052097, -37.834855);
    }
  }

  public static class Darwin extends Location  {
    public Darwin() {
      super(130.841904,  12.461334);
    }
  }


  public static class Location {
    private double longitude;
    private double latitude;

    public double getLongitude() {
      return longitude;
    }

    public double getLatitude() {
      return latitude;
    }

    public Location(double longitude, double latitude) {
      this.longitude = longitude;
      this.latitude = latitude;
    }
  }

  @Test
  public void shouldMatchMakeFacetsOnLocation() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      usingEmbeddedRavenStoreWithVehicles(store);

      FacetResults facetValues;

      try (IDocumentSession session = store.openSession()) {
        String index = ByVehicle.class.getSimpleName();

        SpatialIndexQuery indexQuery = new SpatialIndexQuery();
        indexQuery.setQueryShape(SpatialIndexQuery.getQueryShapeFromLatLon(new Darwin().getLatitude(), new Darwin().getLongitude(), 5));
        indexQuery.setSpatialRelation(SpatialRelation.WITHIN);
        indexQuery.setSpatialFieldName(Constants.DEFAULT_SPATIAL_FIELD_NAME);

        facetValues = store.getDatabaseCommands().getFacets(index, indexQuery, "facets/Vehicle");

        assertNotNull(facetValues);
        assertEquals(2, facetValues.getResults().get("Make").getValues().size());
      }
    }
  }

  public void usingEmbeddedRavenStoreWithVehicles(IDocumentStore store) throws Exception {
    List<Vehicle> vehicles = new ArrayList<>();

    for (int i = 0; i< 3; i++) {
      Vehicle vehicle = new Vehicle();
      vehicle.setMake("Mazda");
      vehicle.setModel("Rx8");
      vehicle.setLatitude(new Darwin().getLatitude());
      vehicle.setLongitude(new Darwin().getLongitude());
      vehicles.add(vehicle);
    }

    for (int i = 0; i< 4; i++) {
      Vehicle vehicle = new Vehicle();
      vehicle.setMake("Mercedes");
      vehicle.setModel("AMG");
      vehicle.setLatitude(new Darwin().getLatitude());
      vehicle.setLongitude(new Darwin().getLongitude());
      vehicles.add(vehicle);
    }

    for (int i = 0; i< 4; i++) {
      Vehicle vehicle = new Vehicle();
      vehicle.setMake("Toyota");
      vehicle.setModel("Camry");
      vehicle.setLatitude(new HawthornEast().getLatitude());
      vehicle.setLongitude(new HawthornEast().getLongitude());
      vehicles.add(vehicle);
    }

    try (IDocumentSession session = store.openSession()) {

      FacetSetup facetSetup = new FacetSetup();
      facetSetup.setId("facets/Vehicle");
      Facet facet1 = new Facet();
      facet1.setName("Make");
      Facet facet2 = new Facet();
      facet2.setName("Model");
      facetSetup.setFacets(Arrays.asList(facet1, facet2));

      session.store(facetSetup);

      new ByVehicle().execute(session.advanced().getDocumentStore());
      session.saveChanges();
      for (Vehicle vehicle : vehicles) {
        session.store(vehicle);
      }
      session.saveChanges();

      Reference<RavenQueryStatistics> statsRef = new Reference<>();
      QAfifTest_Vehicle x= QAfifTest_Vehicle.vehicle;
      session.query(Vehicle.class, ByVehicle.class).where(x.make.eq("Mazda"))
        .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
        .statistics(statsRef).toList();

    }
  }

}

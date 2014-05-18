package net.ravendb.tests.faceted;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import net.ravendb.abstractions.data.Facet;
import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.tests.faceted.QCamera;


public abstract class FacetTestBase extends RemoteClientTest {
  public static void createCameraCostIndex(IDocumentStore store) {
    CameraCostIndex index = new CameraCostIndex();
    store.getDatabaseCommands().putIndex(index.getIndexName(), index.createIndexDefinition());
  }

  public static class CameraCostIndex extends AbstractIndexCreationTask {

    @Override
    public IndexDefinition createIndexDefinition() {
      IndexDefinition def = new IndexDefinition();
      def.setMap("from camera in docs " +
                 "       select new  " +
                 "       {  " +
                 "           camera.Manufacturer, " +
                 "           camera.Model,  " +
                 "           camera.Cost, " +
                 "           camera.DateOfListing, " +
                 "           camera.Megapixels " +
                 "       }");
      def.setName("CameraCost");
      return def;
    }

    @Override
    public String getIndexName() {
      return new CameraCostIndex().createIndexDefinition().getName();
    }

  }

  public static void insertCameraDataAndWaitForNonStaleResults(IDocumentStore store, List<Camera> cameras) throws Exception {
    try (IDocumentSession session = store.openSession()) {
      for (Camera camera: cameras) {
        session.store(camera);
      }
      session.saveChanges();

      session.query(Camera.class, new CameraCostIndex().getIndexName()).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults()).toList();
    }
  }

  public static List<Facet> getFacets() {
    QCamera c = QCamera.camera;

    List<Facet> facets = new ArrayList<>();
    Facet f1 = new Facet();
    f1.setName(c.manufacturer);
    facets.add(f1);

    Facet f2 = new Facet();
    f2.setName(c.cost);
    f2.setRanges(c.cost.lt(200),
        c.cost.gt(200).and(c.cost.lt(400)),
        c.cost.gt(400).and(c.cost.lt(600)),
        c.cost.gt(600).and(c.cost.lt(800)),
        c.cost.gt(800));
    facets.add(f2);

    Facet f3 = new Facet();
    f3.setName(c.megapixels);
    f3.setRanges(c.megapixels.lt(3),
        c.megapixels.gt(3).and(c.megapixels.lt(7)),
        c.megapixels.gt(7).and(c.megapixels.lt(10)),
        c.megapixels.gt(10));
    facets.add(f3);

    return facets;

  }

  private static final List<String> FEATURES = new ArrayList<>();
  static {
    FEATURES.add("Image Stabilizer");
    FEATURES.add("Tripod");
    FEATURES.add("Low Light Compatible");
    FEATURES.add("Fixed Lens");
    FEATURES.add("LCD");
  }

  private static final List<String> MANUFACTURERS = new ArrayList<>();
  static {
    MANUFACTURERS.add("Sony");
    MANUFACTURERS.add("Nikon");
    MANUFACTURERS.add("Phillips");
    MANUFACTURERS.add("Canon");
    MANUFACTURERS.add("Jessops");
  }

  private static final List<String> MODELS  =new ArrayList<>();
  static {
    MODELS.add("Model1");
    MODELS.add("Model2");
    MODELS.add("Model3");
    MODELS.add("Model4");
    MODELS.add("Model5");
  }

  private static final Random random = new Random(1337);

  public static List<Camera> getCameras(int numCameras) {
    List<Camera> cameraList = new ArrayList<>(numCameras);
    for (int i = 1; i <= numCameras; i++) {
      Camera camera = new Camera();
      camera.setId(i);

      Calendar cal = Calendar.getInstance();
      cal.set(1980 + random.nextInt(30), random.nextInt(12), random.nextInt(27));
      camera.setDateOfListing(cal.getTime());
      camera.setManufacturer(MANUFACTURERS.get(random.nextInt(MANUFACTURERS.size())));
      camera.setModel(MODELS.get(random.nextInt(MODELS.size())));
      camera.setCost(random.nextDouble() * 900.0 + 100.0);
      camera.setZoom((int) (random.nextDouble() * 10.0 + 1.0));
      camera.setMegapixels(random.nextDouble() * 10.0 + 1.0);
      camera.setImageStabilizer(random.nextDouble() > 0.6);
      camera.setAdvancedFeatures(Arrays.asList("??"));
      cameraList.add(camera);

    }
    return cameraList;
  }
}

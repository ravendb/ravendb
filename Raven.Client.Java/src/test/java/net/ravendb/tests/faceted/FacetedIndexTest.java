package net.ravendb.tests.faceted;

import static com.mysema.query.collections.CollQueryFactory.from;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.ravendb.abstractions.basic.Lazy;
import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.Facet;
import net.ravendb.abstractions.data.FacetMode;
import net.ravendb.abstractions.data.FacetResult;
import net.ravendb.abstractions.data.FacetResults;
import net.ravendb.abstractions.data.FacetSetup;
import net.ravendb.abstractions.data.FacetValue;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.faceted.QCamera;

import org.apache.http.HttpStatus;
import org.junit.Test;

import com.mysema.query.types.Predicate;


public class FacetedIndexTest extends FacetTestBase {


  private final List<Camera> data;
  private final List<Facet> originalFacets;
  private final List<Facet> stronglyTypedFacets;
  private static final int numCameras = 1000;

  public FacetedIndexTest() {
    data = getCameras(numCameras);

    Facet oldFacet1 = new Facet();
    oldFacet1.setName("Manufacturer");

    Facet oldFacet2 = new Facet();
    oldFacet2.setName("Cost_Range");
    oldFacet2.setMode(FacetMode.RANGES);
    oldFacet2.setRanges(Arrays.asList("[NULL TO Dx200]",
        "[Dx200 TO Dx400]",
        "[Dx400 TO Dx600]",
        "[Dx600 TO Dx800]",
        "[Dx800 TO NULL]"));

    Facet oldFacet3 = new Facet();
    oldFacet3.setName("Megapixels_Range");
    oldFacet3.setMode(FacetMode.RANGES);
    oldFacet3.setRanges(Arrays.asList("[NULL TO Dx3]",
        "[Dx3 TO Dx7]",
        "[Dx7 TO Dx10]",
        "[Dx10 TO NULL]"));

    originalFacets = Arrays.asList(oldFacet1, oldFacet2, oldFacet3);
    stronglyTypedFacets = getFacets();
  }

  @Test
  public void canPerformFacetedSearch_Remotely() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      executeTest(store, originalFacets);
    }
  }

  @Test
  public void remoteFacetedSearchHonorsConditionalGet() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store, stronglyTypedFacets);

      ConditionalGetHelper conditionalGetHelper = new ConditionalGetHelper();
      Reference<Etag> firstEtagRef = new Reference<>();

      String queryUrl = store.getUrl() + "/databases/" + getDefaultDb() + "/facets/CameraCost?facetDoc=facets%%2FCameraFacets&query=Manufacturer%%253A%s&facetStart=0&facetPageSize=";
      String url = String.format(queryUrl, "canon");

      assertEquals(HttpStatus.SC_OK, conditionalGetHelper.performGet(url, null, firstEtagRef));

      //second request should give 304 not modified
      assertEquals(HttpStatus.SC_NOT_MODIFIED, conditionalGetHelper.performGet(url, firstEtagRef.value, firstEtagRef));

      //change index etag by inserting new doc
      insertCameraDataAndWaitForNonStaleResults(store, getCameras(1));

      Reference<Etag> secondEtagRef = new Reference<>();
      //changing the index should give 200 OK
      assertEquals(HttpStatus.SC_OK, conditionalGetHelper.performGet(url, firstEtagRef.value, secondEtagRef));

      //next request should give 304 not modified
      assertEquals(HttpStatus.SC_NOT_MODIFIED, conditionalGetHelper.performGet(url, secondEtagRef.value, secondEtagRef));
    }
  }

  @Test
  public void canPerformFacetedSearch_Remotely_WithStronglyTypedAPI() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      executeTest(store, stronglyTypedFacets);
    }
  }

  @Test
  public void canPerformFacetedSearch_Remotely_Lazy() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store, originalFacets);
      try (IDocumentSession session = store.openSession()) {

        QCamera c = QCamera.camera;

        List<Predicate> expressions = new ArrayList<>();
        expressions.add(c.cost.goe(100).and(c.cost.loe(300)));
        Calendar cal = Calendar.getInstance();
        cal.set(2000, 0, 1);

        expressions.add(c.dateOfListing.gt(cal.getTime()));
        expressions.add(c.megapixels.gt(5).and(c.cost.lt(500)));

        for (Predicate exp: expressions) {
          int oldRequests = session.advanced().getNumberOfRequests();

          Lazy<FacetResults> facetResults = session.query(Camera.class, "CameraCost").where(exp).toFacetsLazy("facets/CameraFacets");
          assertEquals(oldRequests, session.advanced().getNumberOfRequests());
          List<Camera> filteredData = from(c, data).where(exp).list(c);
          checkFacetResultsMatchInMemoryData(facetResults.getValue(), filteredData);
          assertEquals(oldRequests + 1, session.advanced().getNumberOfRequests());
        }

      }
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void canPerformFacetedSearch_Remotely_Lazy_can_work_with_others() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store, originalFacets);
      try (IDocumentSession session = store.openSession()) {
        QCamera c = QCamera.camera;

        List<Predicate> expressions = new ArrayList<>();
        expressions.add(c.cost.goe(100).and(c.cost.loe(300)));
        Calendar cal = Calendar.getInstance();
        cal.set(2000, 0, 1);

        expressions.add(c.dateOfListing.gt(cal.getTime()));
        expressions.add(c.megapixels.gt(5).and(c.cost.lt(500)));
        for (Predicate exp: expressions) {
          int oldRequests = session.advanced().getNumberOfRequests();
          Lazy<Camera> load = session.advanced().lazily().load(Camera.class, oldRequests);
          Lazy<FacetResults> facetResults = session.query(Camera.class, "CameraCost").where(exp).toFacetsLazy("facets/CameraFacets");
          assertEquals(oldRequests, session.advanced().getNumberOfRequests());

          List<Camera> filteredData = from(c, data).where(exp).list(c);
          checkFacetResultsMatchInMemoryData(facetResults.getValue(), filteredData);
          Camera forceLoading = load.getValue();
          assertEquals(oldRequests + 1, session.advanced().getNumberOfRequests());

        }

      }
    }
  }

  private void executeTest(IDocumentStore store, List<Facet> facetsToUse) throws Exception {
    setup(store, facetsToUse);

    try (IDocumentSession s = store.openSession()) {
      QCamera c = QCamera.camera;

      List<Predicate> expressions = new ArrayList<>();
      expressions.add(c.cost.goe(100).and(c.cost.loe(300)));
      Calendar cal = Calendar.getInstance();
      cal.set(2000, 0, 1);

      expressions.add(c.dateOfListing.gt(cal.getTime()));
      expressions.add(c.megapixels.gt(5).and(c.cost.lt(500)));
      expressions.add(c.manufacturer.eq("abc&edf"));


      for (Predicate exp: expressions) {
        FacetResults facetResults = s.query(Camera.class, "CameraCost").where(exp).toFacets("facets/CameraFacets");
        List<Camera> filteredData = from(c, data).where(exp).list(c);
        checkFacetResultsMatchInMemoryData(facetResults, filteredData);
      }
    }
  }

  private void setup(IDocumentStore store, List<Facet> facetsToUse) throws Exception {
    try (IDocumentSession s = store.openSession()) {

      FacetSetup facetSetupDoc = new FacetSetup("facets/CameraFacets", facetsToUse);
      s.store(facetSetupDoc);
      s.saveChanges();
    }
    createCameraCostIndex(store);
    insertCameraDataAndWaitForNonStaleResults(store, data);
  }

  @SuppressWarnings("unused")
  private void printFacetResults(FacetResults facetResults) {
    for(Map.Entry<String, FacetResult> kvp : facetResults.getResults().entrySet()) {
      if (kvp.getValue().getValues().size() > 0) {
        System.out.println(kvp.getKey() + ":");
        for (FacetValue facet : kvp.getValue().getValues()) {
          System.out.println(String.format("    %s: %d", facet.getRange(), facet.getHits()));
        }
        System.out.println();
      }
    }
  }

  private void checkFacetResultsMatchInMemoryData(FacetResults facetResults, List<Camera> filteredData) {
    QCamera c = QCamera.camera;

    long expectedCount = from(c, filteredData).distinct().list(c.manufacturer).size();
    int count = facetResults.getResults().get("Manufacturer").getValues().size();
    assertEquals(expectedCount, count);

    for (FacetValue facet : facetResults.getResults().get("Manufacturer").getValues()) {
      long inMemoryCount = from(c, filteredData).where(c.manufacturer.lower().eq(facet.getRange())).count();
      assertEquals(inMemoryCount, facet.getHits());
    }

    //Go through the expected (in-memory) results and check that there is a corresponding facet result
    //Not the prettiest of code, but it works!!!
    List<FacetValue> costFacets = facetResults.getResults().get("Cost_Range").getValues();
    Map<String, FacetValue> costLookup = new HashMap<>();
    for (FacetValue costFacet : costFacets) {
      costLookup.put(costFacet.getRange(), costFacet);
    }
    checkFacetCount(from(c, filteredData).where(c.cost.loe(200)).count(), costLookup.get("[NULL TO Dx200]"));
    checkFacetCount(from(c, filteredData).where(c.cost.goe(200).and(c.cost.loe(400))).count(), costLookup.get("[Dx200 TO Dx400]"));
    checkFacetCount(from(c, filteredData).where(c.cost.goe(400).and(c.cost.loe(600))).count(), costLookup.get("[Dx400 TO Dx600]"));
    checkFacetCount(from(c, filteredData).where(c.cost.goe(600).and(c.cost.loe(800))).count(), costLookup.get("[Dx600 TO Dx800]"));
    checkFacetCount(from(c, filteredData).where(c.cost.goe(800)).count(), costLookup.get("[Dx800 TO NULL]"));

    //Test the Megapixels_Range facets using the same method
    List<FacetValue> megapixelsFacets = facetResults.getResults().get("Megapixels_Range").getValues();
    Map<String, FacetValue> megapixelsLookup = new HashMap<>();
    for (FacetValue megapixelFacet: megapixelsFacets) {
      megapixelsLookup.put(megapixelFacet.getRange(), megapixelFacet);
    }
    checkFacetCount(from(c, filteredData).where(c.megapixels.loe(3)).count(), megapixelsLookup.get("[NULL TO Dx3]"));
    checkFacetCount(from(c, filteredData).where(c.megapixels.goe(3).and(c.megapixels.loe(7))).count(), megapixelsLookup.get("[Dx3 TO Dx7]"));
    checkFacetCount(from(c, filteredData).where(c.megapixels.goe(7).and(c.megapixels.loe(10))).count(), megapixelsLookup.get("[Dx7 TO Dx10]"));
    checkFacetCount(from(c, filteredData).where(c.megapixels.goe(10)).count(), megapixelsLookup.get("[Dx10 TO NULL]"));

  }


  private void checkFacetCount(long expectedCount, FacetValue facets) {
    if (expectedCount > 0) {
      assertNotNull(facets);
      assertEquals(expectedCount, facets.getHits());
    }
  }

}

package net.ravendb.tests.faceted;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import static com.mysema.query.collections.CollQueryFactory.from;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.Facet;
import net.ravendb.abstractions.data.FacetResults;
import net.ravendb.abstractions.data.FacetValue;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.imports.json.JsonConvert;
import net.ravendb.tests.faceted.QCamera;

import org.apache.http.HttpStatus;
import org.junit.Test;


import com.mysema.query.types.Predicate;

public class DynamicFacetsTest extends FacetTestBase {

  @Test
  public void canPerformDynamicFacetedSearch_Remotely() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      List<Camera> cameras = getCameras(30);
      createCameraCostIndex(store);
      insertCameraDataAndWaitForNonStaleResults(store, cameras);
      List<Facet> facets = getFacets();

      QCamera c = QCamera.camera;

      List<Predicate> expressions = new ArrayList<>();
      expressions.add(c.cost.goe(100).and(c.cost.loe(300)));
      Calendar cal = Calendar.getInstance();
      cal.set(2000, 0, 1);

      expressions.add(c.dateOfListing.gt(cal.getTime()));
      expressions.add(c.megapixels.gt(5).and(c.cost.lt(500)));
      expressions.add(c.manufacturer.eq("abc&edf"));

      try (IDocumentSession s = store.openSession()) {

        for (Predicate exp: expressions) {
          FacetResults facetResults = s.query(Camera.class, CameraCostIndex.class).where(exp).toFacets(facets);
          List<Camera> filteredData = from(c, cameras).where(exp).list(c);
          checkFacetResultsMatchInMemoryData(facetResults, filteredData);
        }
      }
    }
  }

  @Test
  public void remoteDynamicFacetedSearchHonorsConditionalGet() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      createCameraCostIndex(store);
      insertCameraDataAndWaitForNonStaleResults(store, getCameras(1));

      ConditionalGetHelper conditionalGetHelper = new ConditionalGetHelper();

      List<Facet> facets = getFacets();

      String jsonFacets = JsonConvert.serializeObject(facets);
      Reference<Etag> firstEtagRef = new Reference<>();

      String queryUrl = store.getUrl() + "/databases/" + getDefaultDb() + "/facets/CameraCost?query=Manufacturer%%253A%s&facetStart=0&facetPageSize=";
      String requestUrl = String.format(queryUrl, "canon");

      assertEquals(HttpStatus.SC_OK, conditionalGetHelper.performPost(requestUrl, jsonFacets, null, firstEtagRef));

      //second request should give 304 not modified
      assertEquals(HttpStatus.SC_NOT_MODIFIED, conditionalGetHelper.performPost(requestUrl, jsonFacets, firstEtagRef.value, firstEtagRef));

    //change index etag by inserting new doc
      insertCameraDataAndWaitForNonStaleResults(store, getCameras(1));

      Reference<Etag> secondEtagRef = new Reference<>();
      //changing the index should give 200 OK
      assertEquals(HttpStatus.SC_OK, conditionalGetHelper.performPost(requestUrl, jsonFacets, firstEtagRef.value, secondEtagRef));

      //next request should give 304 not modified
      assertEquals(HttpStatus.SC_NOT_MODIFIED, conditionalGetHelper.performPost(requestUrl, jsonFacets, secondEtagRef.value, secondEtagRef));

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

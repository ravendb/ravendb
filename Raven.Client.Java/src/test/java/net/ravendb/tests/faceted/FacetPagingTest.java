package net.ravendb.tests.faceted;

import static com.mysema.query.collections.CollQueryFactory.from;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.ravendb.abstractions.data.Facet;
import net.ravendb.abstractions.data.FacetResults;
import net.ravendb.abstractions.data.FacetSetup;
import net.ravendb.abstractions.data.FacetTermSortMode;
import net.ravendb.abstractions.data.FacetValue;
import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.samples.entities.GroupResult;
import net.ravendb.samples.entities.QGroupResult;
import net.ravendb.tests.faceted.QCamera;

import org.junit.Test;


public class FacetPagingTest extends FacetTestBase {
  private final List<Camera> data;
  private final int numCameras = 1000;

  public FacetPagingTest() {
    data = getCameras(numCameras);
  }

  @Test
  public void canPerformFacetedPagingSearchWithNoPageSizeNoMaxResults_HitsDesc() throws Exception {
    //also specify more results than we have
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");
    facet1.setMaxResults(null);
    facet1.setTermSortMode(FacetTermSortMode.HITS_DESC);
    facet1.setIncludeRemainingTerms(true);
    List<Facet> facets = Arrays.asList(facet1);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);
      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        FacetResults facetResults = s.query(Camera.class, "CameraCost").toFacets("facets/CameraFacets", 2);

        QCamera x = QCamera.camera;

        Map<String, Integer> cameraCounts = new HashMap<>();
        for (Camera c: data) {
          if (!cameraCounts.containsKey(c.getManufacturer())) {
            cameraCounts.put(c.getManufacturer(), 0);
          }
          cameraCounts.put(c.getManufacturer(), cameraCounts.get(c.getManufacturer()) + 1);
        }

        QGroupResult g = QGroupResult.groupResult;

        List<String> camerasByHits = from(g, mapToTuple(cameraCounts))
            .orderBy(g.count.desc(), g.name.asc()).offset(2).list(g.name.toLowerCase());

        assertEquals(3, facetResults.getResults().get("Manufacturer").getValues().size());
        for (int i = 0; i < 3; i++) {
          assertEquals(camerasByHits.get(i), facetResults.getResults().get("Manufacturer").getValues().get(i).getRange());
        }
        for (FacetValue facet : facetResults.getResults().get("Manufacturer").getValues()) {
          long inMemoryCount = from(x, data).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(0, facetResults.getResults().get("Manufacturer").getRemainingTermsCount());
        assertEquals(0, facetResults.getResults().get("Manufacturer").getRemainingTerms().size());
        assertEquals(0, facetResults.getResults().get("Manufacturer").getRemainingHits());

      }
    }
  }

  @Test
  public void canPerformFacetedPagingSearchWithNoPageSizeWithMaxResults_HitsDesc() throws Exception {
    //also specify more results than we have
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");
    facet1.setMaxResults(2);
    facet1.setTermSortMode(FacetTermSortMode.HITS_DESC);
    facet1.setIncludeRemainingTerms(true);
    List<Facet> facets = Arrays.asList(facet1);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);
      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        FacetResults facetResults = s.query(Camera.class, "CameraCost").toFacets("facets/CameraFacets", 2);

        QCamera x = QCamera.camera;

        Map<String, Integer> cameraCounts = new HashMap<>();
        for (Camera c: data) {
          if (!cameraCounts.containsKey(c.getManufacturer())) {
            cameraCounts.put(c.getManufacturer(), 0);
          }
          cameraCounts.put(c.getManufacturer(), cameraCounts.get(c.getManufacturer()) + 1);
        }

        QGroupResult g = QGroupResult.groupResult;

        List<String> camerasByHits = from(g, mapToTuple(cameraCounts))
            .orderBy(g.count.desc(), g.name.asc()).offset(2).limit(2).list(g.name.toLowerCase());

        assertEquals(2, facetResults.getResults().get("Manufacturer").getValues().size());
        for (int i = 0; i < 2; i++) {
          assertEquals(camerasByHits.get(i), facetResults.getResults().get("Manufacturer").getValues().get(i).getRange());
        }
        for (FacetValue facet : facetResults.getResults().get("Manufacturer").getValues()) {
          long inMemoryCount = from(x, data).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTermsCount());
        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTerms().size());
        List<Integer> list = from(g, mapToTuple(cameraCounts))
            .orderBy(g.count.desc(), g.name.asc()).list(g.count);

        assertEquals(list.get(list.size()-1).intValue(), facetResults.getResults().get("Manufacturer").getRemainingHits());

      }
    }
  }

  @Test
  public void canPerformFacetedPagingSearchWithPageSize_HitsDesc() throws Exception {
    //also specify more results than we have
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");
    facet1.setMaxResults(3);
    facet1.setTermSortMode(FacetTermSortMode.HITS_DESC);
    facet1.setIncludeRemainingTerms(true);
    List<Facet> facets = Arrays.asList(facet1);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);
      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        FacetResults facetResults = s.query(Camera.class, "CameraCost").toFacets("facets/CameraFacets", 2, 2);

        QCamera x = QCamera.camera;

        Map<String, Integer> cameraCounts = new HashMap<>();
        for (Camera c: data) {
          if (!cameraCounts.containsKey(c.getManufacturer())) {
            cameraCounts.put(c.getManufacturer(), 0);
          }
          cameraCounts.put(c.getManufacturer(), cameraCounts.get(c.getManufacturer()) + 1);
        }

        QGroupResult g = QGroupResult.groupResult;

        List<String> camerasByHits = from(g, mapToTuple(cameraCounts))
            .orderBy(g.count.desc(), g.name.asc()).offset(2).limit(2).list(g.name.toLowerCase());

        assertEquals(2, facetResults.getResults().get("Manufacturer").getValues().size());
        for (int i = 0; i < 2; i++) {
          assertEquals(camerasByHits.get(i), facetResults.getResults().get("Manufacturer").getValues().get(i).getRange());
        }
        for (FacetValue facet : facetResults.getResults().get("Manufacturer").getValues()) {
          long inMemoryCount = from(x, data).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTermsCount());
        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTerms().size());
        List<Integer> list = from(g, mapToTuple(cameraCounts))
            .orderBy(g.count.desc(), g.name.asc()).list(g.count);

        assertEquals(list.get(list.size()-1).intValue(), facetResults.getResults().get("Manufacturer").getRemainingHits());

      }
    }
  }

  @Test
  public void canPerformFacetedPagingSearchWithPageSize_HitsAsc() throws Exception {
    //also specify more results than we have
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");
    facet1.setMaxResults(3);
    facet1.setTermSortMode(FacetTermSortMode.HITS_ASC);
    facet1.setIncludeRemainingTerms(true);
    List<Facet> facets = Arrays.asList(facet1);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);
      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        FacetResults facetResults = s.query(Camera.class, "CameraCost").toFacets("facets/CameraFacets", 2, 2);

        QCamera x = QCamera.camera;

        Map<String, Integer> cameraCounts = new HashMap<>();
        for (Camera c: data) {
          if (!cameraCounts.containsKey(c.getManufacturer())) {
            cameraCounts.put(c.getManufacturer(), 0);
          }
          cameraCounts.put(c.getManufacturer(), cameraCounts.get(c.getManufacturer()) + 1);
        }

        QGroupResult g = QGroupResult.groupResult;

        List<String> camerasByHits = from(g, mapToTuple(cameraCounts))
            .orderBy(g.count.asc(), g.name.asc()).offset(2).limit(2).list(g.name.toLowerCase());

        assertEquals(2, facetResults.getResults().get("Manufacturer").getValues().size());
        for (int i = 0; i < 2; i++) {
          assertEquals(camerasByHits.get(i), facetResults.getResults().get("Manufacturer").getValues().get(i).getRange());
        }
        for (FacetValue facet : facetResults.getResults().get("Manufacturer").getValues()) {
          long inMemoryCount = from(x, data).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTermsCount());
        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTerms().size());
        List<Integer> list = from(g, mapToTuple(cameraCounts))
            .orderBy(g.count.asc(), g.name.asc()).list(g.count);

        assertEquals(list.get(list.size()-1).intValue(), facetResults.getResults().get("Manufacturer").getRemainingHits());

      }
    }
  }

  @Test
  public void canPerformFacetedPagingSearchWithPageSize_TermDesc() throws Exception {
    //also specify more results than we have
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");
    facet1.setMaxResults(3);
    facet1.setTermSortMode(FacetTermSortMode.VALUE_DESC);
    facet1.setIncludeRemainingTerms(true);
    List<Facet> facets = Arrays.asList(facet1);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);
      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        FacetResults facetResults = s.query(Camera.class, "CameraCost").toFacets("facets/CameraFacets", 2, 2);

        QCamera x = QCamera.camera;

        Map<String, Integer> cameraCounts = new HashMap<>();
        for (Camera c: data) {
          if (!cameraCounts.containsKey(c.getManufacturer())) {
            cameraCounts.put(c.getManufacturer(), 0);
          }
          cameraCounts.put(c.getManufacturer(), cameraCounts.get(c.getManufacturer()) + 1);
        }

        QGroupResult g = QGroupResult.groupResult;

        List<String> camerasByHits = from(g, mapToTuple(cameraCounts))
            .orderBy(g.name.toLowerCase().desc()).offset(2).limit(2).list(g.name.toLowerCase());

        assertEquals(2, facetResults.getResults().get("Manufacturer").getValues().size());
        for (int i = 0; i < 2; i++) {
          assertEquals(camerasByHits.get(i), facetResults.getResults().get("Manufacturer").getValues().get(i).getRange());
        }
        for (FacetValue facet : facetResults.getResults().get("Manufacturer").getValues()) {
          long inMemoryCount = from(x, data).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTermsCount());
        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTerms().size());
        List<Integer> list = from(g, mapToTuple(cameraCounts))
            .orderBy(g.name.toLowerCase().desc()).list(g.count);

        assertEquals(list.get(list.size()-1).intValue(), facetResults.getResults().get("Manufacturer").getRemainingHits());

      }
    }
  }

  @Test
  public void canPerformFacetedPagingSearchWithPageSize_TermAsc() throws Exception {
    //also specify more results than we have
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");
    facet1.setMaxResults(3);
    facet1.setTermSortMode(FacetTermSortMode.VALUE_ASC);
    facet1.setIncludeRemainingTerms(true);
    List<Facet> facets = Arrays.asList(facet1);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);
      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        FacetResults facetResults = s.query(Camera.class, "CameraCost").toFacets("facets/CameraFacets", 2, 2);

        QCamera x = QCamera.camera;

        Map<String, Integer> cameraCounts = new HashMap<>();
        for (Camera c: data) {
          if (!cameraCounts.containsKey(c.getManufacturer())) {
            cameraCounts.put(c.getManufacturer(), 0);
          }
          cameraCounts.put(c.getManufacturer(), cameraCounts.get(c.getManufacturer()) + 1);
        }

        QGroupResult g = QGroupResult.groupResult;

        List<String> camerasByHits = from(g, mapToTuple(cameraCounts))
            .orderBy(g.name.toLowerCase().asc()).offset(2).limit(2).list(g.name.toLowerCase());

        assertEquals(2, facetResults.getResults().get("Manufacturer").getValues().size());
        for (int i = 0; i < 2; i++) {
          assertEquals(camerasByHits.get(i), facetResults.getResults().get("Manufacturer").getValues().get(i).getRange());
        }
        for (FacetValue facet : facetResults.getResults().get("Manufacturer").getValues()) {
          long inMemoryCount = from(x, data).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTermsCount());
        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTerms().size());
        List<Integer> list = from(g, mapToTuple(cameraCounts))
            .orderBy(g.name.toLowerCase().asc()).list(g.count);

        assertEquals(list.get(list.size()-1).intValue(), facetResults.getResults().get("Manufacturer").getRemainingHits());

      }
    }
  }

  @Test
  public void canPerformFacetedPagingSearchWithPageSize_HitsDesc_LuceneQuery() throws Exception {
    //also specify more results than we have
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");
    facet1.setMaxResults(3);
    facet1.setTermSortMode(FacetTermSortMode.HITS_DESC);
    facet1.setIncludeRemainingTerms(true);
    List<Facet> facets = Arrays.asList(facet1);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);
      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        FacetResults facetResults = s.advanced().luceneQuery(Camera.class, "CameraCost").toFacets("facets/CameraFacets", 2, 2);

        QCamera x = QCamera.camera;

        Map<String, Integer> cameraCounts = new HashMap<>();
        for (Camera c: data) {
          if (!cameraCounts.containsKey(c.getManufacturer())) {
            cameraCounts.put(c.getManufacturer(), 0);
          }
          cameraCounts.put(c.getManufacturer(), cameraCounts.get(c.getManufacturer()) + 1);
        }

        QGroupResult g = QGroupResult.groupResult;

        List<String> camerasByHits = from(g, mapToTuple(cameraCounts))
            .orderBy(g.count.desc(), g.name.toLowerCase().asc()).offset(2).limit(2).list(g.name.toLowerCase());

        assertEquals(2, facetResults.getResults().get("Manufacturer").getValues().size());
        for (int i = 0; i < 2; i++) {
          assertEquals(camerasByHits.get(i), facetResults.getResults().get("Manufacturer").getValues().get(i).getRange());
        }
        for (FacetValue facet : facetResults.getResults().get("Manufacturer").getValues()) {
          long inMemoryCount = from(x, data).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTermsCount());
        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTerms().size());
        List<Integer> list = from(g, mapToTuple(cameraCounts))
            .orderBy(g.count.desc(), g.name.toLowerCase().asc()).list(g.count);

        assertEquals(list.get(list.size()-1).intValue(), facetResults.getResults().get("Manufacturer").getRemainingHits());

      }
    }
  }

  @Test
  public void canPerformFacetedPagingSearchWithPageSize_HitsAsc_LuceneQuery() throws Exception {
    //also specify more results than we have
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");
    facet1.setMaxResults(3);
    facet1.setTermSortMode(FacetTermSortMode.HITS_ASC);
    facet1.setIncludeRemainingTerms(true);
    List<Facet> facets = Arrays.asList(facet1);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);
      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        FacetResults facetResults = s.advanced().luceneQuery(Camera.class, "CameraCost").toFacets("facets/CameraFacets", 2, 2);

        QCamera x = QCamera.camera;

        Map<String, Integer> cameraCounts = new HashMap<>();
        for (Camera c: data) {
          if (!cameraCounts.containsKey(c.getManufacturer())) {
            cameraCounts.put(c.getManufacturer(), 0);
          }
          cameraCounts.put(c.getManufacturer(), cameraCounts.get(c.getManufacturer()) + 1);
        }

        QGroupResult g = QGroupResult.groupResult;

        List<String> camerasByHits = from(g, mapToTuple(cameraCounts))
            .orderBy(g.count.asc(), g.name.toLowerCase().asc()).offset(2).limit(2).list(g.name.toLowerCase());

        assertEquals(2, facetResults.getResults().get("Manufacturer").getValues().size());
        for (int i = 0; i < 2; i++) {
          assertEquals(camerasByHits.get(i), facetResults.getResults().get("Manufacturer").getValues().get(i).getRange());
        }
        for (FacetValue facet : facetResults.getResults().get("Manufacturer").getValues()) {
          long inMemoryCount = from(x, data).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTermsCount());
        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTerms().size());
        List<Integer> list = from(g, mapToTuple(cameraCounts))
            .orderBy(g.count.asc(), g.name.toLowerCase().asc()).list(g.count);

        assertEquals(list.get(list.size()-1).intValue(), facetResults.getResults().get("Manufacturer").getRemainingHits());

      }
    }
  }

  @Test
  public void canPerformFacetedPagingSearchWithPageSize_TermDesc_LuceneQuery() throws Exception {
    //also specify more results than we have
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");
    facet1.setMaxResults(3);
    facet1.setTermSortMode(FacetTermSortMode.VALUE_DESC);
    facet1.setIncludeRemainingTerms(true);
    List<Facet> facets = Arrays.asList(facet1);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);
      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        FacetResults facetResults = s.advanced().luceneQuery(Camera.class, "CameraCost").toFacets("facets/CameraFacets", 2, 2);

        QCamera x = QCamera.camera;

        Map<String, Integer> cameraCounts = new HashMap<>();
        for (Camera c: data) {
          if (!cameraCounts.containsKey(c.getManufacturer())) {
            cameraCounts.put(c.getManufacturer(), 0);
          }
          cameraCounts.put(c.getManufacturer(), cameraCounts.get(c.getManufacturer()) + 1);
        }

        QGroupResult g = QGroupResult.groupResult;

        List<String> camerasByHits = from(g, mapToTuple(cameraCounts))
            .orderBy(g.name.toLowerCase().desc()).offset(2).limit(2).list(g.name.toLowerCase());

        assertEquals(2, facetResults.getResults().get("Manufacturer").getValues().size());
        for (int i = 0; i < 2; i++) {
          assertEquals(camerasByHits.get(i), facetResults.getResults().get("Manufacturer").getValues().get(i).getRange());
        }
        for (FacetValue facet : facetResults.getResults().get("Manufacturer").getValues()) {
          long inMemoryCount = from(x, data).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTermsCount());
        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTerms().size());
        List<Integer> list = from(g, mapToTuple(cameraCounts))
            .orderBy(g.name.toLowerCase().desc()).list(g.count);

        assertEquals(list.get(list.size()-1).intValue(), facetResults.getResults().get("Manufacturer").getRemainingHits());

      }
    }
  }

  @Test
  public void canPerformFacetedPagingSearchWithPageSize_TermAsc_LuceneQuery() throws Exception {
    //also specify more results than we have
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");
    facet1.setMaxResults(3);
    facet1.setTermSortMode(FacetTermSortMode.VALUE_ASC);
    facet1.setIncludeRemainingTerms(true);
    List<Facet> facets = Arrays.asList(facet1);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);
      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        FacetResults facetResults = s.advanced().luceneQuery(Camera.class, "CameraCost").toFacets("facets/CameraFacets", 2, 2);

        QCamera x = QCamera.camera;

        Map<String, Integer> cameraCounts = new HashMap<>();
        for (Camera c: data) {
          if (!cameraCounts.containsKey(c.getManufacturer())) {
            cameraCounts.put(c.getManufacturer(), 0);
          }
          cameraCounts.put(c.getManufacturer(), cameraCounts.get(c.getManufacturer()) + 1);
        }

        QGroupResult g = QGroupResult.groupResult;

        List<String> camerasByHits = from(g, mapToTuple(cameraCounts))
            .orderBy(g.name.toLowerCase().asc()).offset(2).limit(2).list(g.name.toLowerCase());

        assertEquals(2, facetResults.getResults().get("Manufacturer").getValues().size());
        for (int i = 0; i < 2; i++) {
          assertEquals(camerasByHits.get(i), facetResults.getResults().get("Manufacturer").getValues().get(i).getRange());
        }
        for (FacetValue facet : facetResults.getResults().get("Manufacturer").getValues()) {
          long inMemoryCount = from(x, data).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTermsCount());
        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTerms().size());
        List<Integer> list = from(g, mapToTuple(cameraCounts))
            .orderBy(g.name.toLowerCase().asc()).list(g.count);

        assertEquals(list.get(list.size()-1).intValue(), facetResults.getResults().get("Manufacturer").getRemainingHits());

      }
    }
  }

  @Test
  public void canPerformFacetedPagingSearchWithNoPageSizeNoMaxResults_HitsDesc_LuceneQuery() throws Exception {
    //also specify more results than we have
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");
    facet1.setMaxResults(null);
    facet1.setTermSortMode(FacetTermSortMode.HITS_DESC);
    facet1.setIncludeRemainingTerms(true);
    List<Facet> facets = Arrays.asList(facet1);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);
      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        FacetResults facetResults = s.advanced().luceneQuery(Camera.class, "CameraCost").toFacets("facets/CameraFacets", 2);

        QCamera x = QCamera.camera;

        Map<String, Integer> cameraCounts = new HashMap<>();
        for (Camera c: data) {
          if (!cameraCounts.containsKey(c.getManufacturer())) {
            cameraCounts.put(c.getManufacturer(), 0);
          }
          cameraCounts.put(c.getManufacturer(), cameraCounts.get(c.getManufacturer()) + 1);
        }

        QGroupResult g = QGroupResult.groupResult;

        List<String> camerasByHits = from(g, mapToTuple(cameraCounts))
            .orderBy(g.count.desc(), g.name.asc()).offset(2).list(g.name.toLowerCase());

        assertEquals(3, facetResults.getResults().get("Manufacturer").getValues().size());
        for (int i = 0; i < 3; i++) {
          assertEquals(camerasByHits.get(i), facetResults.getResults().get("Manufacturer").getValues().get(i).getRange());
        }
        for (FacetValue facet : facetResults.getResults().get("Manufacturer").getValues()) {
          long inMemoryCount = from(x, data).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(0, facetResults.getResults().get("Manufacturer").getRemainingTermsCount());
        assertEquals(0, facetResults.getResults().get("Manufacturer").getRemainingTerms().size());
        assertEquals(0, facetResults.getResults().get("Manufacturer").getRemainingHits());

      }
    }
  }

  @Test
  public void canPerformFacetedPagingSearchWithNoPageSizeWithMaxResults_HitsDesc_LuceneQuery() throws Exception {
    //also specify more results than we have
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");
    facet1.setMaxResults(2);
    facet1.setTermSortMode(FacetTermSortMode.HITS_DESC);
    facet1.setIncludeRemainingTerms(true);
    List<Facet> facets = Arrays.asList(facet1);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);
      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        FacetResults facetResults = s.advanced().luceneQuery(Camera.class, "CameraCost").toFacets("facets/CameraFacets", 2);

        QCamera x = QCamera.camera;

        Map<String, Integer> cameraCounts = new HashMap<>();
        for (Camera c: data) {
          if (!cameraCounts.containsKey(c.getManufacturer())) {
            cameraCounts.put(c.getManufacturer(), 0);
          }
          cameraCounts.put(c.getManufacturer(), cameraCounts.get(c.getManufacturer()) + 1);
        }

        QGroupResult g = QGroupResult.groupResult;

        List<String> camerasByHits = from(g, mapToTuple(cameraCounts))
            .orderBy(g.count.desc(), g.name.asc()).offset(2).limit(2).list(g.name.toLowerCase());

        assertEquals(2, facetResults.getResults().get("Manufacturer").getValues().size());
        for (int i = 0; i < 2; i++) {
          assertEquals(camerasByHits.get(i), facetResults.getResults().get("Manufacturer").getValues().get(i).getRange());
        }
        for (FacetValue facet : facetResults.getResults().get("Manufacturer").getValues()) {
          long inMemoryCount = from(x, data).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTermsCount());
        assertEquals(1, facetResults.getResults().get("Manufacturer").getRemainingTerms().size());
        List<Integer> list = from(g, mapToTuple(cameraCounts))
            .orderBy(g.count.desc(), g.name.asc()).list(g.count);

        assertEquals(list.get(list.size()-1).intValue(), facetResults.getResults().get("Manufacturer").getRemainingHits());

      }
    }
  }

  private List<GroupResult> mapToTuple(Map<String, Integer> map) {
    List<GroupResult> list=  new ArrayList<>();
    for (Map.Entry<String, Integer> entry : map.entrySet()) {
      list.add(new GroupResult(entry.getKey(), entry.getValue()));
    }
    return list;
  }

  private void setup(IDocumentStore store) throws Exception {
    try (IDocumentSession s = store.openSession()) {
      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from camera in docs " +
          "  select new " +
          "  {  " +
          "      camera.Manufacturer, " +
          "      camera.Model,  " +
          "      camera.Cost, " +
          "      camera.DateOfListing, " +
          "      camera.Megapixels " +
          "  }");
      store.getDatabaseCommands().putIndex("CameraCost", indexDefinition);

      int count = 0;
      for (Camera camera : data) {
        s.store(camera);
        count++;

        if (count % (numCameras / 25) == 0) {
          s.saveChanges();
        }
      }
      s.saveChanges();

      s.query(Camera.class, "CameraCost").customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults()).toList();
    }
  }

}

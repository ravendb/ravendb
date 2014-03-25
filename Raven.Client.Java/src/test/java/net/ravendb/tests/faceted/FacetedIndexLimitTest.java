package net.ravendb.tests.faceted;

import static com.mysema.query.collections.CollQueryFactory.from;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.ravendb.abstractions.data.Facet;
import net.ravendb.abstractions.data.FacetResult;
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

import org.apache.commons.lang.time.DateUtils;
import org.junit.Test;

public class FacetedIndexLimitTest extends FacetTestBase {
  private final List<Camera> data;
  private final int numCameras = 1000;

  public FacetedIndexLimitTest() {
    data = getCameras(numCameras);
  }

  @Test
  public void canPerformSearchWithTwoDefaultFacets() throws Exception {
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");

    Facet facet2 = new Facet();
    facet2.setName("Model");

    List<Facet> facets = Arrays.asList(facet1, facet2);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);

      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        FacetResults facetResults = s.query(Camera.class, "CameraCost").toFacets("facets/CameraFacets");

        assertEquals(5, facetResults.getResults().get("Manufacturer").getValues().size());
        assertEquals("canon", facetResults.getResults().get("Manufacturer").getValues().get(0).getRange());
        assertEquals("jessops", facetResults.getResults().get("Manufacturer").getValues().get(1).getRange());
        assertEquals("nikon", facetResults.getResults().get("Manufacturer").getValues().get(2).getRange());
        assertEquals("phillips", facetResults.getResults().get("Manufacturer").getValues().get(3).getRange());
        assertEquals("sony", facetResults.getResults().get("Manufacturer").getValues().get(4).getRange());

        QCamera x = QCamera.camera;

        for (FacetValue facet : facetResults.getResults().get("Manufacturer").getValues()) {
          long inMemoryCount = from(x, data).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(new ArrayList<>(), facetResults.getResults().get("Manufacturer").getRemainingTerms());
        assertEquals(0, facetResults.getResults().get("Manufacturer").getRemainingTermsCount());
        assertEquals(0, facetResults.getResults().get("Manufacturer").getRemainingHits());

        assertEquals(5, facetResults.getResults().get("Model").getValues().size());
        assertEquals("model1", facetResults.getResults().get("Model").getValues().get(0).getRange());
        assertEquals("model2", facetResults.getResults().get("Model").getValues().get(1).getRange());
        assertEquals("model3", facetResults.getResults().get("Model").getValues().get(2).getRange());
        assertEquals("model4", facetResults.getResults().get("Model").getValues().get(3).getRange());
        assertEquals("model5", facetResults.getResults().get("Model").getValues().get(4).getRange());

        for (FacetValue facet : facetResults.getResults().get("Model").getValues()) {
          long inMemoryCount = from(x, data).where(x.model.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(new ArrayList<>(), facetResults.getResults().get("Model").getRemainingTerms());
        assertEquals(0, facetResults.getResults().get("Model").getRemainingTermsCount());
        assertEquals(0, facetResults.getResults().get("Model").getRemainingHits());
      }
    }
  }

  @Test
  public void canPerformFacetedLimitSearch_TermAsc() throws Exception {
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");
    facet1.setMaxResults(2);
    facet1.setIncludeRemainingTerms(true);

    List<Facet> facets = Arrays.asList(facet1);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);
      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        QCamera x = QCamera.camera;

        Calendar cal = Calendar.getInstance();
        cal.set(2000, 0, 1);
        Date date = DateUtils.truncate(cal.getTime(), Calendar.DAY_OF_MONTH);

        FacetResults facetResults = s.query(Camera.class, "CameraCost").where(x.dateOfListing.gt(date)).toFacets("facets/CameraFacets");

        assertEquals(2, facetResults.getResults().get("Manufacturer").getValues().size());
        assertEquals("canon", facetResults.getResults().get("Manufacturer").getValues().get(0).getRange());
        assertEquals("jessops", facetResults.getResults().get("Manufacturer").getValues().get(1).getRange());


        for (FacetValue facet : facetResults.getResults().get("Manufacturer").getValues()) {
          long inMemoryCount = from(x, data).where(x.dateOfListing.gt(date)).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(3, facetResults.getResults().get("Manufacturer").getRemainingTermsCount());
        assertEquals(3, facetResults.getResults().get("Manufacturer").getRemainingTerms().size());
        assertEquals("nikon", facetResults.getResults().get("Manufacturer").getRemainingTerms().get(0));
        assertEquals("phillips", facetResults.getResults().get("Manufacturer").getRemainingTerms().get(1));
        assertEquals("sony", facetResults.getResults().get("Manufacturer").getRemainingTerms().get(2));

        assertEquals(from (x, data).where(x.dateOfListing.gt(date)).count(), facetResults.getResults().get("Manufacturer").getValues().get(0).getHits()
            + facetResults.getResults().get("Manufacturer").getValues().get(1).getHits() + facetResults.getResults().get("Manufacturer").getRemainingHits() );

      }
    }

  }

  @Test
  public void canPerformFacetedLimitSearch_TermDesc() throws Exception {
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

        QCamera x = QCamera.camera;

        Calendar cal = Calendar.getInstance();
        cal.set(2000, 0, 1);
        Date date = DateUtils.truncate(cal.getTime(), Calendar.DAY_OF_MONTH);

        FacetResults facetResults = s.query(Camera.class, "CameraCost").where(x.dateOfListing.gt(date)).toFacets("facets/CameraFacets");

        assertEquals(3, facetResults.getResults().get("Manufacturer").getValues().size());
        assertEquals("sony", facetResults.getResults().get("Manufacturer").getValues().get(0).getRange());
        assertEquals("phillips", facetResults.getResults().get("Manufacturer").getValues().get(1).getRange());
        assertEquals("nikon", facetResults.getResults().get("Manufacturer").getValues().get(2).getRange());


        for (FacetValue facet : facetResults.getResults().get("Manufacturer").getValues()) {
          long inMemoryCount = from(x, data).where(x.dateOfListing.gt(date)).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(2, facetResults.getResults().get("Manufacturer").getRemainingTermsCount());
        assertEquals(2, facetResults.getResults().get("Manufacturer").getRemainingTerms().size());
        assertEquals("jessops", facetResults.getResults().get("Manufacturer").getRemainingTerms().get(0));
        assertEquals("canon", facetResults.getResults().get("Manufacturer").getRemainingTerms().get(1));

        assertEquals(from (x, data).where(x.dateOfListing.gt(date)).count(), facetResults.getResults().get("Manufacturer").getValues().get(0).getHits()
            + facetResults.getResults().get("Manufacturer").getValues().get(1).getHits()
            + facetResults.getResults().get("Manufacturer").getValues().get(2).getHits()
            + facetResults.getResults().get("Manufacturer").getRemainingHits() );

      }
    }
  }

  @Test
  public void canPerformFacetedLimitSearch_HitsAsc() throws Exception {
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");
    facet1.setMaxResults(2);
    facet1.setTermSortMode(FacetTermSortMode.HITS_ASC);
    facet1.setIncludeRemainingTerms(true);

    List<Facet> facets = Arrays.asList(facet1);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);
      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        FacetResults facetResults = s.query(Camera.class, "CameraCost").toFacets("facets/CameraFacets");

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
            .orderBy(g.count.asc(), g.name.asc()).list(g.name.toLowerCase());

        FacetResult manufacturer = facetResults.getResults().get("Manufacturer");
        assertEquals(2, manufacturer.getValues().size());
        assertEquals(camerasByHits.get(0), manufacturer.getValues().get(0).getRange());
        assertEquals(camerasByHits.get(1), manufacturer.getValues().get(1).getRange());

        for (FacetValue facet : manufacturer.getValues()) {
          long inMemoryCount = from(x, data).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(3, manufacturer.getRemainingTermsCount());
        assertEquals(3, manufacturer.getRemainingTerms().size());
        assertEquals(camerasByHits.get(2), manufacturer.getRemainingTerms().get(0));
        assertEquals(camerasByHits.get(3), manufacturer.getRemainingTerms().get(1));
        assertEquals(camerasByHits.get(4), manufacturer.getRemainingTerms().get(2));

        assertEquals(data.size(), manufacturer.getValues().get(0).getHits()  + manufacturer.getValues().get(1).getHits() + manufacturer.getRemainingHits());

      }
    }
  }

  @Test
  public void canPerformFacetedLimitSearch_HitsDesc() throws Exception {
  //also specify more results than we have
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");
    facet1.setMaxResults(20);
    facet1.setTermSortMode(FacetTermSortMode.HITS_DESC);
    facet1.setIncludeRemainingTerms(true);
    List<Facet> facets = Arrays.asList(facet1);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);
      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        FacetResults facetResults = s.query(Camera.class, "CameraCost").toFacets("facets/CameraFacets");

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
            .orderBy(g.count.desc(), g.name.asc()).list(g.name.toLowerCase());

        assertEquals(5, facetResults.getResults().get("Manufacturer").getValues().size());
        for (int i = 0; i < 4; i++) {
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
  public void canPerformSearchWithTwoDefaultFacets_LuceneQuery()  throws Exception {
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");

    Facet facet2 = new Facet();
    facet2.setName("Model");

    List<Facet> facets = Arrays.asList(facet1, facet2);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);

      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        FacetResults facetResults = s.advanced().documentQuery(Camera.class, "CameraCost").toFacets("facets/CameraFacets");

        assertEquals(5, facetResults.getResults().get("Manufacturer").getValues().size());
        assertEquals("canon", facetResults.getResults().get("Manufacturer").getValues().get(0).getRange());
        assertEquals("jessops", facetResults.getResults().get("Manufacturer").getValues().get(1).getRange());
        assertEquals("nikon", facetResults.getResults().get("Manufacturer").getValues().get(2).getRange());
        assertEquals("phillips", facetResults.getResults().get("Manufacturer").getValues().get(3).getRange());
        assertEquals("sony", facetResults.getResults().get("Manufacturer").getValues().get(4).getRange());

        QCamera x = QCamera.camera;

        for (FacetValue facet : facetResults.getResults().get("Manufacturer").getValues()) {
          long inMemoryCount = from(x, data).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(new ArrayList<>(), facetResults.getResults().get("Manufacturer").getRemainingTerms());
        assertEquals(0, facetResults.getResults().get("Manufacturer").getRemainingTermsCount());
        assertEquals(0, facetResults.getResults().get("Manufacturer").getRemainingHits());

        assertEquals(5, facetResults.getResults().get("Model").getValues().size());
        assertEquals("model1", facetResults.getResults().get("Model").getValues().get(0).getRange());
        assertEquals("model2", facetResults.getResults().get("Model").getValues().get(1).getRange());
        assertEquals("model3", facetResults.getResults().get("Model").getValues().get(2).getRange());
        assertEquals("model4", facetResults.getResults().get("Model").getValues().get(3).getRange());
        assertEquals("model5", facetResults.getResults().get("Model").getValues().get(4).getRange());

        for (FacetValue facet : facetResults.getResults().get("Model").getValues()) {
          long inMemoryCount = from(x, data).where(x.model.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(new ArrayList<>(), facetResults.getResults().get("Model").getRemainingTerms());
        assertEquals(0, facetResults.getResults().get("Model").getRemainingTermsCount());
        assertEquals(0, facetResults.getResults().get("Model").getRemainingHits());
      }
    }
  }

  @Test
  public void canPerformFacetedLimitSearch_TermAsc_LuceneQuery() throws Exception {
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");
    facet1.setMaxResults(2);
    facet1.setIncludeRemainingTerms(true);

    List<Facet> facets = Arrays.asList(facet1);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);
      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        QCamera x = QCamera.camera;

        Calendar cal = Calendar.getInstance();
        cal.set(2000, 0, 1);
        Date date = DateUtils.truncate(cal.getTime(), Calendar.DAY_OF_MONTH);

        FacetResults facetResults = s.advanced().documentQuery(Camera.class, "CameraCost").whereGreaterThan(x.dateOfListing, date).toFacets("facets/CameraFacets");

        assertEquals(2, facetResults.getResults().get("Manufacturer").getValues().size());
        assertEquals("canon", facetResults.getResults().get("Manufacturer").getValues().get(0).getRange());
        assertEquals("jessops", facetResults.getResults().get("Manufacturer").getValues().get(1).getRange());


        for (FacetValue facet : facetResults.getResults().get("Manufacturer").getValues()) {
          long inMemoryCount = from(x, data).where(x.dateOfListing.gt(date)).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(3, facetResults.getResults().get("Manufacturer").getRemainingTermsCount());
        assertEquals(3, facetResults.getResults().get("Manufacturer").getRemainingTerms().size());
        assertEquals("nikon", facetResults.getResults().get("Manufacturer").getRemainingTerms().get(0));
        assertEquals("phillips", facetResults.getResults().get("Manufacturer").getRemainingTerms().get(1));
        assertEquals("sony", facetResults.getResults().get("Manufacturer").getRemainingTerms().get(2));

        assertEquals(from (x, data).where(x.dateOfListing.gt(date)).count(), facetResults.getResults().get("Manufacturer").getValues().get(0).getHits()
            + facetResults.getResults().get("Manufacturer").getValues().get(1).getHits() + facetResults.getResults().get("Manufacturer").getRemainingHits() );

      }
    }
  }

  @Test
  public void canPerformFacetedLimitSearch_TermDesc_LuceneQuery() throws Exception {
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

        QCamera x = QCamera.camera;

        Calendar cal = Calendar.getInstance();
        cal.set(2000, 0, 1);
        Date date = DateUtils.truncate(cal.getTime(), Calendar.DAY_OF_MONTH);

        FacetResults facetResults = s.advanced().documentQuery(Camera.class, "CameraCost").whereGreaterThan(x.dateOfListing, date).toFacets("facets/CameraFacets");

        assertEquals(3, facetResults.getResults().get("Manufacturer").getValues().size());
        assertEquals("sony", facetResults.getResults().get("Manufacturer").getValues().get(0).getRange());
        assertEquals("phillips", facetResults.getResults().get("Manufacturer").getValues().get(1).getRange());
        assertEquals("nikon", facetResults.getResults().get("Manufacturer").getValues().get(2).getRange());


        for (FacetValue facet : facetResults.getResults().get("Manufacturer").getValues()) {
          long inMemoryCount = from(x, data).where(x.dateOfListing.gt(date)).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(2, facetResults.getResults().get("Manufacturer").getRemainingTermsCount());
        assertEquals(2, facetResults.getResults().get("Manufacturer").getRemainingTerms().size());
        assertEquals("jessops", facetResults.getResults().get("Manufacturer").getRemainingTerms().get(0));
        assertEquals("canon", facetResults.getResults().get("Manufacturer").getRemainingTerms().get(1));

        assertEquals(from (x, data).where(x.dateOfListing.gt(date)).count(), facetResults.getResults().get("Manufacturer").getValues().get(0).getHits()
            + facetResults.getResults().get("Manufacturer").getValues().get(1).getHits()
            + facetResults.getResults().get("Manufacturer").getValues().get(2).getHits()
            + facetResults.getResults().get("Manufacturer").getRemainingHits() );

      }
    }
  }

  @Test
  public void canPerformFacetedLimitSearch_HitsAsc_LuceneQuery() throws Exception {
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");
    facet1.setMaxResults(2);
    facet1.setTermSortMode(FacetTermSortMode.HITS_ASC);
    facet1.setIncludeRemainingTerms(true);

    List<Facet> facets = Arrays.asList(facet1);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);
      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        FacetResults facetResults = s.advanced().documentQuery(Camera.class, "CameraCost").toFacets("facets/CameraFacets");

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
            .orderBy(g.count.asc(), g.name.asc()).list(g.name.toLowerCase());

        FacetResult manufacturer = facetResults.getResults().get("Manufacturer");
        assertEquals(2, manufacturer.getValues().size());
        assertEquals(camerasByHits.get(0), manufacturer.getValues().get(0).getRange());
        assertEquals(camerasByHits.get(1), manufacturer.getValues().get(1).getRange());

        for (FacetValue facet : manufacturer.getValues()) {
          long inMemoryCount = from(x, data).where(x.manufacturer.toLowerCase().eq(facet.getRange())).count();
          assertEquals(inMemoryCount, facet.getHits());
        }

        assertEquals(3, manufacturer.getRemainingTermsCount());
        assertEquals(3, manufacturer.getRemainingTerms().size());
        assertEquals(camerasByHits.get(2), manufacturer.getRemainingTerms().get(0));
        assertEquals(camerasByHits.get(3), manufacturer.getRemainingTerms().get(1));
        assertEquals(camerasByHits.get(4), manufacturer.getRemainingTerms().get(2));

        assertEquals(data.size(), manufacturer.getValues().get(0).getHits()  + manufacturer.getValues().get(1).getHits() + manufacturer.getRemainingHits());

      }
    }
  }

  @Test
  public void canPerformFacetedLimitSearch_HitsDesc_LuceneQuery() throws Exception {
  //also specify more results than we have
    Facet facet1 = new Facet();
    facet1.setName("Manufacturer");
    facet1.setMaxResults(20);
    facet1.setTermSortMode(FacetTermSortMode.HITS_DESC);
    facet1.setIncludeRemainingTerms(true);
    List<Facet> facets = Arrays.asList(facet1);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);
      try (IDocumentSession s = store.openSession()) {
        s.store(new FacetSetup("facets/CameraFacets", facets));
        s.saveChanges();

        FacetResults facetResults = s.advanced().documentQuery(Camera.class, "CameraCost").toFacets("facets/CameraFacets");

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
            .orderBy(g.count.desc(), g.name.asc()).list(g.name.toLowerCase());

        assertEquals(5, facetResults.getResults().get("Manufacturer").getValues().size());
        for (int i = 0; i < 4; i++) {
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

      s.query(Camera.class, "CameraCost").customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults(5 * 60 *1000)).toList();
    }
  }
}

package net.ravendb.tests.spatial;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.List;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.indexing.FieldIndexing;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RavenQueryStatistics;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.client.linq.IRavenQueryable;
import net.ravendb.tests.spatial.QEvent;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Test;



public class SpatialSearchTest extends RemoteClientTest {

  public static class SpatialIdx extends AbstractIndexCreationTask {
    public SpatialIdx() {
      map = "from e in docs.Events select new { e.Capacity, e.Venue, e.Date, _ = SpatialGenerate(e.Latitude, e.Longitude)}";
      QEvent x = QEvent.event;
      index(x.venue, FieldIndexing.ANALYZED);
    }
  }

  @Test
  public void can_do_spatial_search_with_client_api() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new SpatialIdx().execute(store);
      try (IDocumentSession session = store.openSession()) {
        session.store(new Event("a/1", 38.9579000, -77.3572000, new Date()));
        session.store(new Event("a/2", 38.9690000, -77.3862000, DateUtils.addDays(new Date(), 1)));
        session.store(new Event("b/2", 38.9690000, -77.3862000, DateUtils.addDays(new Date(), 2)));
        session.store(new Event("c/3", 38.9510000, -77.4107000, DateUtils.addDays(new Date(), 3)));
        session.store(new Event("d/1", 37.9510000, -77.4107000, DateUtils.addDays(new Date(), 3)));
        session.saveChanges();
      }

      waitForNonStaleIndexes(store.getDatabaseCommands());

      QEvent x = QEvent.event;

      try (IDocumentSession session = store.openSession()) {
        Reference<RavenQueryStatistics> statsRef = new Reference<>();
        session.advanced().luceneQuery(Event.class, "SpatialIdx")
        .statistics(statsRef)
        .whereLessThanOrEqual("Date", DateUtils.addYears(new Date(), 1))
        .withinRadiusOf(6.0,  38.96939, -77.386398)
        .orderByDescending(x.date)
        .toList();

        assertTrue(statsRef.value.getTotalResults() > 0);
      }
    }
  }

  @Test
  public void can_do_spatial_search_with_client_api3() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new SpatialIdx().execute(store);
      try (IDocumentSession session = store.openSession()) {
        IRavenQueryable<Event> matchingVenues = session.query(Event.class, SpatialIdx.class)
          .customize(new DocumentQueryCustomizationFactory().withinRadiusOf(5, 38.9103000, -77.3942).waitForNonStaleResults());
        assertEquals(" SpatialField: __spatial QueryShape: Circle(-77.394200 38.910300 d=5.000000) Relation: Within", matchingVenues.toString());
      }
    }
  }

  @Test
  public void can_do_spatial_search_with_client_api_within_given_capacity() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      new SpatialIdx().execute(store);

      try (IDocumentSession session = store.openSession()) {
        session.store(new Event("a/1", 38.9579000, -77.3572000, new Date(), 5000));
        session.store(new Event("a/2", 38.9690000, -77.3862000, DateUtils.addDays(new Date(), 1), 5000));
        session.store(new Event("b/2", 38.9690000, -77.3862000, DateUtils.addDays(new Date(), 2), 2000));
        session.store(new Event("c/3", 38.9510000, -77.4107000, DateUtils.addYears(new Date(), 3), 1500));
        session.store(new Event("d/1", 37.9510000, -77.4107000, DateUtils.addYears(new Date(), 3), 1500));
        session.saveChanges();
      }

      waitForNonStaleIndexes(store.getDatabaseCommands());

      try (IDocumentSession session = store.openSession()) {
        Reference<RavenQueryStatistics> statsRef = new Reference<>();
        QEvent x = QEvent.event;
        List<Event> events = session.advanced().luceneQuery(Event.class, "SpatialIdx")
          .statistics(statsRef)
          .whereBetweenOrEqual("Capacity", 0, 2000)
          .withinRadiusOf(6.0, 38.96939, -77.386398)
          .orderByDescending(x.date)
          .toList();

        assertEquals(2, statsRef.value.getTotalResults());

        String[] expectedOrder = new String[] { "c/3", "b/2" };

        for (int i = 0; i < events.size(); i++) {
          assertEquals(expectedOrder[i], events.get(i).getVenue());
        }
      }

      try (IDocumentSession session = store.openSession()) {
        Reference<RavenQueryStatistics> statsRef = new Reference<>();
        QEvent x = QEvent.event;
        List<Event> events = session.advanced().luceneQuery(Event.class, "SpatialIdx")
          .statistics(statsRef)
          .whereBetweenOrEqual("Capacity", 0, 2000)
          .withinRadiusOf(6.0, 38.96939, -77.386398)
          .orderBy(x.date)
          .toList();

        assertEquals(2, statsRef.value.getTotalResults());

        String[] expectedOrder = new String[] { "b/2", "c/3" };

        for (int i = 0; i < events.size(); i++) {
          assertEquals(expectedOrder[i], events.get(i).getVenue());
        }
      }
    }
  }

  @Test
  public void can_do_spatial_search_with_client_api_addorder() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      new SpatialIdx().execute(store);

      try (IDocumentSession session = store.openSession()) {
        session.store(new Event("a/1", 38.9579000, -77.3572000));
        session.store(new Event("b/1", 38.9579000, -77.3572000));
        session.store(new Event("c/1", 38.9579000, -77.3572000));
        session.store(new Event("a/2", 38.9690000, -77.3862000));
        session.store(new Event("b/2", 38.9690000, -77.3862000));
        session.store(new Event("c/2", 38.9690000, -77.3862000));
        session.store(new Event("a/3", 38.9510000, -77.4107000));
        session.store(new Event("b/3", 38.9510000, -77.4107000));
        session.store(new Event("c/3", 38.9510000, -77.4107000));
        session.store(new Event("d/1", 37.9510000, -77.4107000));
        session.saveChanges();
      }

      waitForNonStaleIndexes(store.getDatabaseCommands());

      try (IDocumentSession session = store.openSession()) {
        List<Event> events = session.advanced().luceneQuery(Event.class, "SpatialIdx")
          .withinRadiusOf(6.0, 38.96939, -77.386398)
          .sortByDistance()
          .addOrder("Venue", false)
          .toList();

        String[] expectedOrder = new String[] { "a/2", "b/2", "c/2", "a/1", "b/1", "c/1", "a/3", "b/3", "c/3" };

        assertEquals(expectedOrder.length, events.size());

        for (int i = 0; i < events.size(); i++) {
          assertEquals(expectedOrder[i], events.get(i).getVenue());
        }
      }

      try (IDocumentSession session = store.openSession()) {
        List<Event> events = session.advanced().luceneQuery(Event.class, "SpatialIdx")
          .withinRadiusOf(6.0, 38.96939, -77.386398)
          .addOrder("Venue", false)
          .sortByDistance()
          .toList();

        String[] expectedOrder = new String[] { "a/1", "a/2", "a/3", "b/1", "b/2", "b/3", "c/1", "c/2", "c/3" };
        assertEquals(expectedOrder.length, events.size());

        for (int i = 0; i < events.size(); i++) {
          assertEquals(expectedOrder[i], events.get(i).getVenue());
        }

      }
    }
  }
}

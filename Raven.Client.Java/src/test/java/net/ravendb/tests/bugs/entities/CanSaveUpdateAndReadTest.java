package net.ravendb.tests.bugs.entities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.List;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;

import org.junit.Test;


public class CanSaveUpdateAndReadTest extends RemoteClientTest {

  @Test
  public void canReadEntityNameAfterUpdate() throws Exception  {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession s = store.openSession()) {
        Event e = new Event();
        e.setHappy(true);
        s.store(e);
        s.saveChanges();
      }

      try (IDocumentSession s = store.openSession()) {
        s.load(Event.class, "events/1").happy = false;
        s.saveChanges();
      }

      try (IDocumentSession s = store.openSession()) {
        Event e = s.load(Event.class, "events/1");
        String entityName = s.advanced().getMetadataFor(e).value(String.class, "Raven-Entity-Name");
        assertEquals("Events", entityName);
      }
    }
  }

  @Test
  public void can_read_entity_name_after_update_from_query() throws Exception  {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession s = store.openSession()) {
        Event e = new Event();
        e.setHappy(true);
        s.store(e);
        s.saveChanges();
      }

      try (IDocumentSession s = store.openSession()) {
        s.load(Event.class, "events/1").happy = false;
        s.saveChanges();
      }

      try (IDocumentSession s = store.openSession()) {
        List<Event> events = s.query(Event.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults()).toList();
        assertFalse(events.isEmpty());
      }
    }
  }

  @Test
  public void can_read_entity_name_after_update_from_query_after_entity_is_in_cache() throws Exception  {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession s = store.openSession()) {
        Event e = new Event();
        e.setHappy(true);
        s.store(e);
        s.saveChanges();
      }

      try (IDocumentSession s = store.openSession()) {
        s.load(Event.class, "events/1"); // load into cache
      }

      try (IDocumentSession s = store.openSession()) {
        s.load(Event.class, "events/1").happy = false;
        s.saveChanges();
      }

      try (IDocumentSession s = store.openSession()) {
        List<Event> events = s.query(Event.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults()).toList();
        assertFalse(events.isEmpty());
      }
    }
  }

  public static class Event {
    private String id;
    private boolean happy;

    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public boolean isHappy() {
      return happy;
    }
    public void setHappy(boolean happy) {
      this.happy = happy;
    }

  }

}

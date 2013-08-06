package raven.tests.bugs.entities;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;

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

  //TODO: finish other tests

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

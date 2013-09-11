package raven.tests.linq;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentQueryCustomizationFactory;
import raven.client.document.DocumentStore;

public class LongCountTest extends RemoteClientTest {

  public static class TestDoc {
    private String name;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  @Test
  public void canQueryLongCount() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        TestDoc doc = new TestDoc();
        doc.setName("foo");
        session.store(doc);
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        long count = session.query(TestDoc.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults()).longCount();
        assertEquals(1L, count);
      }
    }
  }

}

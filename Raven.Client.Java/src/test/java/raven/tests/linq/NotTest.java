package raven.tests.linq;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;

import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;

public class NotTest extends RemoteClientTest {

  @QueryEntity
  public static class TestDoc {
    private String someProperty;

    public String getSomeProperty() {
      return someProperty;
    }

    public void setSomeProperty(String someProperty) {
      this.someProperty = someProperty;
    }
  }

  @Test
  public void canQueryWithNot() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        TestDoc doc = new TestDoc();
        doc.setSomeProperty("NOT");
        session.store(doc);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        QNotTest_TestDoc d = QNotTest_TestDoc.testDoc;
        assertNotNull(session.query(TestDoc.class).where(d.someProperty.eq("NOT")).firstOrDefault());
      }
    }
  }

  @Test
  public void canQueryWithOr() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        TestDoc doc = new TestDoc();
        doc.setSomeProperty("OR");
        session.store(doc);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        QNotTest_TestDoc d = QNotTest_TestDoc.testDoc;
        assertNotNull(session.query(TestDoc.class).where(d.someProperty.eq("OR")).firstOrDefault());
      }
    }
  }
  
  @Test
  public void canQueryWithAnd() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        TestDoc doc = new TestDoc();
        doc.setSomeProperty("AND");
        session.store(doc);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        QNotTest_TestDoc d = QNotTest_TestDoc.testDoc;
        assertNotNull(session.query(TestDoc.class).where(d.someProperty.eq("AND")).firstOrDefault());
      }
    }
  }

}

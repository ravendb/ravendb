package net.ravendb.tests.linq;

import static org.junit.Assert.assertNotNull;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.linq.QNotTest_TestDoc;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;


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

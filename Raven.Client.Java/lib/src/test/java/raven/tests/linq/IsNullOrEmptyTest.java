package raven.tests.linq;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;

import com.mysema.query.annotations.QueryEntity;

public class IsNullOrEmptyTest extends RemoteClientTest {
  @QueryEntity
  public static class TestDoc {

    public TestDoc() {
      super();
    }
    public TestDoc(String someProperty) {
      super();
      this.someProperty = someProperty;
    }
    private String someProperty;

    public String getSomeProperty() {
      return someProperty;
    }
    public void setSomeProperty(String someProperty) {
      this.someProperty = someProperty;
    }
  }

  @Test
  public void isNullOrEmptyEqTrue() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(new TestDoc("Has some content"));
        session.store(new TestDoc(""));
        session.store(new TestDoc(null));
        session.saveChanges();
      }
      QIsNullOrEmptyTest_TestDoc x = QIsNullOrEmptyTest_TestDoc.testDoc;
      try (IDocumentSession session = store.openSession()) {
        assertEquals(2, session.query(TestDoc.class).where(x.someProperty.isEmpty()).count());
      }
    }
  }

  @Test
  public void isNullOrEmptyEqFalse() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(new TestDoc("Has some content"));
        session.store(new TestDoc(""));
        session.store(new TestDoc(null));
        session.saveChanges();
      }
      QIsNullOrEmptyTest_TestDoc x = QIsNullOrEmptyTest_TestDoc.testDoc;
      try (IDocumentSession session = store.openSession()) {
        assertEquals(1, session.query(TestDoc.class).where(x.someProperty.isNotEmpty()).count());
      }
    }
  }


}

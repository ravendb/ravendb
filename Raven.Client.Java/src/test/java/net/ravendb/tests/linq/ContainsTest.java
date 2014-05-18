package net.ravendb.tests.linq;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.linq.QContainsTest_TestDoc;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;


public class ContainsTest extends RemoteClientTest {

  @QueryEntity
  public static class TestDoc {
    private String someProperty;
    private List<String> stringArray;

    public String getSomeProperty() {
      return someProperty;
    }
    public void setSomeProperty(String someProperty) {
      this.someProperty = someProperty;
    }
    public List<String> getStringArray() {
      return stringArray;
    }
    public void setStringArray(List<String> stringArray) {
      this.stringArray = stringArray;
    }
  }

  @Test
  public void canQueryArrayWithContains() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        TestDoc doc = new TestDoc();
        doc.setStringArray(Arrays.asList("test", "doc", "foo"));
        session.store(doc);
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        TestDoc otherDoc = new TestDoc();
        otherDoc.setSomeProperty("foo");

        QContainsTest_TestDoc x = QContainsTest_TestDoc.testDoc;
        TestDoc doc = session.query(TestDoc.class).where(x.stringArray.contains(otherDoc.someProperty)).firstOrDefault();
        assertNotNull(doc);
      }
    }
  }

  @Test
  public void doesNotSupportStrings() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        TestDoc doc = new TestDoc();
        doc.setSomeProperty("Ensure that contains on String is not supported.");
        session.store(doc);
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        TestDoc otherDoc = new TestDoc();
        otherDoc.setSomeProperty("Contains");

        QContainsTest_TestDoc x = QContainsTest_TestDoc.testDoc;
        try {
          session.query(TestDoc.class).where(x.someProperty.contains(otherDoc.someProperty)).toList();
          fail();
        } catch (IllegalStateException e) {
          //ok
          assertTrue(e.getMessage().contains("Contains is not supported, doing a substring match"));
        }

      }
    }
  }


}

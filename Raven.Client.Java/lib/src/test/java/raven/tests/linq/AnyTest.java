package raven.tests.linq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.junit.Test;

import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentQueryCustomizationFactory;
import raven.client.document.DocumentStore;
import raven.querydsl.RavenString;

import com.mysema.query.annotations.QueryEntity;
import com.mysema.query.types.expr.Param;

public class AnyTest extends RemoteClientTest {

  @QueryEntity
  public static class TestDoc  {
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
  public void canQueryArrayWithAny() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        TestDoc doc1 = new TestDoc();
        doc1.setStringArray(Arrays.asList("test2", "doc2", "foo2"));
        session.store(doc1);

        TestDoc doc2 = new TestDoc();
        doc2.setStringArray(Arrays.asList("doc", "foo"));
        session.store(doc2);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        TestDoc otherDoc = new TestDoc();
        otherDoc.setSomeProperty("foo");

        QAnyTest_TestDoc x = QAnyTest_TestDoc.testDoc;
        
        RavenString s = new RavenString("s");

        TestDoc doc = session.query(TestDoc.class).where(x.stringArray.any(s.eq(otherDoc.someProperty))).firstOrDefault();

        assertNotNull(doc);
        assertEquals(2, doc.stringArray.size());
      }
    }
  }

  @Test
  public void canCountWithAny() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        TestDoc doc1 = new TestDoc();
        doc1.setStringArray(Arrays.asList("one", "two"));
        session.store(doc1);

        TestDoc doc2 = new TestDoc();
        doc2.setStringArray(new ArrayList<String>());
        session.store(doc2);

        TestDoc doc3 = new TestDoc();
        doc3.setStringArray(new ArrayList<String>());
        session.store(doc3);

        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        QAnyTest_TestDoc x = QAnyTest_TestDoc.testDoc;
        assertEquals(1, session.query(TestDoc.class)
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .where(x.stringArray.isNotEmpty())
            .count());
      }
    }
  }

  @Test
  public void cannotCountNullArraysWithAnyIfThereIsNothingElseStoredInTheIndex() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        TestDoc doc1 = new TestDoc();
        doc1.setStringArray(Arrays.asList("one", "two"));
        session.store(doc1);

        TestDoc doc2 = new TestDoc();
        doc2.setStringArray(new ArrayList<String>());
        session.store(doc2);

        TestDoc doc3 = new TestDoc();
        doc3.setStringArray(new ArrayList<String>());
        session.store(doc3);

        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        QAnyTest_TestDoc x = QAnyTest_TestDoc.testDoc;
        assertEquals(0, session.query(TestDoc.class)
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .where(x.stringArray.isEmpty())
            .count());
      }
    }
  }

  @Test
  public void canCountNullArraysWithAnyIfHaveAnotherPropertyStoredInTheIndex() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        TestDoc doc1 = new TestDoc();
        doc1.setStringArray(Arrays.asList("one", "two"));
        doc1.setSomeProperty("Test");
        session.store(doc1);

        TestDoc doc2 = new TestDoc();
        doc2.setStringArray(new ArrayList<String>());
        doc2.setSomeProperty("Test");
        session.store(doc2);

        TestDoc doc3 = new TestDoc();
        doc3.setStringArray(new ArrayList<String>());
        doc3.setSomeProperty("Test");
        session.store(doc3);

        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        QAnyTest_TestDoc x = QAnyTest_TestDoc.testDoc;
        assertEquals(2, session.query(TestDoc.class)
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .where(x.stringArray.isEmpty().and(x.someProperty.eq("Test")))
            .count());
      }
    }
  }

  @QueryEntity
  public static class OrderableEntity {
    private Date order;

    public Date getOrder() {
      return order;
    }

    public void setOrder(Date order) {
      this.order = order;
    }

  }

  @Test
  public void nullRefWhenQuerying() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Date date = new Date();
        QAnyTest_OrderableEntity x = QAnyTest_OrderableEntity.orderableEntity;
        session.query(OrderableEntity.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .where(x.order.gt(date)).toList();

      }
    }
  }


}

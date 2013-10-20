package net.ravendb.tests.linq;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;


public class WhereStringEqualsTest extends RemoteClientTest {

  public static void setup(IDocumentStore store) throws Exception {
    try (IDocumentSession session = store.openSession()) {
      session.store(new MyEntity("Some data"));
      session.store(new MyEntity("Some DATA"));
      session.store(new MyEntity("Some other data"));
      session.saveChanges();
    }
  }

  @Test
  public void queryString_IgnoreCase_ShouldWork() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);

      QWhereStringEqualsTest_MyEntity x = QWhereStringEqualsTest_MyEntity.myEntity;

      try (IDocumentSession session = store.openSession()) {
        int count = session.query(MyEntity.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResultsAsOfNow())
          .count(x.stringData.equalsIgnoreCase("Some data"));

        assertEquals(2, count);
      }
    }
  }

  @Test
  public void queryString_WithoutSpecifyingTheComparisonType_ShouldJustWork() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);

      QWhereStringEqualsTest_MyEntity x = QWhereStringEqualsTest_MyEntity.myEntity;

      try (IDocumentSession session = store.openSession()) {
        int count = session.query(MyEntity.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResultsAsOfNow())
          .count(x.stringData.eq("Some data"));

        assertEquals(2, count);
      }
    }
  }

  @Test
  public void regularStringEqual_CaseSensitive_ShouldWork() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      setup(store);

      QWhereStringEqualsTest_MyEntity x = QWhereStringEqualsTest_MyEntity.myEntity;

      try (IDocumentSession session = store.openSession()) {
        int count = session.query(MyEntity.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResultsAsOfNow())
          .count(x.stringData.notEqualsIgnoreCase("Some data"));

        assertEquals(1, count);
      }
    }
  }



  @QueryEntity
  public static class MyEntity {
    private String stringData;


    public String getStringData() {
      return stringData;
    }


    public void setStringData(String stringData) {
      this.stringData = stringData;
    }

    public MyEntity(String stringData) {
      super();
      this.stringData = stringData;
    }

    public MyEntity() {
      super();
    }

  }
}

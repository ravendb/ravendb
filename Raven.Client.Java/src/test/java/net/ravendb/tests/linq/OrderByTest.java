package net.ravendb.tests.linq;

import static org.junit.Assert.assertEquals;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.linq.QOrderByTest_Section;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;


public class OrderByTest extends RemoteClientTest {

  @QueryEntity
  public static class Section {
    private String id;
    private int position;
    private String name;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public int getPosition() {
      return position;
    }

    public void setPosition(int position) {
      this.position = position;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Section() {

    }

    public Section(int position) {
      this.position = position;
      this.name = String.format("Position: %d", position);
    }
  }

  @Test
  public void canDescOrderBy_AProjection() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        for (int i = 0; i < 10; i++) {
          session.store(new Section(i));
          session.saveChanges();
        }
      }
      try (IDocumentSession session = store.openSession()) {
        QOrderByTest_Section x = QOrderByTest_Section.section;
        int lastPosition = session.query(Section.class)
          .orderBy(x.position.desc())
          .select(x.position)
          .firstOrDefault();

        assertEquals(9, lastPosition);
      }
    }
  }

  @Test
  public void canAscOrderBy_AProjection() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        for (int i = 5; i < 10; i++) {
          session.store(new Section(i));
          session.saveChanges();
        }
      }
      try (IDocumentSession session = store.openSession()) {
        for (int i = 4; i >=0; i--) {
          session.store(new Section(i));
          session.saveChanges();
        }
      }

      try (IDocumentSession session = store.openSession()) {
        QOrderByTest_Section x = QOrderByTest_Section.section;
        int lastPosition = session.query(Section.class)
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .orderBy(x.position.asc())
          .select(x.position)
          .firstOrDefault();

        assertEquals(0, lastPosition);
      }
    }
  }



}

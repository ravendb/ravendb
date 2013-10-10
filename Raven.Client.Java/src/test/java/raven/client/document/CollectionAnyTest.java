package raven.client.document;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.linq.IRavenQueryable;
import raven.client.linq.RavenQueryInspector;
import raven.querydsl.RavenString;
import raven.samples.entities.QCompany;
import raven.samples.entities.QEmployee;
import raven.tests.linq.WhereClauseTest.IndexedUser;

public class CollectionAnyTest {


  private IDocumentStore store;
  private IDocumentSession session;


  public CollectionAnyTest() {
    store = new DocumentStore("http://fake");
    store.initialize();
    session = store.openSession();
  }

  private RavenQueryInspector<IndexedUser> getRavenQueryInspector() {
    return (RavenQueryInspector<IndexedUser>) session.query(IndexedUser.class);
  }

  @Test
  public void canHandleMultipleAny() {
    QCompany c = QCompany.company;
    QEmployee e = QEmployee.employee;
    RavenString s = new RavenString("s");
    RavenQueryInspector<IndexedUser> queryInspector = getRavenQueryInspector();
    IRavenQueryable<IndexedUser> q = queryInspector.where(c.employees.any(e.specialties.any(s.eq("C#"))));
    assertEquals("Employees,Specialties:C#", q.toString());


  }

}

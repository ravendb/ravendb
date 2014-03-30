package net.ravendb.client.document;

import static org.junit.Assert.assertEquals;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.linq.IRavenQueryable;
import net.ravendb.client.linq.RavenQueryInspector;
import net.ravendb.querydsl.RavenString;
import net.ravendb.samples.entities.QCompany;
import net.ravendb.samples.entities.QEmployee;
import net.ravendb.tests.linq.WhereClauseTest.IndexedUser;

import org.junit.Test;


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

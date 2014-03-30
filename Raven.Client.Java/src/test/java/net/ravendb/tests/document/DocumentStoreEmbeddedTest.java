package net.ravendb.tests.document;

import static org.junit.Assert.assertEquals;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;

import org.junit.Test;


public class DocumentStoreEmbeddedTest extends RemoteClientTest {

  @Test
  public void canRefreshEntityFromDatabase() throws Exception {
    Company company = new Company();
    company.setName("Company Name");
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(company);
        session.saveChanges();

        try (IDocumentSession session2 = store.openSession()) {
          Company company2 = session2.load(Company.class, company.getId());
          company2.setName("Hibernating Rhinos");
          session2.store(company2);
          session2.saveChanges();
        }

        session.advanced().refresh(company);
        assertEquals("Hibernating Rhinos", company.getName());

      }
    }
  }

}

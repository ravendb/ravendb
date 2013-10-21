package net.ravendb.tests.document;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;

import org.junit.Test;


public class TotalCountServerTest extends RemoteClientTest {
  @Test
  public void totalResultIsIncludedInQueryResult() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company company1 = new Company();
        company1.setName("Company1");
        company1.setAddress1("");
        company1.setAddress2("");
        company1.setAddress3("");
        company1.setContacts(new ArrayList<Contact>());
        company1.setPhone(2);

        Company company2 = new Company();
        company2.setName("Company2");
        company2.setAddress1("");
        company2.setAddress2("");
        company2.setAddress3("");
        company2.setContacts(new ArrayList<Contact>());
        company2.setPhone(2);

        session.store(company1);
        session.store(company2);
        session.saveChanges();

      }

      try (IDocumentSession session = store.openSession()) {
        int resultCount = session.advanced().luceneQuery(Company.class).waitForNonStaleResults().getQueryResult().getTotalResults();
        assertEquals(2, resultCount);
      }
    }

  }
}

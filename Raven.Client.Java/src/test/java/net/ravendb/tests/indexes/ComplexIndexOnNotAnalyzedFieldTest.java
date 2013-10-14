package net.ravendb.tests.indexes;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import net.ravendb.abstractions.data.IndexQuery;
import net.ravendb.abstractions.data.QueryResult;
import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;


public class ComplexIndexOnNotAnalyzedFieldTest extends RemoteClientTest {

  @Test
  public void canQueryOnKey() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {

        store.getDatabaseCommands().put("companies/", null, RavenJObject.parse("{'Name':'Hibernating Rhinos', 'Partners': ['companies/49', 'companies/50']}"),
          RavenJObject.parse("{'Raven-Entity-Name': 'Companies'}"));
        IndexDefinition indexDefinition = new IndexDefinition();
        indexDefinition.setMap("from company in docs.Companies from partner in company.Partners select new { Partner = partner }");
        store.getDatabaseCommands().putIndex("CompaniesByPartners", indexDefinition);

        IndexQuery query  =new IndexQuery();
        query.setQuery("Partner:companies/49");
        query.setPageSize(10);

        QueryResult queryResult = null;
        do {
          queryResult = store.getDatabaseCommands().query("CompaniesByPartners", query, new String[0]);
        } while (queryResult.isStale());

        assertEquals("Hibernating Rhinos", queryResult.getResults().get(0).value(String.class, "Name"));

      }
    }
  }

}

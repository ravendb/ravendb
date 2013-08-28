package raven.tests.document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.List;

import org.junit.Test;

import raven.abstractions.closure.Action1;
import raven.abstractions.data.Constants;
import raven.abstractions.data.IndexQuery;
import raven.abstractions.indexing.FieldIndexing;
import raven.abstractions.indexing.FieldStorage;
import raven.abstractions.indexing.IndexDefinition;
import raven.client.IDocumentQueryCustomization;
import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;
import raven.client.indexes.IndexDefinitionBuilder;

public class DocumentStoreServerTest extends RemoteClientTest {

  @Test
  public void should_insert_into_db_and_set_id() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company entity = new Company();
        entity.setName("Company");

        session.store(entity);
        session.saveChanges();
        assertNotEquals(Constants.EMPTY_UUID, entity.getId());

      }
    }
  }

  @Test
  public void can_get_index_names() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      Collection<String> indexNames = store.getDatabaseCommands().getIndexNames(0, 25);
      assertTrue("Raven/DocumentsByEntityName", indexNames.contains("Raven/DocumentsByEntityName"));
    }
  }

  @Test
  public void can_get_index_def() throws Exception {
    QCompany c = QCompany.company;
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      IndexDefinitionBuilder definitionBuilder = new IndexDefinitionBuilder();
      definitionBuilder.setMap("docs.Companies.Select(c => new { Name = c.Name})");
      definitionBuilder.getIndexes().put(c.name, FieldIndexing.NOT_ANALYZED);
      store.getDatabaseCommands().putIndex("Companies/Name", definitionBuilder);

      IndexDefinition indexDefinition = store.getDatabaseCommands().getIndex("Companies/Name");
      assertEquals("docs.Companies.Select(c => new { Name = c.Name})", indexDefinition.getMap());
      assertEquals(FieldIndexing.NOT_ANALYZED, indexDefinition.getIndexes().get("Name"));
    }
  }

  @Test
  public void can_get_indexes() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      IndexDefinitionBuilder definitionBuilder = new IndexDefinitionBuilder();

      QCompany c = new QCompany("c");
      definitionBuilder.setMap("from c in docs.Companies select new { c.Name}");
      definitionBuilder.getIndexes().put(c.name, FieldIndexing.NOT_ANALYZED);
      store.getDatabaseCommands().putIndex("Companies/Name", definitionBuilder);

      Collection<IndexDefinition> indexDefinitions = store.getDatabaseCommands().getIndexes(0, 10);
      for (IndexDefinition indexDef : indexDefinitions) {
        if (indexDef.getName().equals("Companies/Name")) {
          return ;// test finished ok
        }
      }
      fail("Unable to find Companies/Name index");
    }
  }

  @Test
  public void can_delete_by_index() throws Exception {
    Company entity = new Company();
    entity.setName("Company");

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(entity);
        session.saveChanges();

        session.advanced().luceneQuery(Company.class).waitForNonStaleResults().toList(); // wait for the index to settle down
      }

      IndexQuery query = new IndexQuery();
      query.setQuery("Tag:[[Companies]]");
      store.getDatabaseCommands().deleteByIndex("Raven/DocumentsByEntityName", query, false).waitForCompletion();

      try (IDocumentSession session = store.openSession()) {
        assertEquals(0, session.advanced().luceneQuery(Company.class).waitForNonStaleResults().toList().size());
      }
    }
  }

  @Test
  public void can_order_by_using_linq() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from company in docs.Companies select new { company.Name, company.Phone }");
      store.getDatabaseCommands().putIndex("CompaniesByName", indexDefinition);

      try (IDocumentSession session = store.openSession()) {
        Company c1 = new Company();
        c1.setName("A");
        c1.setPhone(4);
        Company c2 = new Company();
        c2.setName("B");
        c2.setPhone(3);
        Company c3 = new Company();
        c3.setName("B");
        c3.setPhone(4);
        session.store(c1);
        session.store(c2);
        session.store(c3);
        session.saveChanges();

        session.advanced().luceneQuery(Company.class, "CompaniesByName").waitForNonStaleResults().toList(); // wait for the index to settle down
      }

      QCompany c = QCompany.company;

      try (IDocumentSession session = store.openSession()) {
         List<Company> companies = session.query(Company.class, "CompaniesByName").orderBy(c.name.desc()).toList();
         assertEquals("B", companies.get(0).getName());
         assertEquals("B", companies.get(1).getName());
         assertEquals("A", companies.get(2).getName());

         companies = session.query(Company.class, "CompaniesByName").orderBy(c.name.asc()).toList();
         assertEquals("A", companies.get(0).getName());
         assertEquals("B", companies.get(1).getName());
         assertEquals("B", companies.get(2).getName());

         companies = session.query(Company.class, "CompaniesByName").orderBy(c.phone.asc()).toList();
         assertEquals(3, companies.get(0).getPhone());
         assertEquals(4, companies.get(1).getPhone());
         assertEquals(4, companies.get(2).getPhone());

         companies = session.query(Company.class, "CompaniesByName").orderBy(c.phone.asc(), c.name.asc()).toList();
         assertEquals(3, companies.get(0).getPhone());
         assertEquals(4, companies.get(1).getPhone());
         assertEquals(4, companies.get(2).getPhone());
         assertEquals("B", companies.get(0).getName());
         assertEquals("A", companies.get(1).getName());
         assertEquals("B", companies.get(2).getName());


      }
    }
  }

  @Test
  public void can_create_index_with_decimal_as_firstfield() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company company = new Company();
        company.setName("Company 1");
        company.setPhone(5);
        company.setAccountsReceivable(3904.39);
        session.store(company);
        session.saveChanges();

        IndexDefinition definition = new IndexDefinition();
        definition.setMap("from doc in docs where doc.Name != null select new { doc.AccountsReceivable, doc.Name}");
        definition.getStores().put("Name", FieldStorage.YES);
        definition.getStores().put("AccountsReceivable", FieldStorage.YES);
        store.getDatabaseCommands().putIndex("company_by_name", definition);

        List<Company> list = session.query(Company.class, "company_by_name").customize(new Action1<IDocumentQueryCustomization>() {

          @Override
          public void apply(IDocumentQueryCustomization c) {
            c.waitForNonStaleResults(3600 * 1000);
          }
        }).toList();
        Company single = list.get(0);
        assertNotNull(single);
        assertEquals("Company 1", single.getName());
        assertEquals(3904.39, single.getAccountsReceivable(), 0.001);

      }
    }
  }


  //TODO: finish me
}

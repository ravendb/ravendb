package raven.tests.document;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Test;

import raven.abstractions.commands.DeleteCommandData;
import raven.abstractions.commands.ICommandData;
import raven.abstractions.commands.PutCommandData;
import raven.abstractions.data.BatchResult;
import raven.abstractions.data.Constants;
import raven.abstractions.data.DatabaseStatistics;
import raven.abstractions.data.IndexQuery;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.data.JsonDocumentMetadata;
import raven.abstractions.data.PatchCommandType;
import raven.abstractions.data.PatchRequest;
import raven.abstractions.exceptions.ConcurrencyException;
import raven.abstractions.indexing.FieldIndexing;
import raven.abstractions.indexing.FieldStorage;
import raven.abstractions.indexing.IndexDefinition;
import raven.abstractions.json.linq.RavenJArray;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJToken;
import raven.abstractions.json.linq.RavenJValue;
import raven.client.IDocumentQuery;
import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentQueryCustomizationFactory;
import raven.client.document.DocumentStore;
import raven.client.indexes.IndexDefinitionBuilder;
import raven.client.linq.IRavenQueryable;
import raven.tests.document.Company.CompanyType;

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

        List<Company> list = session.query(Company.class, "company_by_name")
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults(3600 * 1000))
            .toList();
        Company single = list.get(0);
        assertNotNull(single);
        assertEquals("Company 1", single.getName());
        assertEquals(3904.39, single.getAccountsReceivable(), 0.001);

      }
    }
  }
  // Can_select_from_index_using_linq_method_chain_using_decimal_and_greater_than_or_equal
  @Test
  public void test_select_gte() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company company = new Company();
        company.setName("Company 1");
        company.setPhone(5);
        company.setAccountsReceivable(3904.39);
        session.store(company);
        session.saveChanges();

        IndexDefinition definition = new IndexDefinition();
        definition.setMap("from doc in docs where doc.Name != null select new { doc.Name, doc.AccountsReceivable}");
        definition.getStores().put("Name", FieldStorage.YES);
        definition.getStores().put("AccountsReceivable", FieldStorage.YES);
        store.getDatabaseCommands().putIndex("company_by_name", definition);


        QCompany c = QCompany.company;
        List<Company> list = session.query(Company.class, "company_by_name")
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults(3600 * 1000))
            .where(c.accountsReceivable.gt(1)).toList();
        Company single = list.get(0);
        assertNotNull(single);
        assertEquals("Company 1", single.getName());
        assertEquals(3904.39, single.getAccountsReceivable(), 0.001);

      }
    }
  }

  @Test
  public void can_create_index_with_decimal_as_lastfield() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company company = new Company();
        company.setName("Company 1");
        company.setPhone(5);
        company.setAccountsReceivable(3904.39);
        session.store(company);
        session.saveChanges();

        IndexDefinition definition = new IndexDefinition();
        definition.setMap("from doc in docs where doc.Name != null select new {  doc.Name, doc.AccountsReceivable }");
        definition.getStores().put("Name", FieldStorage.YES);
        definition.getStores().put("AccountsReceivable", FieldStorage.YES);
        store.getDatabaseCommands().putIndex("company_by_name", definition);

        List<Company> list = session.query(Company.class, "company_by_name")
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults(3600 * 1000))
            .toList();
        Company single = list.get(0);
        assertNotNull(single);
        assertEquals("Company 1", single.getName());
        assertEquals(3904.39, single.getAccountsReceivable(), 0.001);

      }
    }
  }

  @Test
  public void can_update_by_index() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      Company entity = new Company();
      entity.setName("Company");

      try (IDocumentSession session = store.openSession()) {

        session.store(entity);
        session.saveChanges();

        session.advanced().luceneQuery(Company.class).waitForNonStaleResults().toList();  // wait for the index to settle down
      }

      IndexQuery query = new IndexQuery();
      query.setQuery("Tag:[[Companies]]");
      PatchRequest patchRequest = new PatchRequest();
      patchRequest.setType(PatchCommandType.SET);
      patchRequest.setName("Name");
      patchRequest.setValue(RavenJToken.fromObject("Another Company"));

      store.getDatabaseCommands().updateByIndex("Raven/DocumentsByEntityName", query, new PatchRequest[] { patchRequest } , false ).waitForCompletion();

      try (IDocumentSession session = store.openSession()) {
        assertEquals("Another Company", session.load(Company.class, entity.getId()).getName());
      }
    }

  }

  @Test
  public void can_specify_cutoff_using_server() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      IndexQuery query = new IndexQuery();
      query.setPageSize(10);
      query.setCutoff(DateUtils.addHours(new Date(), -1));
      store.getDatabaseCommands().query("Raven/DocumentsByEntityName", query, null);
    }
  }

  @Test
  public void can_read_projected_dates() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from doc in docs select new { doc.Date}");
      indexDefinition.getStores().put("Date", FieldStorage.YES);

      store.getDatabaseCommands().putIndex("my_index", indexDefinition);

      try (IDocumentSession session = store.openSession()) {
        RavenJObject dateObject = new RavenJObject();
        Calendar cal = Calendar.getInstance();
        cal.set(2000, 0, 1);

        dateObject.add("Date", new RavenJValue(DateUtils.truncate(cal.getTime(), Calendar.DAY_OF_MONTH)));
        session.store(dateObject);
        session.saveChanges();
      }

      Calendar cal = Calendar.getInstance();
      cal.set(2000, 0, 1);
      Date date = DateUtils.truncate(cal.getTime(), Calendar.DAY_OF_MONTH);

      try (IDocumentSession session = store.openSession()) {
        IDocumentQuery<DateHolder> query = session.advanced().luceneQuery(Object.class, "my_index").selectFields(DateHolder.class, "Date").waitForNonStaleResults();
        DateHolder dateHolder = query.toList().get(0);
        assertEquals(date , dateHolder.getDate());
      }
    }
  }

  public static class DateHolder {
    private Date date;

    public Date getDate() {
      return date;
    }

    public void setDate(Date date) {
      this.date = date;
    }

  }

  @Test
  public void can_query_using_special_characters() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from doc in docs select new { doc.Language, doc.Type}");
      indexDefinition.getStores().put("Name", FieldStorage.YES);
      indexDefinition.getStores().put("Phone", FieldStorage.YES);

      store.getDatabaseCommands().putIndex("my_index", indexDefinition);

      try (IDocumentSession session = store.openSession()) {
        RavenJObject object1 = new RavenJObject();
        object1.add("Language", new RavenJValue("Fran�ais")); //Note the �
        object1.add("Type", new RavenJValue("Feats"));

        session.store(object1);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        IDocumentQuery<RavenJObject> query = session.advanced().luceneQuery(RavenJObject.class, "my_index").where("Type:Feats AND Language:Fran�ais").waitForNonStaleResults();
        query.toList();

        assertEquals(1, query.getQueryResult().getTotalResults());
      }
    }
  }

  @Test
  public void can_query_indexes_returning_complex_objects() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      IndexDefinition indexDefinition = new IndexDefinition();;
      indexDefinition.setMap("from doc in docs select new { doc.Language, doc.Type, Value = new{ Answers = 42, Paths = 7 }  }");
      indexDefinition.getStores().put("Value", FieldStorage.YES);

      store.getDatabaseCommands().putIndex("my_index", indexDefinition);

      try (IDocumentSession session = store.openSession()) {
        RavenJObject object1 = new RavenJObject();
        object1.add("Language", new RavenJValue("Fran�ais")); //Note the �
        object1.add("Type", new RavenJValue("Feats"));

        session.store(object1);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        IDocumentQuery<RavenJObject> query = session.advanced().luceneQuery(RavenJObject.class, "my_index")
            .where("Type:Feats AND Language:Fran�ais")
            .selectFields(RavenJObject.class, "Value")
            .waitForNonStaleResults();
        RavenJObject first = query.first();
        assertEquals(Integer.valueOf(42), first.value(RavenJObject.class, "Value").value(int.class, "Answers"));
        assertEquals(Integer.valueOf(7), first.value(RavenJObject.class, "Value").value(int.class, "Paths"));
      }
    }
  }

  @Test
  public void requesting_stats() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        DatabaseStatistics databaseStatistics = session.load(DatabaseStatistics.class, "stats");
        assertNull(databaseStatistics);
      }
    }
  }

  @Test
  public void can_get_entity_back_with_enum() throws Exception {
    Company company = new Company();
    company.setName("Company Name");
    company.setType(CompanyType.PRIVATE);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(company);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        Company companyFound = session.load(Company.class, company.getId());
        assertEquals(companyFound.getType(), company.getType());
      }
    }
  }

  @Test
  public void can_store_and_get_array_metadata() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company company = new Company();
        company.setName("Company");
        session.store(company);

        RavenJObject metadata = session.advanced().getMetadataFor(company);
        metadata.add("Raven-Allowed-Users", new RavenJArray(Arrays.asList("ayende", "oren", "rob")));

        session.saveChanges();
        RavenJObject metadataFromServer = session.advanced().getMetadataFor(session.load(Company.class, company.getId()));
        List<String> values = metadataFromServer.value(RavenJArray.class, "Raven-Allowed-Users").values(String.class);
        assertArrayEquals(values.toArray(new String[0]), new String[] { "ayende", "oren", "rob" });

      }
    }
  }

  @Test
  public void can_store_using_batch() throws Exception {
    Company c1 = new Company();
    c1.setName("Hibernating Rhinos");

    PutCommandData put1 = new PutCommandData("rhino1", null, RavenJObject.fromObject(c1), new RavenJObject());

    Company c2 = new Company();
    c2.setName("Hibernating Rhinos");
    PutCommandData put2 = new PutCommandData("rhino2", null, RavenJObject.fromObject(c2), new RavenJObject());

    DeleteCommandData del = new DeleteCommandData("rhino2", null);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      BatchResult[] batchResults = store.getDatabaseCommands().batch(Arrays.asList(put1, put2, del));

      assertEquals("rhino1", batchResults[0].getKey());
      assertEquals("rhino2", batchResults[1].getKey());

      assertNull(store.getDatabaseCommands().get("rhino2"));
      assertNotNull(store.getDatabaseCommands().get("rhino1"));
    }

  }

  @Test
  public void can_get_document_metadata() throws Exception {

    Company c1 = new Company();
    c1.setName("Hibernating Rhinos");

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.getDatabaseCommands().put("rhino1", null, RavenJObject.fromObject(c1), new RavenJObject());

      JsonDocument doc = store.getDatabaseCommands().get("rhino1");
      JsonDocumentMetadata meta = store.getDatabaseCommands().head("rhino1");

      assertNotNull(meta);
      assertEquals(doc.getKey(), meta.getKey());
      assertEquals(doc.getEtag(), meta.getEtag());
      assertEquals(doc.getLastModified(), meta.getLastModified());

    }
  }

  @Test
  public void when_document_does_not_exist_Then_metadata_should_be_null() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      assertNull(store.getDatabaseCommands().head("rhino1"));
    }
  }

  @Test
  public void can_defer_commands_until_savechanges() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company c1 = new Company();
        c1.setName("Hibernating Rhinos");

        PutCommandData put1 = new PutCommandData("rhino1", null, RavenJObject.fromObject(c1), new RavenJObject());

        Company c2 = new Company();
        c2.setName("Hibernating Rhinos");
        PutCommandData put2 = new PutCommandData("rhino2", null, RavenJObject.fromObject(c2), new RavenJObject());

        ICommandData[] commands = new ICommandData[] { put1, put2 };

        session.advanced().defer(commands);
        session.advanced().defer(new DeleteCommandData("rhino2", null));

        Company c3 = new Company();
        c3.setId("rhino3");
        c3.setName("Hibernating Rhinos");

        session.store(c3);

        assertEquals(0, session.advanced().getNumberOfRequests());

        session.saveChanges();
        assertEquals(1, session.advanced().getNumberOfRequests());

        // make sure that session is empty
        session.saveChanges();
        assertEquals(1, session.advanced().getNumberOfRequests());
      }

      assertNull(store.getDatabaseCommands().get("rhino2"));
      assertNotNull(store.getDatabaseCommands().get("rhino1"));
      assertNotNull(store.getDatabaseCommands().get("rhino3"));
    }
  }

  @Test
  public void can_get_two_documents_in_one_call() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(new Company("1", "Company A"));
        session.store(new Company("2", "Company B"));
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        Company[] companies = session.load(Company.class, "1", "2");
        assertEquals(2, companies.length);
        assertEquals("Company A", companies[0].getName());
        assertEquals("Company B", companies[1].getName());
      }
    }
  }

  @Test
  public void can_get_documents() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(new Company("1", "Company A"));
        session.store(new Company("2", "Company B"));
        session.saveChanges();
      }

      List<JsonDocument> jsonDocuments = store.getDatabaseCommands().getDocuments(0, 10, true);
      assertEquals(2, jsonDocuments.size());
    }
  }

  @Test
  public void can_delete_document() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company entity = new Company(null, "Company");
        session.store(entity);
        session.saveChanges();

        try (IDocumentSession session2 = store.openSession()) {
          assertNotNull(session2.load(Company.class, entity.getId()));
        }

        session.delete(entity);
        session.saveChanges();

        try (IDocumentSession session3 = store.openSession()) {
          assertNull(session3.load(Company.class, entity.getId()));
        }
      }
    }
  }

  @Test
  public void can_project_from_index() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company company = new Company(null, "Company 1");
        company.setPhone(5);
        session.store(company);
        session.saveChanges();

        IndexDefinition indexDefinition = new IndexDefinition();
        indexDefinition.setMap("from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}");
        indexDefinition.getStores().put("Name", FieldStorage.YES);
        indexDefinition.getStores().put("Phone", FieldStorage.YES);
        indexDefinition.getIndexes().put("Name", FieldIndexing.NOT_ANALYZED);

        store.getDatabaseCommands().putIndex("company_by_name", indexDefinition);

        IDocumentQuery<Company> q = session.advanced().luceneQuery(Company.class, "company_by_name").
          selectFields(Company.class, "Name", "Phone").waitForNonStaleResults();

        Company single = q.single();
        assertEquals("Company 1", single.getName());
        assertEquals(5, single.getPhone());
      }
    }
  }

  @Test
  public void can_select_from_index_using_linq_method_chain() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company company = new Company(null, "Company 1");
        company.setPhone(5);
        session.store(company);
        session.saveChanges();

        IndexDefinition indexDefinition = new IndexDefinition();
        indexDefinition.setMap("from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}");
        indexDefinition.getStores().put("Name", FieldStorage.YES);
        indexDefinition.getStores().put("Phone", FieldStorage.YES);

        store.getDatabaseCommands().putIndex("company_by_name", indexDefinition);

        QCompany c = QCompany.company;

        IRavenQueryable<Company> q = session.query(Company.class, "company_by_name")
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .where(c.name.eq(company.getName()));

        Company single = q.toList().get(0);
        assertEquals("Company 1", single.getName());
        assertEquals(5, single.getPhone());
      }
    }
  }

  @Test
  public void can_select_from_index_using_linq_method_chain_with_variable() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company company = new Company(null, "Company 1");
        company.setPhone(5);
        session.store(company);
        session.saveChanges();

        IndexDefinition indexDefinition = new IndexDefinition();
        indexDefinition.setMap("from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}");
        indexDefinition.getStores().put("Name", FieldStorage.YES);
        indexDefinition.getStores().put("Phone", FieldStorage.YES);

        store.getDatabaseCommands().putIndex("company_by_name", indexDefinition);

        QCompany c = QCompany.company;

        String name = company.getName();

        IRavenQueryable<Company> q = session.query(Company.class, "company_by_name")
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .where(c.name.eq(name));

        Company single = q.toList().get(0);
        assertEquals("Company 1", single.getName());
        assertEquals(5, single.getPhone());
      }
    }
  }

  @Test
  public void can_select_from_index_using_linq_with_no_where_clause() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company company = new Company(null, "Company 1");
        company.setPhone(5);
        session.store(company);
        session.saveChanges();

        IndexDefinition indexDefinition = new IndexDefinition();
        indexDefinition.setMap("from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}");
        indexDefinition.getStores().put("Name", FieldStorage.YES);
        indexDefinition.getStores().put("Phone", FieldStorage.YES);

        store.getDatabaseCommands().putIndex("company_by_name", indexDefinition);

        IRavenQueryable<Company> q = session.query(Company.class, "company_by_name")
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults());
        Company single = q.toList().get(0);
        assertEquals("Company 1", single.getName());
        assertEquals(5, single.getPhone());
      }
    }
  }

  @Test
  public void can_select_from_index_using_linq_method_chain_using_integer() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company company = new Company(null, "Company 1");
        company.setPhone(5);
        session.store(company);
        session.saveChanges();

        IndexDefinition indexDefinition = new IndexDefinition();
        indexDefinition.setMap("from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}");
        indexDefinition.getStores().put("Name", FieldStorage.YES);
        indexDefinition.getStores().put("Phone", FieldStorage.YES);

        store.getDatabaseCommands().putIndex("company_by_name", indexDefinition);

        QCompany c = QCompany.company;

        IRavenQueryable<Company> q = session.query(Company.class, "company_by_name")
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .where(c.phone.eq(5));

        Company single = q.toList().get(0);
        assertEquals("Company 1", single.getName());
        assertEquals(5, single.getPhone());
      }
    }
  }

  //Can_select_from_index_using_linq_method_chain_using_integer_and_greater_than_or_equal
  @Test
  public void can_select_from_index_using_linq_method_chain_using_integer_and_gte() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company company = new Company(null, "Company 1");
        company.setPhone(5);
        session.store(company);
        session.saveChanges();

        IndexDefinition indexDefinition = new IndexDefinition();
        indexDefinition.setMap("from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}");
        indexDefinition.getStores().put("Name", FieldStorage.YES);
        indexDefinition.getStores().put("Phone", FieldStorage.YES);

        store.getDatabaseCommands().putIndex("company_by_name", indexDefinition);

        QCompany c = QCompany.company;

        IRavenQueryable<Company> q = session.query(Company.class, "company_by_name")
            .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
            .where(c.phone.gt(1));

        Company single = q.toList().get(0);
        assertEquals("Company 1", single.getName());
        assertEquals(5, single.getPhone());
      }
    }
  }

  @Test(expected = ConcurrencyException.class)
  public void optimistic_concurrency() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.advanced().setUseOptimisticConcurrency(true);
        Company company = new Company(null, "Company 1");
        session.store(company);
        session.saveChanges();

        try (IDocumentSession session2 = store.openSession()) {
          Company company2 = session2.load(Company.class, company.getId());
          company2.setName("foo");
          session2.saveChanges();
        }

        company.setName("Company 2");
        session.saveChanges();
      }
    }
  }


  //TODO: finish me
}

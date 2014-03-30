package net.ravendb.tests.document;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import net.ravendb.abstractions.commands.DeleteCommandData;
import net.ravendb.abstractions.commands.ICommandData;
import net.ravendb.abstractions.commands.PutCommandData;
import net.ravendb.abstractions.data.Attachment;
import net.ravendb.abstractions.data.BatchResult;
import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.data.DatabaseStatistics;
import net.ravendb.abstractions.data.IndexQuery;
import net.ravendb.abstractions.data.JsonDocument;
import net.ravendb.abstractions.data.JsonDocumentMetadata;
import net.ravendb.abstractions.data.PatchCommandType;
import net.ravendb.abstractions.data.PatchRequest;
import net.ravendb.abstractions.exceptions.ConcurrencyException;
import net.ravendb.abstractions.exceptions.DocumentDoesNotExistsException;
import net.ravendb.abstractions.indexing.FieldIndexing;
import net.ravendb.abstractions.indexing.FieldStorage;
import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.abstractions.indexing.SortOptions;
import net.ravendb.abstractions.json.linq.RavenJArray;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.abstractions.json.linq.RavenJValue;
import net.ravendb.client.IDocumentQuery;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.IndexDefinitionBuilder;
import net.ravendb.client.linq.IRavenQueryable;
import net.ravendb.tests.document.QCompany;
import net.ravendb.tests.document.Company.CompanyType;
import net.ravendb.tests.indexes.LinqIndexesFromClient;
import net.ravendb.tests.indexes.QLinqIndexesFromClient_LocationAge;
import net.ravendb.tests.indexes.QLinqIndexesFromClient_User;
import net.ravendb.tests.indexes.LinqIndexesFromClient.LocationAge;
import net.ravendb.tests.indexes.LinqIndexesFromClient.LocationCount;
import net.ravendb.tests.indexes.LinqIndexesFromClient.User;
import net.ravendb.tests.spatial.Event;
import net.ravendb.tests.spatial.SpatialIndexTest;
import net.ravendb.tests.spatial.SpatialIndexTestHelper;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Test;


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

        session.advanced().documentQuery(Company.class).waitForNonStaleResults().toList(); // wait for the index to settle down
      }

      IndexQuery query = new IndexQuery();
      query.setQuery("Tag:[[Companies]]");
      store.getDatabaseCommands().deleteByIndex("Raven/DocumentsByEntityName", query, false).waitForCompletion();

      try (IDocumentSession session = store.openSession()) {
        assertEquals(0, session.advanced().documentQuery(Company.class).waitForNonStaleResults().toList().size());
      }
    }
  }

  @Test
  public void can_order_by_using_linq() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from company in docs.Companies select new { company.Name, company.Phone }");
      indexDefinition.getSortOptions().put("Phone", SortOptions.INT);
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

        session.advanced().documentQuery(Company.class, "CompaniesByName").waitForNonStaleResults().toList(); // wait for the index to settle down
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

        session.advanced().documentQuery(Company.class).waitForNonStaleResults().toList();  // wait for the index to settle down
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
        IDocumentQuery<DateHolder> query = session.advanced().documentQuery(Object.class, "my_index").selectFields(DateHolder.class, "Date").waitForNonStaleResults();
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

      //store.getJsonRequestFactory().setDisableRequestCompression(true);

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
        IDocumentQuery<RavenJObject> query = session.advanced().documentQuery(RavenJObject.class, "my_index").where("Type:Feats AND Language:Fran�ais").waitForNonStaleResults();
        query.toList();

        assertEquals(1, query.getQueryResult().getTotalResults());
      }
    }
  }

  @Test
  public void can_query_indexes_returning_complex_objects() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      IndexDefinition indexDefinition = new IndexDefinition();
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
        IDocumentQuery<RavenJObject> query = session.advanced().documentQuery(RavenJObject.class, "my_index")
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

        IDocumentQuery<Company> q = session.advanced().documentQuery(Company.class, "company_by_name").
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

  @Test
  public void should_update_stored_entity() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company company = new Company(null, "Company 1");
        session.store(company);
        session.saveChanges();

        String id = company.getId();
        company.setName("Company 2");
        session.saveChanges();

        Company companyFound = session.load(Company.class, company.getId());
        assertEquals("Company 2", companyFound.getName());
        assertEquals(id, company.getId());
      }
    }
  }

  @Test
  public void should_update_retrieved_entity() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company company = new Company(null, "Company 1");
        session.store(company);
        session.saveChanges();

        String companyId = company.getId();
        try (IDocumentSession session2 = store.openSession()) {
          Company companyFound = session2.load(Company.class, companyId);
          companyFound.setName("New Name");
          session2.saveChanges();
          assertEquals("New Name", session2.load(Company.class, companyId).getName());
        }

      }
    }
  }

  @Test
  public void should_retrieve_all_entities() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(new Company(null, "Company 1"));
        session.store(new Company(null, "Company 2"));

        session.saveChanges();

        try (IDocumentSession session2 = store.openSession()) {
          List<Company> companyFound = session2.advanced().documentQuery(Company.class).waitForNonStaleResults().toList();
          assertEquals(2, companyFound.size());
        }


      }
    }
  }

  @Test
  public void can_sort_from_index() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        Company c1 = new Company();
        c1.setName("Company 1");
        c1.setPhone(5);

        Company c2 = new Company();
        c2.setName("Company 2");
        c2.setPhone(3);

        session.store(c1);
        session.store(c2);
        session.saveChanges();

        IndexDefinition indexDefinition = new IndexDefinition();
        indexDefinition.setMap("from doc in docs where doc.Name != null select new { doc.Name, doc.Phone}");
        indexDefinition.getIndexes().put("Phone", FieldIndexing.ANALYZED);

        store.getDatabaseCommands().putIndex("company_by_name", indexDefinition);

        // wait unit the index is build
        session.advanced().documentQuery(Company.class, "company_by_name").waitForNonStaleResults().toList();

        List<Company> companies = session.advanced().documentQuery(Company.class, "company_by_name").
            orderBy("Phone").waitForNonStaleResults().toList();

        assertEquals("Company 2", companies.get(0).getName());
        assertEquals("Company 1", companies.get(1).getName());

      }
    }
  }

  @Test
  public void can_query_from_spatial_index() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        for (Event event: SpatialIndexTestHelper.getEvents()) {
          session.store(event);
        }
        session.saveChanges();

        IndexDefinition indexDefinition = new IndexDefinition();
        indexDefinition.setMap("from e in docs.Events select new { Tag = \"Event\", _ = SpatialIndex.Generate(e.Latitude, e.Longitude) }");
        indexDefinition.getIndexes().put("Tag", FieldIndexing.NOT_ANALYZED);

        store.getDatabaseCommands().putIndex("eventsByLatLng", indexDefinition);

        // Wait until the index is built
        session.advanced().documentQuery(Event.class, "eventsByLatLng")
        .waitForNonStaleResults()
        .toList();

        final double lat = 38.96939, lng = -77.386398;
        final double radiusInKm = 6.0 * 1.609344;

        List<Event> events = session.advanced()
            .documentQuery(Event.class, "eventsByLatLng")
            .whereEquals("Tag", "Event")
            .withinRadiusOf(radiusInKm, lat, lng)
            .sortByDistance()
            .waitForNonStaleResults().toList();

        int inRange = 0;
        for (Event event: SpatialIndexTestHelper.getEvents()) {
          if (SpatialIndexTest.getGeographicalDistance(lat, lng, event.getLatitude(), event.getLongitude()) <= radiusInKm) {
            inRange++;
          }
        }

        assertEquals(inRange, events.size());
        assertEquals(7, events.size());

        double previous = 0;

        for (Event e: events) {
          double distance  = SpatialIndexTest.getGeographicalDistance(lat, lng, e.getLatitude(), e.getLongitude());
          System.out.println("Venue: " + e.getVenue() + ", Distance " + distance);
          assertTrue(distance < radiusInKm);
          assertTrue(distance >= previous);
          previous = distance;
        }
      }
    }
  }

  @Test
  public void can_create_index_using_linq_from_client() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
      builder.setMap("from user in docs.Users where user.Location == \"Tel Aviv\" select new { user.Name} ");

      store.getDatabaseCommands().putIndex("UsersByLocation", builder.toIndexDefinition(store.getConventions()));

      try (IDocumentSession session = store.openSession()) {

        User user = new LinqIndexesFromClient.User();
        user.setLocation("Tel Aviv");
        user.setName("Yael");

        session.store(user);
        session.saveChanges();

        User single = session.advanced().documentQuery(LinqIndexesFromClient.User.class, "UsersByLocation").
            where("Name:Yael").
            waitForNonStaleResults().
            single();

        assertEquals("Yael", single.getName());
      }
    }
  }

  @Test
  public void can_create_index_using_linq_from_client_using_map_reduce() throws Exception {
    IndexDefinitionBuilder builder = new IndexDefinitionBuilder();

    QLinqIndexesFromClient_User u = QLinqIndexesFromClient_User.user;

    builder.setMap("from user in docs.users where user.Location == \"Tel Aviv\" select new {user.Location, Count = 1 }");
    builder.setReduce("from loc in results group loc by loc.Location into g select new { Location = g.Key, Count = g.Sum(x => x.Count) }");
    builder.getIndexes().put(u.location, FieldIndexing.NOT_ANALYZED);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      store.getDatabaseCommands().putIndex("UsersCountByLocation", builder);

      try (IDocumentSession session = store.openSession()) {
        User user = new LinqIndexesFromClient.User();
        user.setLocation("Tel Aviv");
        user.setName("Yael");

        session.store(user);
        session.saveChanges();

        LocationCount single = session.advanced().documentQuery(LinqIndexesFromClient.LocationCount.class, "UsersCountByLocation")
            .where("Location:\"Tel Aviv\"")
            .waitForNonStaleResults()
            .single();

        assertEquals("Tel Aviv", single.getLocation());
        assertEquals(1, single.getCount());

      }
    }
  }

  @Test
  public void can_get_correct_averages_from_map_reduce_index() throws Exception {
    QLinqIndexesFromClient_User u = QLinqIndexesFromClient_User.user;

    IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
    builder.setMap("from user in docs.users select new { user.Location, AgeSum = user.Age, AverageAge = user.Age, Count = 1 }");
    builder.setReduce("from loc in results group loc by loc.Location into g " +
        " let count = g.Sum( x=> x.Count)" +
        " let age = g.Sum (x => x.AgeSum)  " +
        " select new {Location = g.Key, AverageAge = age/count, Count = count, AgeSum = age }");
    builder.getIndexes().put(u.location, FieldIndexing.NOT_ANALYZED);


    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      store.getDatabaseCommands().putIndex("AvgAgeByLocation", builder.toIndexDefinition(store.getConventions()));

      try (IDocumentSession session = store.openSession()) {
        User user1 = new LinqIndexesFromClient.User();
        user1.setAge(29);
        user1.setName("Yael");
        user1.setLocation("Tel Aviv");

        User user2 = new LinqIndexesFromClient.User();
        user2.setAge(24);
        user2.setLocation("Tel Aviv");
        user2.setName("Einat");

        session.store(user1);
        session.store(user2);

        session.saveChanges();

        LocationAge single = session.advanced().documentQuery(LinqIndexesFromClient.LocationAge.class, "AvgAgeByLocation")
            .where("Location:\"Tel Aviv\"")
            .waitForNonStaleResults()
            .single();

        assertEquals("Tel Aviv", single.getLocation());
        assertEquals(26.5, single.getAverageAge(), 0.0001);
      }
    }
  }

  @Test
  public void can_get_correct_maximum_from_map_reduce_index() throws Exception {

    IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
    QLinqIndexesFromClient_LocationAge l = QLinqIndexesFromClient_LocationAge.locationAge;
    builder.setMap("from user in docs.users select new {user.Age}");
    builder.getIndexes().put(l.averageAge, FieldIndexing.ANALYZED);
    builder.getStores().put(l.averageAge, FieldStorage.YES);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      store.getDatabaseCommands().putIndex("MaxAge", builder.toIndexDefinition(store.getConventions()));

      try (IDocumentSession session = store.openSession()) {

        User user1 = new LinqIndexesFromClient.User();
        user1.setAge(27);
        user1.setName("Foo");

        User user2 = new LinqIndexesFromClient.User();
        user2.setAge(33);
        user2.setName("Bar");

        User user3 = new LinqIndexesFromClient.User();
        user3.setAge(29);
        user3.setName("Bar");

        session.store(user1);
        session.store(user2);
        session.store(user3);

        session.saveChanges();

        User user = session.advanced().documentQuery(LinqIndexesFromClient.User.class, "MaxAge")
            .orderBy("-Age")
            .take(1)
            .waitForNonStaleResults()
            .single();

        assertEquals(33, user.getAge());
      }
    }
  }

  @Test
  public void can_get_correct_maximum_from_map_reduce_index_using_orderbydescending() throws Exception {
    IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
    QLinqIndexesFromClient_LocationAge l = QLinqIndexesFromClient_LocationAge.locationAge;
    builder.setMap("from user in docs.users select new {user.Age}");
    builder.getIndexes().put(l.averageAge, FieldIndexing.ANALYZED);
    builder.getStores().put(l.averageAge, FieldStorage.YES);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.getDatabaseCommands().putIndex("MaxAge", builder.toIndexDefinition(store.getConventions()));
      try (IDocumentSession session = store.openSession()) {
        User user1 = new LinqIndexesFromClient.User();
        user1.setAge(27);
        user1.setName("Foo");

        User user2 = new LinqIndexesFromClient.User();
        user2.setAge(33);
        user2.setName("Bar");

        User user3 = new LinqIndexesFromClient.User();
        user3.setAge(29);
        user3.setName("Bar");

        session.store(user1);
        session.store(user2);
        session.store(user3);

        session.saveChanges();

        User user = session.advanced()
            .documentQuery(LinqIndexesFromClient.User.class, "MaxAge")
            .orderByDescending("Age")
            .take(1)
            .waitForNonStaleResults()
            .single();

        assertEquals(33, user.getAge());
      }
    }
  }

  @Test
  public void using_attachments() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      Attachment attachment = store.getDatabaseCommands().getAttachment("ayende");
      assertNull(attachment);

      RavenJObject meta = new RavenJObject();
      meta.add("Hello", new RavenJValue("World"));
      store.getDatabaseCommands().putAttachment("ayende", null, new ByteArrayInputStream(new byte[] { 1,2,3}), meta);

      attachment = store.getDatabaseCommands().getAttachment("ayende");
      assertNotNull(attachment);

      assertArrayEquals(new byte[] {1, 2, 3}, attachment.getData());
      assertEquals("World", attachment.getMetadata().value(String.class, "Hello"));

      store.getDatabaseCommands().deleteAttachment("ayende", null);
      attachment = store.getDatabaseCommands().getAttachment("ayende");
      assertNull(attachment);
    }
  }

  @Test
  public void getting_attachment_metadata() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      RavenJObject meta = new RavenJObject();
      meta.add("Hello", new RavenJValue("World"));
      store.getDatabaseCommands().putAttachment("sample", null, new ByteArrayInputStream(new byte[] { 1,2,3}), meta);

      Attachment attachmentOnlyWithMetadata = store.getDatabaseCommands().headAttachment("sample");
      assertEquals("World", attachmentOnlyWithMetadata.getMetadata().value(String.class, "Hello"));
      try {
        attachmentOnlyWithMetadata.getData();
        fail("getData should fail");
      } catch (IllegalArgumentException e) {
        assertEquals("Cannot get attachment data because it was NOT loaded using GET method", e.getMessage());
      }
    }
  }

  @Test
  public void getting_headers_of_attachments_with_prefix() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      RavenJObject meta1 = new RavenJObject();
      meta1.add("Hello", new RavenJValue("World"));
      store.getDatabaseCommands().putAttachment("sample/1", null, new ByteArrayInputStream(new byte[] { 1,2,3}), meta1);

      RavenJObject meta2 = new RavenJObject();
      meta2.add("Hello", new RavenJValue("World"));
      store.getDatabaseCommands().putAttachment("example/1", null, new ByteArrayInputStream(new byte[] { 1,2,3}), meta2);

      RavenJObject meta3 = new RavenJObject();
      meta3.add("Hello", new RavenJValue("World"));
      store.getDatabaseCommands().putAttachment("sample/2", null, new ByteArrayInputStream(new byte[] { 1,2,3}), meta3);

      List<Attachment> attachmentHeaders = store.getDatabaseCommands().getAttachmentHeadersStartingWith("sample", 0, 5);
      assertEquals(2, attachmentHeaders.size());

      for (Attachment attachment : attachmentHeaders) {
        assertTrue(attachment.getKey().startsWith("sample"));
        assertEquals("World", attachment.getMetadata().value(String.class, "Hello"));

        try {
          attachment.getData();
          fail("previous should fail");
        } catch (IllegalArgumentException e) {
          assertEquals("Cannot get attachment data because it was NOT loaded using GET method", e.getMessage());
        }
      }
    }
  }

  @Test
  public void using_attachments_can_properly_set_WebRequest_Headers() throws Exception {
    String key = String.format("%s-%d", "test", new Date().getTime());
    RavenJObject metadata = new RavenJObject();
    metadata.add("owner", 5);
    metadata.add("Content-Type", "text/plain");
    metadata.add("filename", "test.txt");
    metadata.add("Content-Length", 100);

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.getDatabaseCommands().putAttachment(key, null, new ByteArrayInputStream(new byte[] {0, 1, 2}), metadata);
    }
  }

  @Test
  public void can_patch_existing_document_when_present() throws Exception {
    Company company = new Company(null, "Hibernating Rhinos");

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        session.store(company);
        session.saveChanges();
      }

      store.getDatabaseCommands().patch(company.getId(), new PatchRequest[] {
        new PatchRequest(PatchCommandType.SET, "Name", new RavenJValue("Existing"))
      }, new PatchRequest[] {
        new PatchRequest(PatchCommandType.SET, "Name", new RavenJValue("New"))
      }, new RavenJObject());

      try (IDocumentSession session = store.openSession()) {
        Company company2 = session.load(Company.class, company.getId());
        assertNotNull(company2);
        assertEquals("Existing", company2.getName());
      }
    }
  }

  @Test
  public void can_patch_default_document_when_missing() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.getDatabaseCommands().patch("Company/1", new PatchRequest[] {
          new PatchRequest(PatchCommandType.SET, "Name", new RavenJValue("Existing"))
      }, new PatchRequest[] {
          new PatchRequest(PatchCommandType.SET, "Name", new RavenJValue("New"))
      }, new RavenJObject());

      try (IDocumentSession session = store.openSession()) {
        Company company2 = session.load(Company.class, "Company/1");
        assertNotNull(company2);
        assertEquals("New", company2.getName());
      }
    }
  }

  @Test
  public void should_not_throw_when_ignore_missing_true() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.getDatabaseCommands().patch("Company/1", new PatchRequest[] {
          new PatchRequest(PatchCommandType.SET, "Name", new RavenJValue("Existing"))
      });

      store.getDatabaseCommands().patch("Company/1", new PatchRequest[] {
          new PatchRequest(PatchCommandType.SET, "Name", new RavenJValue("Existing"))
      }, true);
    }
  }

  @Test(expected = DocumentDoesNotExistsException.class)
  public void should_throw_when_ignore_missing_false() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.getDatabaseCommands().patch("Company/1", new PatchRequest[] {
          new PatchRequest(PatchCommandType.SET, "Name", new RavenJValue("Existing"))
      });

      store.getDatabaseCommands().patch("Company/1", new PatchRequest[] {
          new PatchRequest(PatchCommandType.SET, "Name", new RavenJValue("Existing"))
      }, false);
    }
  }

  @Test
  public void should_return_false_on_batch_delete_when_document_missing() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      BatchResult[] batchResult = store.getDatabaseCommands().batch(Arrays.<ICommandData> asList(new DeleteCommandData("Company/1", null)));

      assertNotNull(batchResult);
      assertEquals(1, batchResult.length);
      assertNotNull(batchResult[0].getDeleted());
      assertFalse(batchResult[0].getDeleted());
    }
  }

  @Test
  public void should_return_true_on_batch_delete_when_document_present() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.getDatabaseCommands().put("Company/1", null, new RavenJObject(), new RavenJObject());
      BatchResult[] batchResult = store.getDatabaseCommands().batch(Arrays.<ICommandData> asList(new DeleteCommandData("Company/1", null)));
      assertNotNull(batchResult);
      assertEquals(1, batchResult.length);
      assertNotNull(batchResult[0].getDeleted());
      assertTrue(batchResult[0].getDeleted());
    }
  }
}

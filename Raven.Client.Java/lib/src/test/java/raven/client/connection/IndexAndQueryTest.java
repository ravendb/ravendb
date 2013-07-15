package raven.client.connection;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.mockito.cglib.core.CollectionUtils;
import org.mockito.cglib.core.Transformer;

import raven.abstractions.closure.Functions;
import raven.abstractions.data.Constants;
import raven.abstractions.data.IndexQuery;
import raven.abstractions.data.QueryResult;
import raven.abstractions.data.SortedField;
import raven.abstractions.indexing.IndexDefinition;
import raven.abstractions.indexing.SortOptions;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJValue;
import raven.client.RavenDBAwareTests;
import raven.client.document.DocumentConvention;
import raven.client.indexes.IndexDefinitionBuilder;
import raven.client.listeners.IDocumentConflictListener;
import raven.linq.dsl.IndexExpression;
import raven.linq.dsl.expressions.AnonymousExpression;
import raven.samples.Developer;
import raven.samples.QDeveloper;
import raven.samples.entities.Company;
import raven.samples.entities.Employee;

public class IndexAndQueryTest extends RavenDBAwareTests {
  private DocumentConvention convention;
  private HttpJsonRequestFactory factory;
  private ReplicationInformer replicationInformer;
  private ServerClient serverClient;

  @Before
  public void init() {
    System.setProperty("java.net.preferIPv4Stack" , "true");
    convention = new DocumentConvention();
    factory = new HttpJsonRequestFactory(10);
    replicationInformer = new ReplicationInformer();

    serverClient = new ServerClient(DEFAULT_SERVER_URL, convention, null,
        new Functions.StaticFunction1<String, ReplicationInformer>(replicationInformer), null, factory,
        UUID.randomUUID(), new IDocumentConflictListener[0]);
  }

  @Test
  public void testAsDocument() {
    //TODO provide logic + test for it + MetadataFor (http://ravendb.net/docs/2.0/client-api/querying/static-indexes/defining-static-index)
  }

  @Test
  public void testSimpleMapReduce() throws Exception {
    try {
      createDb("db1");
      IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
      insertSampleCompaniesEmployees(db1Commands);

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("docs.Companies.SelectMany(c => c.Employees).Select(x => new {Name = x.Name,Count = 1})");
      indexDefinition.setReduce("results.GroupBy(x => x.Name).Select(x => new {Name = x.Key,Count = Enumerable.Sum(x, y => ((int) y.Count))})");
      db1Commands.putIndex("nameCounts", indexDefinition);
      waitForNonStaleIndexes(db1Commands);

      IndexQuery query = new IndexQuery();
      query.setStart(0);
      query.setPageSize(1);
      query.setSortedFields(new SortedField[] { new SortedField("-Count") });

      QueryResult queryResult = db1Commands.query("nameCounts", query, new String[0]);
      assertEquals(1, queryResult.getResults().size());
      assertEquals("John", queryResult.getResults().get(0).value(String.class, "Name"));

    } finally {
      deleteDb("db1");
    }
  }


  @Test
  public void testSortOptions() throws Exception {
    try {
      createDb("db1");
      IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
      insertSampleCompaniesEmployees(db1Commands);

      IndexDefinition indexDefinitionInt = new IndexDefinition();
      indexDefinitionInt.setMap("docs.Companies.Select(x => new { NumberOfHappyCustomers = x.NumberOfHappyCustomers })");
      indexDefinitionInt.getSortOptions().put("NumberOfHappyCustomers", SortOptions.INT);
      db1Commands.putIndex("sortByNhcInt", indexDefinitionInt);

      IndexDefinition indexDefinitionDefault = new IndexDefinition();
      indexDefinitionDefault.setMap("docs.Companies.Select(x => new { NumberOfHappyCustomers = x.NumberOfHappyCustomers })");
      indexDefinitionDefault.getSortOptions().put("NumberOfHappyCustomers", SortOptions.STRING);
      db1Commands.putIndex("sortByNhcDefault", indexDefinitionDefault);

      waitForNonStaleIndexes(db1Commands);

      IndexQuery queryInt = new IndexQuery();
      queryInt.setSortedFields(new SortedField[] { new SortedField("NumberOfHappyCustomers") });
      QueryResult queryResultInt = db1Commands.query("sortByNhcInt", queryInt, new String[0]);
      assertEquals(3, queryResultInt.getResults().size());

      List<String> companyNames = extractSinglePropertyFromList(queryResultInt.getResults(), "Name", String.class);
      assertEquals(Arrays.asList("Twitter", "Coca Cola", "Google"), companyNames);

      IndexQuery queryDefault = new IndexQuery();
      queryDefault.setSortedFields(new SortedField[] { new SortedField("NumberOfHappyCustomers") });
      QueryResult queryResultDefault = db1Commands.query("sortByNhcDefault", queryDefault, new String[0]);
      assertEquals(3, queryResultDefault.getResults().size());
      companyNames = extractSinglePropertyFromList(queryResultDefault.getResults(), "Name", String.class);
      assertEquals(Arrays.asList("Google", "Twitter", "Coca Cola"), companyNames);

    } finally {
      deleteDb("db1");
    }
  }

  @Test
  public void testAdvancedMapReduce() throws Exception {
    try {
      createDb("db1");
      IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
      insertSampleCompaniesEmployees(db1Commands);

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from c in docs.Companies from e in c.Employees from s in e.Specialties select new {Spec = s, Count = 1}");
      indexDefinition.setReduce("results.GroupBy(x => x.Spec).Select(x => new {Spec = x.Key,Count = Enumerable.Sum(x, y => ((int) y.Count))})");

      db1Commands.putIndex("DevelopersCountBySkill", indexDefinition);
      waitForNonStaleIndexes(db1Commands);

      QueryResult queryAll = db1Commands.query("DevelopersCountBySkill", new IndexQuery(), new String[0]);
      assertEquals(2, queryAll.getResults().size());

    } finally {
      deleteDb("db1");
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDynamicQuery() throws Exception {
    try {
      createDb("db1");
      IDatabaseCommands db1Commands = serverClient.forDatabase("db1");

      insertSampleCompaniesEmployees(db1Commands);

      QueryResult queryAll = db1Commands.query("dynamic/Companies", new IndexQuery(), new String[0]);
      assertEquals(3, queryAll.getResults().size());

      IndexQuery twitterQuery = new IndexQuery();
      twitterQuery.setQuery("Name:\"Twitter\"");

      QueryResult twitterResult = db1Commands.query("dynamic/Companies", twitterQuery, new String[0]);
      assertEquals(1, twitterResult.getResults().size());
      assertEquals("Twitter", twitterResult.getResults().iterator().next().get("Name").value(String.class));

      IndexQuery happyTwitterQuery = new IndexQuery();
      happyTwitterQuery.setQuery("Name:\"Twitter\" AND NumberOfHappyCustomers_Range:{Ix20 TO NULL}");

      QueryResult happpyTwitterResult = db1Commands.query("dynamic/Companies", happyTwitterQuery, new String[0]);
      assertEquals(1, happpyTwitterResult.getResults().size());
      assertEquals("Twitter", happpyTwitterResult.getResults().iterator().next().get("Name").value(String.class));

      IndexQuery happyTwitterQuery500 = new IndexQuery();
      happyTwitterQuery500.setQuery("Name:\"Twitter\" AND NumberOfHappyCustomers_Range:{Ix500 TO NULL}");

      QueryResult happpyTwitterResult500 = db1Commands.query("dynamic/Companies", happyTwitterQuery500, new String[0]);
      assertEquals(0, happpyTwitterResult500.getResults().size());

      IndexQuery companiesWithJohns = new IndexQuery();
      companiesWithJohns.setQuery("Employees,Name:John");
      QueryResult companiesWithJohnsResult = db1Commands.query("dynamic/Companies", companiesWithJohns, new String[0]);
      assertEquals(2, companiesWithJohnsResult.getResults().size());

      IndexQuery companiesWithJavaWorkers = new IndexQuery();
      companiesWithJavaWorkers.setQuery("Employees,Specialties:Java");
      QueryResult companiesWithJavaWorkersResult = db1Commands.query("dynamic/Companies", companiesWithJavaWorkers, new String[0]);
      assertEquals(1, companiesWithJavaWorkersResult.getResults().size());

      IndexQuery companiesByNameDesc = new IndexQuery();
      companiesByNameDesc.setSortedFields(new SortedField[] { new SortedField("-Name") } );
      companiesByNameDesc.setFieldsToFetch(new String[] { "Name" });
      QueryResult companiesByNameDescResult = db1Commands.query("dynamic/Companies", companiesByNameDesc, new String[0]);
      assertEquals(3, companiesByNameDescResult.getResults().size());
      List<String> companyNames = CollectionUtils.transform(companiesByNameDescResult.getResults(), new Transformer() {

        @Override
        public Object transform(Object value) {
          RavenJObject obj = (RavenJObject)value;
          return obj.value(String.class, "Name");
        }
      });

      assertEquals(Arrays.asList("Twitter", "Google", "Coca Cola"), companyNames);

      companiesByNameDesc.setStart(1);
      companiesByNameDesc.setPageSize(1);
      companiesByNameDescResult = db1Commands.query("dynamic/Companies", companiesByNameDesc, new String[0]);
      assertEquals(1, companiesByNameDescResult.getResults().size());
      assertEquals(3, companiesByNameDescResult.getTotalResults());

    } finally {
      deleteDb("db1");
    }
  }

  private void insertSampleCompaniesEmployees(IDatabaseCommands db1Commands) {
    Company c1 = new Company("1", "Coca Cola", Arrays.asList(
        new Employee("John", new String[] {  }, new Date(), 100.0) )
        , "US", 5000 );

    Company c2 = new Company("2", "Twitter",  Arrays.asList(
        new Employee("Mark", new String[] { "Java", "C#" }, new Date(), 100.0) ,
        new Employee("Jonatan", new String[] { "Java"}, new Date(), 100.0) ,
        new Employee("Greg", new String[] { "C#"}, new Date(), 100.0) )
        , "US", 300);

    Company c3 = new Company("3", "Google",  Arrays.asList(
        new Employee("Taylor", new String[] { "C#" }, new Date(), 100.0) ,
        new Employee("Alice", new String[] { }, new Date(), 100.0) ,
        new Employee("John", new String[] { "C#"}, new Date(), 100.0)
        ), "US", 200000);


    RavenJObject meta1 = new RavenJObject();
    meta1.add(Constants.RAVEN_ENTITY_NAME, RavenJValue.fromObject("Companies"));
    meta1.add(Constants.LAST_MODIFIED, RavenJValue.fromObject(new Date()));

    db1Commands.put("companies/1", null, RavenJObject.fromObject(c1), meta1);
    db1Commands.put("companies/2", null, RavenJObject.fromObject(c2), meta1);
    db1Commands.put("companies/3", null, RavenJObject.fromObject(c3), meta1);
    waitForNonStaleIndexes(db1Commands);
  }

  @Test
  public void testCreateIndexAndQuery() throws Exception {
    try {
      createDb("db1");
      IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
      Developer d1 = new Developer();
      d1.setNick("john");
      d1.setId(10l);

      Developer d2 = new Developer();
      d2.setNick("marcin");
      d2.setId(15l);

      RavenJObject meta1 = new RavenJObject();
      meta1.add(Constants.RAVEN_ENTITY_NAME, RavenJValue.fromObject("Developers"));
      meta1.add(Constants.LAST_MODIFIED, RavenJValue.fromObject(new Date()));

      RavenJObject meta2 = new RavenJObject();
      meta2.add(Constants.RAVEN_ENTITY_NAME, RavenJValue.fromObject("Developers"));
      meta2.add(Constants.LAST_MODIFIED, RavenJValue.fromObject(new Date()));

      db1Commands.put("developers/1", null, RavenJObject.fromObject(d1), meta1);
      db1Commands.put("developers/2", null, RavenJObject.fromObject(d2), meta2);

      QDeveloper d = QDeveloper.developer;

      IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
      IndexExpression indexExpression = IndexExpression
          .from(Developer.class)
          .where(d.nick.startsWith("m"))
          .select(AnonymousExpression.create(Developer.class).with(d.nick, d.nick));
      builder.setMap(indexExpression);

      db1Commands.putIndex("devStartWithM", builder.toIndexDefinition(convention));

      waitForNonStaleIndexes(db1Commands);

      IndexQuery query = new IndexQuery();

      QueryResult queryResult = db1Commands.query("devStartWithM", query, new String[0]);
      assertEquals(false, queryResult.isStale());
      assertEquals(1, queryResult.getResults().size());
      assertEquals("marcin", queryResult.getResults().iterator().next().value(String.class, "Nick"));

    } finally {
      deleteDb("db1");
    }
  }

}

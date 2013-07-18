package raven.client.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.cglib.core.CollectionUtils;
import org.mockito.cglib.core.Transformer;

import raven.abstractions.basic.Holder;
import raven.abstractions.closure.Functions;
import raven.abstractions.data.Constants;
import raven.abstractions.data.Etag;
import raven.abstractions.data.IndexQuery;
import raven.abstractions.data.MultiLoadResult;
import raven.abstractions.data.QueryHeaderInformation;
import raven.abstractions.data.QueryResult;
import raven.abstractions.data.SortedField;
import raven.abstractions.exceptions.ServerClientException;
import raven.abstractions.indexing.IndexDefinition;
import raven.abstractions.indexing.SortOptions;
import raven.abstractions.json.linq.RavenJArray;
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
import raven.samples.entities.QCompany;

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

  @Test(expected = ServerClientException.class)
  public void testPutInvalidIndex() throws Exception {
    try {
      createDb("db1");
      IndexDefinition index = new IndexDefinition();
      index.setMap("from doc in docs where doc.Type == 'posts' select new{ doc.Title.Length }");
      IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
      db1Commands.putIndex("invalidIndex", index);

    } finally {
      deleteDb("db1");
    }
  }

  @Test
  public void testUpdateByIndexPatch() throws Exception {
    try {
      createDb("db1");
      IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
      insertSampleCompaniesEmployees(db1Commands);


    } finally {
      deleteDb("db1");
    }
  }

  @Test
  @Ignore("waiting for RavenDB-1229")
  public void testDeleteByIndex() throws Exception {
    try {
      createDb("db1");
      IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
      insertSampleCompaniesEmployees(db1Commands);

      try {
        db1Commands.deleteByIndex("noSuchIndex", new IndexQuery());
        fail();
      } catch (IllegalStateException e) {
        // ok
      }

      QCompany c = new QCompany("c");

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap(
          IndexExpression.from(Company.class)
          .where(c.name.startsWith("T").or(c.name.startsWith("C")))
          .select(AnonymousExpression.create(Company.class).with(c.name, c.name)).toLinq()
          );

      String indexName = "companies/startsWithTorC";

      db1Commands.putIndex(indexName, indexDefinition);
      waitForNonStaleIndexes(db1Commands);

      Operation operation = db1Commands.deleteByIndex(indexName, new IndexQuery());
      RavenJArray completion = (RavenJArray) operation.waitForCompletion();
      assertEquals(2, completion.size());

    } finally {
      deleteDb("db1");
    }
  }

  @Test
  public void testAsDocument() {
    //TODO provide logic + test for it + MetadataFor (http://ravendb.net/docs/2.0/client-api/querying/static-indexes/defining-static-index)
  }

  @Test
  public void testStreamQuery() throws Exception {
    try {
      createDb("db1");
      IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
      insertSampleCompaniesEmployees(db1Commands);

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("docs.Companies.Select(x => new {Name = x.Name})");
      db1Commands.putIndex("companies/simple", indexDefinition);
      waitForNonStaleIndexes(db1Commands);

      IndexQuery query = new IndexQuery();
      Holder<QueryHeaderInformation> queryHeaderInfo = new Holder<>();
      Iterator<RavenJObject> iterator = db1Commands.streamQuery("companies/simple", query, queryHeaderInfo);

      Set<String> companyNames = new HashSet<>();

      while (iterator.hasNext()) {
        RavenJObject ravenJObject = iterator.next();
        companyNames.add(ravenJObject.value(String.class, "Name"));
      }

      assertEquals(new HashSet<>(Arrays.asList("Coca Cola", "Twitter", "Google")), companyNames);

      QueryHeaderInformation headerInformation = queryHeaderInfo.value;

      assertNotNull(headerInformation.getIndex());

    } finally {
      deleteDb("db1");
    }
  }

  @Test
  public void testStreamDocs() throws Exception {
    try {
      createDb("db1");
      IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
      insertSampleCompaniesEmployees(db1Commands);

      try {
        db1Commands.streamDocs(Etag.empty(), "companies/", "", 10, 5);
        fail("can't use etag with startsWith");
      } catch (IllegalArgumentException e) {
        //ok
      }


      Iterator<RavenJObject> streamDocs = db1Commands.streamDocs(null, "companies/", null, 0, Integer.MAX_VALUE);
      Set<String> companyNames = new HashSet<>();

      while (streamDocs.hasNext()) {
        RavenJObject ravenJObject = streamDocs.next();
        companyNames.add(ravenJObject.value(String.class, "Name"));
      }
      assertEquals(new HashSet<>(Arrays.asList("Coca Cola", "Twitter", "Google")), companyNames);

      int count = 0;
      streamDocs = db1Commands.streamDocs(null, "companies/", "*", 1, 2);
      while (streamDocs.hasNext()) {
        RavenJObject ravenJObject = streamDocs.next();
        assertNotNull(ravenJObject.value(String.class, "Name"));
        count++;
      }

      assertEquals(2, count);

    } finally {
      deleteDb("db1");
    }
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

  @Test
  public void testQueryWithIncludes() throws Exception {
    try {
      createDb("db1");
      IDatabaseCommands db1Commands = serverClient.forDatabase("db1");
      insertSampleDataWithIncludes(db1Commands);

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from s in docs.School select new {Name = s.Name}");

      db1Commands.putIndex("schoolsByName", indexDefinition);
      waitForNonStaleIndexes(db1Commands);

      IndexQuery query = new IndexQuery();
      QueryResult queryResult = db1Commands.query("schoolsByName", query, new String[] { "Students" });

      assertEquals(1, queryResult.getResults().size());
      assertEquals(2, queryResult.getIncludes().size());

      MultiLoadResult students = db1Commands.get(new String[] { "student/1", "student/2" }, new String[0]);
      assertEquals(2, students.getResults().size());
      assertTrue(students.getResults().get(0).containsKey("Name"));

      MultiLoadResult studentsMeta = db1Commands.get(new String[] { "student/1", "student/2" }, new String[0], null, null, true);
      assertEquals(2, studentsMeta.getResults().size());
      assertFalse(studentsMeta.getResults().get(0).containsKey("Name"));

      MultiLoadResult schoolWithStudents = db1Commands.get(new String[] { "school/1" }, new String[] { "Students" });
      assertEquals(1, schoolWithStudents.getResults().size());
      assertEquals(2, schoolWithStudents.getIncludes().size());

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

  private void insertSampleDataWithIncludes(IDatabaseCommands dbCommands) {

    // create objects
    RavenJObject metaSchool = new RavenJObject();
    metaSchool.add(Constants.RAVEN_ENTITY_NAME, new RavenJValue("School"));

    RavenJObject metaStudent = new RavenJObject();
    metaStudent.add(Constants.RAVEN_ENTITY_NAME, new RavenJValue("Student"));

    RavenJObject school = new RavenJObject();
    school.add("Name", new RavenJValue("Harvard"));
    school.add("Students", new RavenJArray(new RavenJValue("student/1"), new RavenJValue("student/2")));

    RavenJObject student1 = new RavenJObject();
    student1.add("Name", new RavenJValue("Yoda"));

    RavenJObject student2 = new RavenJObject();
    student2.add("Name", new RavenJValue("Jedi"));

    // put objects
    dbCommands.put("school/1", null, school, metaSchool);
    dbCommands.put("student/1", null, student1, metaStudent);
    dbCommands.put("student/2", null, student2, metaStudent);

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

      try {
        db1Commands.putIndex("devStartWithM", builder.toIndexDefinition(convention), false);
        fail("Can't overwrite index.");
      } catch (ServerClientException e) {
        //ok
      }


      waitForNonStaleIndexes(db1Commands);

      IndexQuery query = new IndexQuery();

      QueryResult queryResult = db1Commands.query("devStartWithM", query, new String[0]);
      assertEquals(false, queryResult.isStale());
      assertEquals(1, queryResult.getResults().size());
      assertEquals("marcin", queryResult.getResults().iterator().next().value(String.class, "Nick"));

      queryResult = db1Commands.query("devStartWithM", query, new String[0], true);
      assertEquals(1, queryResult.getResults().size());
      RavenJObject ravenJObject = queryResult.getResults().iterator().next();
      assertFalse(ravenJObject.containsKey("Nick"));
      assertTrue(ravenJObject.containsKey("@metadata"));

      db1Commands.deleteIndex("devStartWithM");
      assertNull(db1Commands.getIndex("devStartWithM"));

      try {
        queryResult = db1Commands.query("devStartWithM", query, new String[0]);
        fail();
      } catch (ServerClientException e) {
        //ok
      }

    } finally {
      deleteDb("db1");
    }
  }

}

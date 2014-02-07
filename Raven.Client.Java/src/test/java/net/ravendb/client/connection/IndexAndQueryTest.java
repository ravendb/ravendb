package net.ravendb.client.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.IndexQuery;
import net.ravendb.abstractions.data.JsonDocument;
import net.ravendb.abstractions.data.MultiLoadResult;
import net.ravendb.abstractions.data.PatchCommandType;
import net.ravendb.abstractions.data.PatchRequest;
import net.ravendb.abstractions.data.QueryHeaderInformation;
import net.ravendb.abstractions.data.QueryResult;
import net.ravendb.abstractions.data.SortedField;
import net.ravendb.abstractions.exceptions.ServerClientException;
import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.abstractions.indexing.SortOptions;
import net.ravendb.abstractions.json.linq.RavenJArray;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.abstractions.json.linq.RavenJValue;
import net.ravendb.client.RavenDBAwareTests;
import net.ravendb.client.connection.IDatabaseCommands;
import net.ravendb.client.connection.Operation;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.client.indexes.IndexDefinitionBuilder;
import net.ravendb.samples.Developer;
import net.ravendb.samples.entities.Company;
import net.ravendb.samples.entities.Employee;

import org.junit.Ignore;
import org.junit.Test;
import org.mockito.cglib.core.CollectionUtils;
import org.mockito.cglib.core.Transformer;


public class IndexAndQueryTest extends RavenDBAwareTests {

  @Test(expected = ServerClientException.class)
  public void testPutInvalidIndex() throws Exception {
    try {
      createDb();
      IndexDefinition index = new IndexDefinition();
      index.setMap("from doc in docs where doc.Type == 'posts' select new{ doc.Title.Length }");
      IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
      dbCommands.putIndex("invalidIndex", index);

    } finally {
      deleteDb();
    }
  }

  @Test
  public void testUpdateByIndexPatch() throws Exception {
    try {
      createDb();
      IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
      insertSampleCompaniesEmployees(dbCommands);

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from c in docs.Companies select new { Name = c.Name}");
      dbCommands.putIndex("companiesIndex", indexDefinition);
      waitForNonStaleIndexes(dbCommands);

      PatchRequest patch = new PatchRequest();
      patch.setType(PatchCommandType.SET);
      patch.setName("IsBig");
      patch.setValue(new RavenJValue(true));

      Operation operation = dbCommands.updateByIndex("companiesIndex", new IndexQuery(), new PatchRequest[] { patch });
      operation.waitForCompletion();

      JsonDocument jsonDocument = dbCommands.get("companies/1");
      assertTrue(jsonDocument.getDataAsJson().containsKey("IsBig"));

    } finally {
      deleteDb();
    }
  }

  @Test
  public void testDeleteByIndex() throws Exception {
    try {
      createDb();
      IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
      insertSampleCompaniesEmployees(dbCommands);

      try {
        dbCommands.deleteByIndex("noSuchIndex", new IndexQuery()).waitForCompletion();
        fail();
      } catch (IllegalStateException e) {
        // ok
      }

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from c in docs.Companies where c.Name.StartsWith(\"T\") || c.Name.StartsWith(\"C\") select new {Name = c.Name}");

      String indexName = "companies/startsWithTorC";

      dbCommands.putIndex(indexName, indexDefinition);
      waitForNonStaleIndexes(dbCommands);

      Operation operation = dbCommands.deleteByIndex(indexName, new IndexQuery());
      RavenJArray completion = (RavenJArray) operation.waitForCompletion();
      assertEquals(2, completion.size());

    } finally {
      deleteDb();
    }
  }

  @Test
  public void testStreamQuery() throws Exception {
    try {
      createDb();
      IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
      insertSampleCompaniesEmployees(dbCommands);

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("docs.Companies.Select(x => new {Name = x.Name})");
      dbCommands.putIndex("companies/simple", indexDefinition);
      waitForNonStaleIndexes(dbCommands);

      IndexQuery query = new IndexQuery();
      Reference<QueryHeaderInformation> queryHeaderInfo = new Reference<>();
      Set<String> companyNames = new HashSet<>();
      try (RavenJObjectIterator iterator = dbCommands.streamQuery("companies/simple", query, queryHeaderInfo)) {
        while (iterator.hasNext()) {
          RavenJObject ravenJObject = iterator.next();
          companyNames.add(ravenJObject.value(String.class, "Name"));
        }
      }

      assertEquals(new HashSet<>(Arrays.asList("Coca Cola", "Twitter", "Google")), companyNames);

      QueryHeaderInformation headerInformation = queryHeaderInfo.value;

      assertNotNull(headerInformation.getIndex());

    } finally {
      deleteDb();
    }
  }

  @Test
  public void testStreamDocs() throws Exception {
    try {
      createDb();
      IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
      insertSampleCompaniesEmployees(dbCommands);

      try {
        dbCommands.streamDocs(Etag.empty(), "companies/", "", 10, 5);
        fail("can't use etag with startsWith");
      } catch (IllegalArgumentException e) {
        //ok
      }


      RavenJObjectIterator streamDocs = dbCommands.streamDocs(null, "companies/", null, 0, Integer.MAX_VALUE);
      Set<String> companyNames = new HashSet<>();

      while (streamDocs.hasNext()) {
        RavenJObject ravenJObject = streamDocs.next();
        companyNames.add(ravenJObject.value(String.class, "Name"));
      }
      assertEquals(new HashSet<>(Arrays.asList("Coca Cola", "Twitter", "Google")), companyNames);

      int count = 0;
      streamDocs = dbCommands.streamDocs(null, "companies/", "*", 1, 2);
      while (streamDocs.hasNext()) {
        RavenJObject ravenJObject = streamDocs.next();
        assertNotNull(ravenJObject.value(String.class, "Name"));
        count++;
      }

      dbCommands.get("123");

      assertEquals(2, count);

    } finally {
      deleteDb();
    }
  }

  private static class CompaniesMapReduce extends AbstractIndexCreationTask {
    public CompaniesMapReduce() {
      map = "from c in docs.Companies " +
        "from e in c.Employees " +
        "select new { Name = e.Name, Count = 1}";
      reduce = "from result in results " +
        "group result by result.Name into g " +
        "select new { Name = g.Key, Count = g.Sum(x => x.Count) }";
    }
  }

  @Test
  public void testMapReduceWithDsl() throws Exception {
    try {
      createDb();
      IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
      insertSampleCompaniesEmployees(dbCommands);

      dbCommands.putIndex("companiesMapReduce", new CompaniesMapReduce().createIndexDefinition());
      waitForNonStaleIndexes(dbCommands);

      IndexQuery query = new IndexQuery();
      query.setStart(0);
      query.setPageSize(1);
      query.setSortedFields(new SortedField[] { new SortedField("-Count") });

      QueryResult queryResult = dbCommands.query("companiesMapReduce", query, new String[0]);
      assertEquals(1, queryResult.getResults().size());
      assertEquals("John", queryResult.getResults().get(0).value(String.class, "Name"));


    } finally {
      deleteDb();
    }
  }


  @Test
  public void testSimpleMapReduce() throws Exception {
    try {
      createDb();
      IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
      insertSampleCompaniesEmployees(dbCommands);

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("docs.Companies.SelectMany(c => c.Employees).Select(x => new {Name = x.Name,Count = 1})");
      indexDefinition.setReduce("results.GroupBy(x => x.Name).Select(x => new {Name = x.Key,Count = Enumerable.Sum(x, y => ((int) y.Count))})");
      dbCommands.putIndex("nameCounts", indexDefinition);
      waitForNonStaleIndexes(dbCommands);

      IndexQuery query = new IndexQuery();
      query.setStart(0);
      query.setPageSize(1);
      query.setSortedFields(new SortedField[] { new SortedField("-Count") });

      QueryResult queryResult = dbCommands.query("nameCounts", query, new String[0]);
      assertEquals(1, queryResult.getResults().size());
      assertEquals("John", queryResult.getResults().get(0).value(String.class, "Name"));

    } finally {
      deleteDb();
    }
  }


  @Test
  public void testSortOptions() throws Exception {
    try {
      createDb();
      IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
      insertSampleCompaniesEmployees(dbCommands);

      IndexDefinition indexDefinitionInt = new IndexDefinition();
      indexDefinitionInt.setMap("docs.Companies.Select(x => new { NumberOfHappyCustomers = x.NumberOfHappyCustomers })");
      indexDefinitionInt.getSortOptions().put("NumberOfHappyCustomers", SortOptions.INT);
      dbCommands.putIndex("sortByNhcInt", indexDefinitionInt);

      IndexDefinition indexDefinitionDefault = new IndexDefinition();
      indexDefinitionDefault.setMap("docs.Companies.Select(x => new { NumberOfHappyCustomers = x.NumberOfHappyCustomers })");
      indexDefinitionDefault.getSortOptions().put("NumberOfHappyCustomers", SortOptions.STRING);
      dbCommands.putIndex("sortByNhcDefault", indexDefinitionDefault);

      waitForNonStaleIndexes(dbCommands);

      IndexQuery queryInt = new IndexQuery();
      queryInt.setSortedFields(new SortedField[] { new SortedField("NumberOfHappyCustomers") });
      QueryResult queryResultInt = dbCommands.query("sortByNhcInt", queryInt, new String[0]);
      assertEquals(3, queryResultInt.getResults().size());

      List<String> companyNames = extractSinglePropertyFromList(queryResultInt.getResults(), "Name", String.class);
      assertEquals(Arrays.asList("Twitter", "Coca Cola", "Google"), companyNames);

      IndexQuery queryDefault = new IndexQuery();
      queryDefault.setSortedFields(new SortedField[] { new SortedField("NumberOfHappyCustomers") });
      QueryResult queryResultDefault = dbCommands.query("sortByNhcDefault", queryDefault, new String[0]);
      assertEquals(3, queryResultDefault.getResults().size());
      companyNames = extractSinglePropertyFromList(queryResultDefault.getResults(), "Name", String.class);
      assertEquals(Arrays.asList("Google", "Twitter", "Coca Cola"), companyNames);

    } finally {
      deleteDb();
    }
  }

  @Test
  public void testAdvancedMapReduce() throws Exception {
    try {
      createDb();
      IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
      insertSampleCompaniesEmployees(dbCommands);

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from c in docs.Companies from e in c.Employees from s in e.Specialties select new {Spec = s, Count = 1}");
      indexDefinition.setReduce("results.GroupBy(x => x.Spec).Select(x => new {Spec = x.Key,Count = Enumerable.Sum(x, y => ((int) y.Count))})");

      dbCommands.putIndex("DevelopersCountBySkill", indexDefinition);
      waitForNonStaleIndexes(dbCommands);

      QueryResult queryAll = dbCommands.query("DevelopersCountBySkill", new IndexQuery(), new String[0]);
      assertEquals(2, queryAll.getResults().size());

    } finally {
      deleteDb();
    }
  }

  @Test
  public void testQueryWithIncludes() throws Exception {
    try {
      createDb();
      IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
      insertSampleDataWithIncludes(dbCommands);

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from s in docs.School select new {Name = s.Name}");

      dbCommands.putIndex("schoolsByName", indexDefinition);
      waitForNonStaleIndexes(dbCommands);

      IndexQuery query = new IndexQuery();
      QueryResult queryResult = dbCommands.query("schoolsByName", query, new String[] { "Students" });

      assertEquals(1, queryResult.getResults().size());
      assertEquals(2, queryResult.getIncludes().size());

      MultiLoadResult students = dbCommands.get(new String[] { "student/1", "student/2" }, new String[0]);
      assertEquals(2, students.getResults().size());
      assertTrue(students.getResults().get(0).containsKey("Name"));

      MultiLoadResult studentsMeta = dbCommands.get(new String[] { "student/1", "student/2" }, new String[0], null, null, true);
      assertEquals(2, studentsMeta.getResults().size());
      assertFalse(studentsMeta.getResults().get(0).containsKey("Name"));

      MultiLoadResult schoolWithStudents = dbCommands.get(new String[] { "school/1" }, new String[] { "Students" });
      assertEquals(1, schoolWithStudents.getResults().size());
      assertEquals(2, schoolWithStudents.getIncludes().size());

    } finally {
      deleteDb();
    }
  }

  @Test
  public void testDynamicQuery() throws Exception {
    try {
      createDb();
      IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());

      insertSampleCompaniesEmployees(dbCommands);

      QueryResult queryAll = dbCommands.query("dynamic/Companies", new IndexQuery(), new String[0]);
      assertEquals(3, queryAll.getResults().size());

      IndexQuery twitterQuery = new IndexQuery();
      twitterQuery.setQuery("Name:\"Twitter\"");

      QueryResult twitterResult = dbCommands.query("dynamic/Companies", twitterQuery, new String[0]);
      assertEquals(1, twitterResult.getResults().size());
      assertEquals("Twitter", twitterResult.getResults().iterator().next().get("Name").value(String.class));

      IndexQuery happyTwitterQuery = new IndexQuery();
      happyTwitterQuery.setQuery("Name:\"Twitter\" AND NumberOfHappyCustomers_Range:{Ix20 TO NULL}");

      QueryResult happpyTwitterResult = dbCommands.query("dynamic/Companies", happyTwitterQuery, new String[0]);
      assertEquals(1, happpyTwitterResult.getResults().size());
      assertEquals("Twitter", happpyTwitterResult.getResults().iterator().next().get("Name").value(String.class));

      IndexQuery happyTwitterQuery500 = new IndexQuery();
      happyTwitterQuery500.setQuery("Name:\"Twitter\" AND NumberOfHappyCustomers_Range:{Ix500 TO NULL}");

      QueryResult happpyTwitterResult500 = dbCommands.query("dynamic/Companies", happyTwitterQuery500, new String[0]);
      assertEquals(0, happpyTwitterResult500.getResults().size());

      IndexQuery companiesWithJohns = new IndexQuery();
      companiesWithJohns.setQuery("Employees,Name:John");
      QueryResult companiesWithJohnsResult = dbCommands.query("dynamic/Companies", companiesWithJohns, new String[0]);
      assertEquals(2, companiesWithJohnsResult.getResults().size());

      IndexQuery companiesWithJavaWorkers = new IndexQuery();
      companiesWithJavaWorkers.setQuery("Employees,Specialties:Java");
      QueryResult companiesWithJavaWorkersResult = dbCommands.query("dynamic/Companies", companiesWithJavaWorkers, new String[0]);
      assertEquals(1, companiesWithJavaWorkersResult.getResults().size());

      IndexQuery companiesByNameDesc = new IndexQuery();
      companiesByNameDesc.setSortedFields(new SortedField[] { new SortedField("-Name") } );
      companiesByNameDesc.setFieldsToFetch(new String[] { "Name" });
      QueryResult companiesByNameDescResult = dbCommands.query("dynamic/Companies", companiesByNameDesc, new String[0]);
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
      companiesByNameDescResult = dbCommands.query("dynamic/Companies", companiesByNameDesc, new String[0]);
      assertEquals(1, companiesByNameDescResult.getResults().size());
      assertEquals(3, companiesByNameDescResult.getTotalResults());

    } finally {
      deleteDb();
    }
  }

  private static void insertSampleDataWithIncludes(IDatabaseCommands dbCommands) {

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

  private void insertSampleCompaniesEmployees(IDatabaseCommands dbCommands) {
    Company c1 = new Company("1", "Coca Cola", Arrays.asList(
      new Employee("John", new ArrayList<String>(), new Date(), 100.0) )
      , "US", 5000 );

    Company c2 = new Company("2", "Twitter",  Arrays.asList(
      new Employee("Mark", Arrays.asList( "Java", "C#" ), new Date(), 100.0) ,
      new Employee("Jonatan", Arrays.asList( "Java"), new Date(), 100.0) ,
      new Employee("Greg", Arrays.asList( "C#"), new Date(), 100.0) )
      , "US", 300);

    Company c3 = new Company("3", "Google",  Arrays.asList(
      new Employee("Taylor", Arrays.asList( "C#" ), new Date(), 100.0) ,
      new Employee("Alice", new ArrayList<String>(), new Date(), 100.0) ,
      new Employee("John", Arrays.asList( "C#"), new Date(), 100.0)
      ), "US", 200000);


    RavenJObject meta1 = new RavenJObject();
    meta1.add(Constants.RAVEN_ENTITY_NAME, RavenJToken.fromObject("Companies"));

    dbCommands.put("companies/1", null, RavenJObject.fromObject(c1), meta1);
    dbCommands.put("companies/2", null, RavenJObject.fromObject(c2), meta1);
    dbCommands.put("companies/3", null, RavenJObject.fromObject(c3), meta1);
    waitForNonStaleIndexes(dbCommands);
  }

  @Test
  public void testCreateIndexAndQuery() throws Exception {
    try {
      createDb();
      IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());
      Developer d1 = new Developer();
      d1.setNick("john");
      d1.setId(10l);

      Developer d2 = new Developer();
      d2.setNick("marcin");
      d2.setId(15l);

      RavenJObject meta1 = new RavenJObject();
      meta1.add(Constants.RAVEN_ENTITY_NAME, RavenJToken.fromObject("Developers"));

      RavenJObject meta2 = new RavenJObject();
      meta2.add(Constants.RAVEN_ENTITY_NAME, RavenJToken.fromObject("Developers"));

      dbCommands.put("developers/1", null, RavenJObject.fromObject(d1), meta1);
      dbCommands.put("developers/2", null, RavenJObject.fromObject(d2), meta2);

      IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
      builder.setMap("from d in docs.Developers where d.Nick.StartsWith(\"m\") select new { Nick = d.Nick}");

      dbCommands.putIndex("devStartWithM", builder.toIndexDefinition(convention));

      try {
        dbCommands.putIndex("devStartWithM", builder.toIndexDefinition(convention), false);
        fail("Can't overwrite index.");
      } catch (ServerClientException e) {
        //ok
      }


      waitForNonStaleIndexes(dbCommands);

      IndexQuery query = new IndexQuery();

      QueryResult queryResult = dbCommands.query("devStartWithM", query, new String[0]);
      assertEquals(false, queryResult.isStale());
      assertEquals(1, queryResult.getResults().size());
      assertEquals("marcin", queryResult.getResults().iterator().next().value(String.class, "Nick"));

      queryResult = dbCommands.query("devStartWithM", query, new String[0], true);
      assertEquals(1, queryResult.getResults().size());
      RavenJObject ravenJObject = queryResult.getResults().iterator().next();
      assertFalse(ravenJObject.containsKey("Nick"));
      assertTrue(ravenJObject.containsKey("@metadata"));

      dbCommands.deleteIndex("devStartWithM");
      assertNull(dbCommands.getIndex("devStartWithM"));

      try {
        queryResult = dbCommands.query("devStartWithM", query, new String[0]);
        fail();
      } catch (ServerClientException e) {
        //ok
      }

    } finally {
      deleteDb();
    }
  }

}

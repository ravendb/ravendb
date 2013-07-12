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

  @SuppressWarnings("unchecked")
  @Test
  public void testDynamicQuery() throws Exception {
    try {
      createDb("db1");
      IDatabaseCommands db1Commands = serverClient.forDatabase("db1");

      Company c1 = new Company("1", "Coca Cola", Arrays.asList(
          new Employee("John", new String[] {  }, new Date(), 100.0) )
          , "US", 100 );

      Company c2 = new Company("2", "Twitter",  Arrays.asList(
          new Employee("Mark", new String[] { "Java", "C#" }, new Date(), 100.0) ,
          new Employee("Jonatan", new String[] { "Java"}, new Date(), 100.0) ,
          new Employee("Greg", new String[] { "C#"}, new Date(), 100.0) )
          , "US", 200);

      Company c3 = new Company("3", "Google",  Arrays.asList(
          new Employee("Taylor", new String[] { "C#" }, new Date(), 100.0) ,
          new Employee("Alice", new String[] { }, new Date(), 100.0) ,
          new Employee("John", new String[] { "C#"}, new Date(), 100.0)
          ), "US", 200000);


      RavenJObject meta1 = new RavenJObject();
      meta1.add(Constants.RAVEN_ENTITY_NAME, RavenJValue.fromObject("Companies"));
      meta1.add(Constants.LAST_MODIFIED, RavenJValue.fromObject(new Date()));

      db1Commands.put("companies/Cola-Cola", null, RavenJObject.fromObject(c1), meta1);
      db1Commands.put("companies/Twitter", null, RavenJObject.fromObject(c2), meta1);
      db1Commands.put("companies/Google", null, RavenJObject.fromObject(c3), meta1);
      waitForNonStaleIndexes(db1Commands);

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

    } finally {
      deleteDb("db1");
    }
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

package raven.client.connection;

import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import raven.abstractions.closure.Functions;
import raven.abstractions.data.Constants;
import raven.abstractions.data.IndexQuery;
import raven.abstractions.data.QueryResult;
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

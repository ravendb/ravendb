package raven.client.connection;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import raven.abstractions.closure.Functions;
import raven.abstractions.data.Constants;
import raven.abstractions.data.StringDistanceTypes;
import raven.abstractions.data.SuggestionQuery;
import raven.abstractions.data.SuggestionQueryResult;
import raven.abstractions.indexing.FieldIndexing;
import raven.abstractions.indexing.IndexDefinition;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJValue;
import raven.client.RavenDBAwareTests;
import raven.client.document.DocumentConvention;
import raven.client.listeners.IDocumentConflictListener;

public class SuggestionTest extends RavenDBAwareTests {
  private DocumentConvention convention;
  private HttpJsonRequestFactory factory;
  private ReplicationInformer replicationInformer;
  private ServerClient serverClient;

  @Before
  public void init() {
    convention = new DocumentConvention();
    factory = new HttpJsonRequestFactory(10);
    replicationInformer = new ReplicationInformer();

    serverClient = new ServerClient(DEFAULT_SERVER_URL, convention, null,
        new Functions.StaticFunction1<String, ReplicationInformer>(replicationInformer), null, factory,
        UUID.randomUUID(), new IDocumentConflictListener[0]);
  }

  @Test
  public void testSuggestion() throws Exception {
    try {
      createDb();
      IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());

      RavenJObject meta1 = new RavenJObject();
      meta1.add(Constants.RAVEN_ENTITY_NAME, RavenJValue.fromObject("users"));
      meta1.add(Constants.LAST_MODIFIED, RavenJValue.fromObject(new Date()));

      List<String> persons = Arrays.asList("John Smith", "Jack Johnson", "Robery Jones", "David Jones");
      for (int i = 0; i < persons.size(); i++) {
        RavenJObject document = new RavenJObject();
        document.add("FullName", new RavenJValue(persons.get(i)));
        dbCommands.put("users/" + (i+1), null, document, meta1);
      }

      IndexDefinition index = new IndexDefinition();
      index.setMap("from user in docs.users select new { user.FullName }");
      index.getIndexes().put("FullName", FieldIndexing.ANALYZED);
      dbCommands.putIndex("suggestIndex", index);

      SuggestionQuery suggestionQuery = new SuggestionQuery();
      suggestionQuery.setTerm("johne");
      suggestionQuery.setAccuracy(0.4f);
      suggestionQuery.setMaxSuggestions(5);
      suggestionQuery.setDistance(StringDistanceTypes.JARO_WINKLER);
      suggestionQuery.setPopularity(true);
      suggestionQuery.setField("FullName");

      waitForNonStaleIndexes(dbCommands);
      SuggestionQueryResult suggestionQueryResult = dbCommands.suggest("suggestIndex", suggestionQuery);
      String[] suggestions = suggestionQueryResult.getSuggestions();
      assertEquals(Arrays.asList("john", "jones", "johnson"), Arrays.asList(suggestions));

    } finally {
      deleteDb();
    }
  }
}

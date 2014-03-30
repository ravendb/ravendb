package net.ravendb.client.connection;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.data.StringDistanceTypes;
import net.ravendb.abstractions.data.SuggestionQuery;
import net.ravendb.abstractions.data.SuggestionQueryResult;
import net.ravendb.abstractions.indexing.FieldIndexing;
import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.abstractions.indexing.SuggestionOptions;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJValue;
import net.ravendb.client.RavenDBAwareTests;
import net.ravendb.client.connection.IDatabaseCommands;

import org.junit.Test;


public class SuggestionTest extends RavenDBAwareTests {

  @Test
  public void testSuggestion() throws Exception {
    try {
      createDb();
      IDatabaseCommands dbCommands = serverClient.forDatabase(getDbName());

      RavenJObject meta1 = new RavenJObject();
      meta1.add(Constants.RAVEN_ENTITY_NAME, RavenJValue.fromObject("users"));

      List<String> persons = Arrays.asList("John Smith", "Jack Johnson", "Robery Jones", "David Jones");
      for (int i = 0; i < persons.size(); i++) {
        RavenJObject document = new RavenJObject();
        document.add("FullName", new RavenJValue(persons.get(i)));
        dbCommands.put("users/" + (i+1), null, document, meta1);
      }

      IndexDefinition index = new IndexDefinition();
      index.setMap("from user in docs.users select new { user.FullName }");
      index.getIndexes().put("FullName", FieldIndexing.ANALYZED);
      SuggestionOptions suggestionOptions = new SuggestionOptions();
      suggestionOptions.setAccuracy(0.5f);
      suggestionOptions.setDistance(StringDistanceTypes.DEFAULT);
      index.getSuggestions().put("FullName", suggestionOptions);
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

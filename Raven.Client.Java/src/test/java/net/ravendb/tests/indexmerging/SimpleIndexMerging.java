package net.ravendb.tests.indexmerging;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;

import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.abstractions.indexing.IndexMergeResults;
import net.ravendb.abstractions.indexing.MergeSuggestions;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;


public class SimpleIndexMerging extends RemoteClientTest {

  @Test
  public void willSuggestMergeTwoSimpleIndexesForSameCollection() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {


      IndexDefinition index1 = new IndexDefinition();
      index1.setMap("from o in docs.Orders select new { o.Customer }");
      store.getDatabaseCommands().putIndex("test1", index1);

      IndexDefinition index2 = new IndexDefinition();
      index2.setMap("from o in docs.Orders select new { o.Email }");
      store.getDatabaseCommands().putIndex("test2", index2);

      IndexMergeResults suggestions = store.getDatabaseCommands().getIndexMergeSuggestions();
      assertEquals(1, suggestions.getSuggestions().size());

      MergeSuggestions mergeSuggestion = suggestions.getSuggestions().get(0);
      assertEquals(Arrays.asList("test1", "test2") , mergeSuggestion.getCanMerge());

      String suggestedIndexMap = removeSpaces(mergeSuggestion.getMergedIndex().getMap());
      String expectedIndexMap = removeSpaces("from doc in docs.Orders select new { Customer = doc.Customer, Email = doc.Email }");
      assertEquals(expectedIndexMap, suggestedIndexMap);

    }
  }

  protected String removeSpaces(String inputString) {
    inputString = inputString.replaceAll("\r\n\t", " ").trim();
    return inputString.replaceAll("\\s+", " ").trim();
  }
}

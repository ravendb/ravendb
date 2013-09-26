package raven.tests.indexes;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;
import raven.client.indexes.AbstractIndexCreationTask;

public class DynamicFieldIndexingTest extends RemoteClientTest {
  public static class Item {
    private String id;
    private Map<String, String> values;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public Map<String, String> getValues() {
      return values;
    }
    public void setValues(Map<String, String> values) {
      this.values = values;
    }

  }

  public static class WithDynamicIndex extends AbstractIndexCreationTask {
    public WithDynamicIndex() {
      map = "from item in docs.Items select new { _ = item.Values.Select(x=> CreateField(x.Key, x.Value))}";
    }
  }

  @Test
  public void canSearchDynamically() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new WithDynamicIndex().execute(store);
      try (IDocumentSession session = store.openSession()) {
        Item item1 = new Item();
        Map<String, String> values = new HashMap<>();
        values.put("Name", "Fitzchak");
        values.put("User", "Admin");

        item1.setValues(values);
        session.store(item1);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        List<Item> list = session.advanced().luceneQuery(Item.class, WithDynamicIndex.class)
          .waitForNonStaleResults()
          .whereEquals("Name", "Fitzchak")
          .toList();
        assertEquals(1, list.size());
      }
    }
  }

  @Test
  public void canSearchDynamicFieldWithSpaces() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new WithDynamicIndex().execute(store);
      try (IDocumentSession session = store.openSession()) {
        Item item1 = new Item();
        Map<String, String> values = new HashMap<>();
        values.put("First Name", "Fitzchak");
        values.put("User", "Admin");

        item1.setValues(values);
        session.store(item1);
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        List<Item> list = session.advanced().luceneQuery(Item.class, WithDynamicIndex.class)
          .waitForNonStaleResults(5 * 60 * 1000)
          .whereEquals("First Name", "Fitzchak")
          .toList();
        assertEquals(1, list.size());
      }
    }
  }


}

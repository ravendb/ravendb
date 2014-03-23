package net.ravendb.tests.bugs.indexing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;

import org.junit.Test;


public class IndexingOnDictionaryTest extends RemoteClientTest {

  @Test
  public void canIndexValuesForDictionary() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession s = store.openSession()) {
        User u = new User();
        HashMap<String, String> items = new HashMap<>();
        items.put("Color", "Red");
        u.setItems(items);

        s.store(u);
        s.saveChanges();
      }

      try (IDocumentSession s = store.openSession()) {
        List<User> users = s.advanced().documentQuery(User.class)
            .whereEquals("Items.Color", "Red")
            .toList();
        assertFalse(users.isEmpty());
        assertEquals("Red", users.get(0).getItems().get("Color"));
      }
    }
  }

  @Test
  public void canIndexValuesForDictionaryAsPartOfDictionary() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession s = store.openSession()) {
        User u = new User();
        HashMap<String, String> items = new HashMap<>();
        items.put("Color", "Red");
        u.setItems(items);

        s.store(u);
        s.saveChanges();
      }

      try (IDocumentSession s = store.openSession()) {
        List<User> users = s.advanced().documentQuery(User.class)
            .whereEquals("Items,Key", "Color")
            .andAlso()
            .whereEquals("Items,Value", "Red")
            .toList();
        assertFalse(users.isEmpty());
      }
    }
  }

  @Test
  public void canIndexNestedValuesForDictionaryAsPartOfDictionary() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession s = store.openSession()) {
        User u = new User();
        HashMap<String, NestedItem> items = new HashMap<>();
        NestedItem nestedItem = new NestedItem();
        nestedItem.setName("Red");
        items.put("Color", nestedItem);
        u.setNestedItems(items);

        s.store(u);
        s.saveChanges();
      }

      try (IDocumentSession s = store.openSession()) {
        List<User> users = s.advanced().documentQuery(User.class)
            .whereEquals("NestedItems,Key", "Color")
            .andAlso()
            .whereEquals("NestedItems,Value.Name", "Red")
            .toList();
        assertFalse(users.isEmpty());
      }
    }
  }

  @Test
  public void canIndexValuesForDictionaryWithNumberForIndex() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession s = store.openSession()) {
        User u = new User();
        Map<String, String> map = new HashMap<>();
        map.put("3", "Red");
        u.setItems(map);

        s.store(u);
        s.saveChanges();
      }

      try (IDocumentSession s = store.openSession()) {
        List<User> users = s.advanced().documentQuery(User.class)
            .whereEquals("Items._3", "Red").toList();
        assertFalse(users.isEmpty());
      }
    }
  }


  public static class User {
    private String id;
    private Map<String, String> items;
    private Map<String, NestedItem> nestedItems;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public Map<String, String> getItems() {
      return items;
    }
    public void setItems(Map<String, String> items) {
      this.items = items;
    }
    public Map<String, NestedItem> getNestedItems() {
      return nestedItems;
    }
    public void setNestedItems(Map<String, NestedItem> nestedItems) {
      this.nestedItems = nestedItems;
    }

  }

  public static class NestedItem {
    private String name;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

  }

}

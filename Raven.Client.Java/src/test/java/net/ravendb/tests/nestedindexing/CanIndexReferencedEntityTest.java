package net.ravendb.tests.nestedindexing;

import static org.junit.Assert.assertEquals;

import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;

import org.junit.Test;


public class CanIndexReferencedEntityTest extends RemoteClientTest {
  @Test
  public void simple() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      IndexDefinition definition = new IndexDefinition();
      definition.setMap("from i in docs.Items select new { RefName = LoadDocument(i.Ref).Name }");
      store.getDatabaseCommands().putIndex("test", definition);
      try (IDocumentSession session = store.openSession()) {
        Item item1 = new Item("items/1", "items/2", "oren");
        session.store(item1);
        Item item2 = new Item("items/2", null, "ayende");
        session.store(item2);
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        Item item = session.advanced().luceneQuery(Item.class, "test")
          .waitForNonStaleResults()
          .whereEquals("RefName", "ayende")
          .single();
        assertEquals("items/1", item.getId());
      }
    }
  }

  @Test
  public void whenReferencedItemChanges() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      IndexDefinition definition = new IndexDefinition();
      definition.setMap("from i in docs.Items select new { RefName = LoadDocument(i.Ref).Name }");
      store.getDatabaseCommands().putIndex("test", definition);
      try (IDocumentSession session = store.openSession()) {
        Item item1 = new Item("items/1", "items/2", "oren");
        session.store(item1);
        Item item2 = new Item("items/2", null, "ayende");
        session.store(item2);
        session.saveChanges();
      }
      waitForNonStaleIndexes(store.getDatabaseCommands());
      try (IDocumentSession session = store.openSession()) {
        session.load(Item.class, 2).setName("Arava");
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        Item item = session.advanced().luceneQuery(Item.class, "test")
          .waitForNonStaleResults(5 * 60 * 1000)
          .whereEquals("RefName", "arava")
          .single();
        assertEquals("items/1", item.getId());
      }
    }
  }

  @Test
  public void whenReferencedItemChangesInBatch() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      IndexDefinition definition = new IndexDefinition();
      definition.setMap("from i in docs.Items select new { RefName = LoadDocument(i.Ref).Name }");
      store.getDatabaseCommands().putIndex("test", definition);
      try (IDocumentSession session = store.openSession()) {
        Item item1 = new Item("items/1", "items/2", "oren");
        session.store(item1);
        Item item2 = new Item("items/2", null, "ayende");
        session.store(item2);
        session.saveChanges();
      }
      waitForNonStaleIndexes(store.getDatabaseCommands());
      try (IDocumentSession session = store.openSession()) {
        session.load(Item.class, 2).setName("Arava");
        session.store(new Item("items/3", null, "ayende"));
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        Item item = session.advanced().luceneQuery(Item.class, "test")
          .waitForNonStaleResults()
          .whereEquals("RefName", "arava")
          .single();
        assertEquals("items/1", item.getId());
      }
    }
  }

  @Test
  public void whenReferencedItemDeleted() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      IndexDefinition definition = new IndexDefinition();
      definition.setMap("from i in docs.Items select new { RefNameNotNull = LoadDocument(i.Ref).Name != null }");
      store.getDatabaseCommands().putIndex("test", definition);
      try (IDocumentSession session = store.openSession()) {
        Item item1 = new Item("items/1", "items/2", "oren");
        session.store(item1);
        Item item2 = new Item("items/2", null, "ayende");
        session.store(item2);
        session.saveChanges();
      }
      waitForNonStaleIndexes(store.getDatabaseCommands());
      try (IDocumentSession session = store.openSession()) {
        session.delete(session.load(Item.class, 2));
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        Item item = session.advanced().luceneQuery(Item.class, "test")
          .waitForNonStaleResults(5 * 60 * 1000)
          .whereEquals("RefNameNotNull", false)
          .single();
        assertEquals("items/1", item.getId());
      }
    }
  }

  @Test
  public void nightOfTheLivingDead() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      IndexDefinition definition = new IndexDefinition();
      definition.setMap("from i in docs.Items select new { RefName = LoadDocument(i.Ref).Name }");
      store.getDatabaseCommands().putIndex("test", definition);
      try (IDocumentSession session = store.openSession()) {
        Item item1 = new Item("items/1", "items/2", "oren");
        session.store(item1);
        Item item2 = new Item("items/2", null, "ayende");
        session.store(item2);
        session.saveChanges();
      }
      waitForNonStaleIndexes(store.getDatabaseCommands());
      try (IDocumentSession session = store.openSession()) {
        session.delete(session.load(Item.class, 2));
        session.saveChanges();
      }
      waitForNonStaleIndexes(store.getDatabaseCommands());
      try (IDocumentSession session = store.openSession()) {
        session.store(new Item("items/2", null, "Rahien"));
        session.saveChanges();
      }
      waitForNonStaleIndexes(store.getDatabaseCommands());

      try (IDocumentSession session = store.openSession()) {
        Item item = session.advanced().luceneQuery(Item.class, "test")
          .waitForNonStaleResults()
          .whereEquals("RefName", "Rahien")
          .single();
        assertEquals("items/1", item.getId());
      }
    }
  }

  @Test
  public void selfReferencing() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      IndexDefinition definition = new IndexDefinition();
      definition.setMap("from i in docs.Items select new { RefName = LoadDocument(i.Ref).Name }");
      store.getDatabaseCommands().putIndex("test", definition);
      try (IDocumentSession session = store.openSession()) {
        Item item1 = new Item("items/1", "items/1", "oren");
        session.store(item1);
        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession()) {
        session.load(Item.class, 1).setName("Ayende");
        session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        Item item = session.advanced().luceneQuery(Item.class, "test")
          .waitForNonStaleResults()
          .whereEquals("RefName", "Ayende")
          .single();
        assertEquals("items/1", item.getId());
      }
    }
  }

  @Test
  public void loops() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      IndexDefinition definition = new IndexDefinition();
      definition.setMap("from i in docs.Items select new { RefName = LoadDocument(i.Ref).Name }");
      store.getDatabaseCommands().putIndex("test", definition);
      try (IDocumentSession session = store.openSession()) {
        Item item1 = new Item("items/1", "items/2", "Oren");
        session.store(item1);
        Item item2 = new Item("items/2", "items/1", "Rahien");
        session.store(item2);
        session.saveChanges();
      }
      waitForNonStaleIndexes(store.getDatabaseCommands());
      try (IDocumentSession session = store.openSession()) {
        session.load(Item.class, 2).setName("Ayende");
        session.saveChanges();
      }
      waitForNonStaleIndexes(store.getDatabaseCommands());

      try (IDocumentSession session = store.openSession()) {
        Item item = session.advanced().luceneQuery(Item.class, "test")
          .waitForNonStaleResults()
          .whereEquals("RefName", "Ayende")
          .single();
        assertEquals("items/1", item.getId());
      }
    }
  }

}

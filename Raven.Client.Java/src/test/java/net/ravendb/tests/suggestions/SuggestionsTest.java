package net.ravendb.tests.suggestions;

import static org.junit.Assert.assertEquals;

import net.ravendb.abstractions.data.StringDistanceTypes;
import net.ravendb.abstractions.data.SuggestionQuery;
import net.ravendb.abstractions.data.SuggestionQueryResult;
import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.abstractions.indexing.SuggestionOptions;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.bugs.QUser;
import net.ravendb.tests.bugs.User;

import org.junit.Ignore;
import org.junit.Test;



public class SuggestionsTest extends RemoteClientTest {

  private void createIndexAndData(IDocumentStore store) throws Exception {
    IndexDefinition indexDefinition = new IndexDefinition();
    indexDefinition.setMap("from doc in docs select new { doc.Name }");
    indexDefinition.getSuggestions().put("Name", new SuggestionOptions());

    store.getDatabaseCommands().putIndex("Test", indexDefinition);

    try (IDocumentSession session = store.openSession()) {
      User user1 = new User();
      user1.setName("Ayende");
      session.store(user1);

      User user2 = new User();
      user2.setName("Oren");
      session.store(user2);

      session.saveChanges();

      session.query(User.class, "Test").customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults()).toList();
    }
  }

  @Test
  public void exactMatch() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      createIndexAndData(store);

      SuggestionQuery suggestionQuery = new SuggestionQuery();
      suggestionQuery.setField("Name");
      suggestionQuery.setTerm("Oren");
      suggestionQuery.setMaxSuggestions(10);

      SuggestionQueryResult suggestionQueryResult = store.getDatabaseCommands().suggest("Test", suggestionQuery);
      assertEquals(1, suggestionQueryResult.getSuggestions().length);
      assertEquals("oren", suggestionQueryResult.getSuggestions()[0]);
    }
  }

  @Test
  public void usingLinq() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      createIndexAndData(store);
      try (IDocumentSession session = store.openSession()) {
        QUser x = QUser.user;
        SuggestionQueryResult suggestionResult = session.query(User.class, "test")
          .where(x.name.eq("Oren"))
          .suggest();
        assertEquals(1, suggestionResult.getSuggestions().length);
        assertEquals("oren", suggestionResult.getSuggestions()[0]);
      }
    }
  }

  @Test
  public void usingLinq_with_typo_with_options_multiple_fields() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      createIndexAndData(store);
      try (IDocumentSession session = store.openSession()) {
        QUser x = QUser.user;
        SuggestionQueryResult suggestionQueryResult = session.query(User.class, "test")
          .where(x.name.eq("Orin"))
          .where(x.email.eq("whatever"))
          .suggest(new SuggestionQuery("Name", "Orin"));
        assertEquals(1, suggestionQueryResult.getSuggestions().length);
        assertEquals("oren", suggestionQueryResult.getSuggestions()[0]);
      }
    }
  }

  @Test
  public void usingLinq_with_typo_multiple_fields_in_reverse_order() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      createIndexAndData(store);
      try (IDocumentSession session = store.openSession()) {
        QUser x = QUser.user;
        SuggestionQueryResult suggestionQueryResult = session.query(User.class, "test")
          .where(x.email.eq("whatever"))
          .where(x.name.eq("Orin"))
          .suggest(new SuggestionQuery("Name", "Orin"));
        assertEquals(1, suggestionQueryResult.getSuggestions().length);
        assertEquals("oren", suggestionQueryResult.getSuggestions()[0]);
      }
    }
  }

  @Test
  public void usingLinq_WithOptions() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      createIndexAndData(store);
      try (IDocumentSession session = store.openSession()) {
        QUser x = QUser.user;
        SuggestionQuery suggestionQuery = new SuggestionQuery();
        suggestionQuery.setAccuracy(0.4f);

        SuggestionQueryResult suggestionQueryResult = session.query(User.class, "test")
          .where(x.name.eq("Orin"))
          .suggest(suggestionQuery);
        assertEquals(1, suggestionQueryResult.getSuggestions().length);
        assertEquals("oren", suggestionQueryResult.getSuggestions()[0]);
      }
    }
  }

  @Test
  public void withTypo() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      createIndexAndData(store);
      SuggestionQuery query = new SuggestionQuery("Name", "Oern");// intentional typo
      query.setMaxSuggestions(10);
      query.setAccuracy(0.2f);
      query.setDistance(StringDistanceTypes.LEVENSHTEIN);
      SuggestionQueryResult suggestionQueryResult = store.getDatabaseCommands().suggest("Test", query);
      assertEquals(1, suggestionQueryResult.getSuggestions().length);
      assertEquals("oren", suggestionQueryResult.getSuggestions()[0]);
    }
  }

}

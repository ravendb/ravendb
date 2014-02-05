package net.ravendb.tests.suggestions;

import static org.junit.Assert.assertEquals;

import net.ravendb.abstractions.data.StringDistanceTypes;
import net.ravendb.abstractions.data.SuggestionQuery;
import net.ravendb.abstractions.data.SuggestionQueryResult;
import net.ravendb.abstractions.indexing.SuggestionOptions;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.tests.bugs.QUser;
import net.ravendb.tests.bugs.User;

import org.junit.Ignore;
import org.junit.Test;



public class SuggestionsUsingAnIndexTest extends RemoteClientTest {

  public static class DefaultSuggestionIndex extends AbstractIndexCreationTask {
    public DefaultSuggestionIndex() {
      QUser x = QUser.user;
      map = "from user in docs.users select new { user.Name }";
      suggestion(x.name);
    }
  }

  public static class SuggestionIndex extends AbstractIndexCreationTask {
    public SuggestionIndex() {
      map = "from user in docs.users select new { user.Name}";
      QUser x = QUser.user;
      SuggestionOptions suggestionOptions = new SuggestionOptions();
      suggestion(x.name, suggestionOptions);
      suggestionOptions.setAccuracy(0.2f);
      suggestionOptions.setDistance(StringDistanceTypes.LEVENSHTEIN);
    }
  }

  @Test
  public void exactMatch() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.executeIndex(new DefaultSuggestionIndex());
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("Ayende");
        session.store(user1);

        User user2 = new User();
        user2.setName("Oren");
        session.store(user2);

        session.saveChanges();

        session.query(User.class, DefaultSuggestionIndex.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults()).toList();
      }
      try (IDocumentSession session = store.openSession()) {
        SuggestionQuery suggestionQuery = new SuggestionQuery();
        suggestionQuery.setField("Name");
        suggestionQuery.setTerm("Oren");
        suggestionQuery.setMaxSuggestions(10);
        SuggestionQueryResult suggestionQueryResult = store.getDatabaseCommands().suggest("DefaultSuggestionIndex", suggestionQuery);
        assertEquals(1, suggestionQueryResult.getSuggestions().length);
        assertEquals("oren", suggestionQueryResult.getSuggestions()[0]);
      }
    }
  }

  @Test
  public void usingLinq() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.executeIndex(new DefaultSuggestionIndex());
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("Ayende");
        session.store(user1);

        User user2 = new User();
        user2.setName("Oren");
        session.store(user2);

        session.saveChanges();

        session.query(User.class, DefaultSuggestionIndex.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults()).toList();
      }
      try (IDocumentSession session = store.openSession()) {
        QUser x = QUser.user;
        SuggestionQueryResult suggestionQueryResult = session.query(User.class, DefaultSuggestionIndex.class)
          .where(x.name.eq("Oren"))
          .suggest();
        assertEquals(1, suggestionQueryResult.getSuggestions().length);
        assertEquals("oren", suggestionQueryResult.getSuggestions()[0]);
      }
    }
  }

  @Test
  public void usingLinq_with_typo_with_options_multiple_fields() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.executeIndex(new DefaultSuggestionIndex());
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("Ayende");
        session.store(user1);

        User user2 = new User();
        user2.setName("Oren");
        session.store(user2);

        session.saveChanges();

        session.query(User.class, DefaultSuggestionIndex.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults()).toList();
      }
      try (IDocumentSession session = store.openSession()) {
        QUser x = QUser.user;

        SuggestionQuery suggestionQuery = new SuggestionQuery();
        suggestionQuery.setField("Name");
        suggestionQuery.setTerm("Orin");

        SuggestionQueryResult suggestionQueryResult = session.query(User.class, DefaultSuggestionIndex.class)
          .where(x.name.eq("Orin"))
          .where(x.email.eq("whatever"))
          .suggest(suggestionQuery);
        assertEquals(1, suggestionQueryResult.getSuggestions().length);
        assertEquals("oren", suggestionQueryResult.getSuggestions()[0]);

      }
    }
  }

  @Test
  public void usingLinq_with_typo_multiple_fields_in_reverse_order() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.executeIndex(new DefaultSuggestionIndex());
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("Ayende");
        session.store(user1);

        User user2 = new User();
        user2.setName("Oren");
        session.store(user2);

        session.saveChanges();

        session.query(User.class, DefaultSuggestionIndex.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults()).toList();
      }
      try (IDocumentSession session = store.openSession()) {
        QUser x = QUser.user;

        SuggestionQuery suggestionQuery = new SuggestionQuery();
        suggestionQuery.setField("Name");
        suggestionQuery.setTerm("Orin");

        SuggestionQueryResult suggestionQueryResult = session.query(User.class, DefaultSuggestionIndex.class)
          .where(x.email.eq("whatever"))
          .where(x.name.eq("Orin"))
          .suggest(suggestionQuery);
        assertEquals(1, suggestionQueryResult.getSuggestions().length);
        assertEquals("oren", suggestionQueryResult.getSuggestions()[0]);

      }
    }
  }

  @Test
  public void usingLinq_WithOptions() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.executeIndex(new SuggestionIndex());
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("Ayende");
        session.store(user1);

        User user2 = new User();
        user2.setName("Oren");
        session.store(user2);

        session.saveChanges();

        session.query(User.class, SuggestionIndex.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults()).toList();
      }
      try (IDocumentSession session = store.openSession()) {
        QUser x = QUser.user;
        SuggestionQuery options = new SuggestionQuery();
        options.setAccuracy(0.4f);

        SuggestionQueryResult suggestionQueryResult = session.query(User.class, SuggestionIndex.class)
          .where(x.name.eq("Orin"))
          .suggest(options);
        assertEquals(1, suggestionQueryResult.getSuggestions().length);
        assertEquals("oren", suggestionQueryResult.getSuggestions()[0]);
      }
    }
  }

  @Test
  public void withTypo() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.executeIndex(new SuggestionIndex());
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("Ayende");
        session.store(user1);

        User user2 = new User();
        user2.setName("Oren");
        session.store(user2);

        session.saveChanges();

        session.query(User.class, SuggestionIndex.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults()).toList();
      }
      try (IDocumentSession session = store.openSession()) {
        SuggestionQuery suggestionQuery = new SuggestionQuery("Name", "Oern");// intentional typo
        suggestionQuery.setMaxSuggestions(10);
        suggestionQuery.setAccuracy(0.2f);
        suggestionQuery.setDistance(StringDistanceTypes.LEVENSHTEIN);
        SuggestionQueryResult suggestionQueryResult = store.getDatabaseCommands().suggest("SuggestionIndex", suggestionQuery);
        assertEquals(1, suggestionQueryResult.getSuggestions().length);
        assertEquals("oren", suggestionQueryResult.getSuggestions()[0]);
      }
    }
  }


}

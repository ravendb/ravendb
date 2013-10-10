package raven.tests.suggestions;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import raven.abstractions.basic.Lazy;
import raven.abstractions.data.SuggestionQueryResult;
import raven.abstractions.indexing.IndexDefinition;
import raven.abstractions.indexing.SuggestionOptions;
import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentQueryCustomizationFactory;
import raven.client.document.DocumentStore;
import raven.tests.bugs.QUser;
import raven.tests.bugs.User;


public class SuggestionsLazyTest extends RemoteClientTest {

  @Test
  public void usingLinq() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
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
      try (IDocumentSession session = store.openSession()) {
        int oldRequests = session.advanced().getNumberOfRequests();

        QUser x = QUser.user;
        Lazy<SuggestionQueryResult> suggestionQueryResult = session.query(User.class, "test").where(x.name.eq("Oren")).suggestLazy();

        assertEquals(oldRequests, session.advanced().getNumberOfRequests());
        assertEquals(1, suggestionQueryResult.getValue().getSuggestions().length);
        assertEquals("oren", suggestionQueryResult.getValue().getSuggestions()[0]);

        assertEquals(oldRequests + 1, session.advanced().getNumberOfRequests());
      }
    }
  }
}

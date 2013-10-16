package net.ravendb.tests.suggestions;

import static org.junit.Assert.assertEquals;

import net.ravendb.abstractions.basic.Lazy;
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

import org.junit.Test;



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

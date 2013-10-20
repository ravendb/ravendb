package net.ravendb.tests.linq;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import net.ravendb.client.IDocumentQueryCustomization;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.listeners.IDocumentQueryListener;
import net.ravendb.tests.bugs.QUser;
import net.ravendb.tests.bugs.indexing.IndexingOnDictionaryTest.User;

import org.junit.Test;


public class RavenDB14Test extends RemoteClientTest {


  private List<String> queries = new ArrayList<>();

  @Test
  public void whereThenFirstHasAnd() throws Exception {
    try (DocumentStore store = (DocumentStore) new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.registerListener(new RecordQueriesListener(queries));

      QUser x = QUser.user;
      try (IDocumentSession session = store.openSession()) {
        session.query(User.class).where(x.name.eq("ayende").and(x.active)).firstOrDefault();

        assertEquals("Name:ayende AND Active:true", queries.get(0));
      }
    }
  }

  @Test
  public void whereThenSingleHasAnd() throws Exception {
    try (DocumentStore store = (DocumentStore) new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      store.registerListener(new RecordQueriesListener(queries));

      QUser x = QUser.user;
      try (IDocumentSession session = store.openSession()) {
        session.query(User.class).where(x.name.eq("ayende").and(x.active)).singleOrDefault();
        assertEquals("Name:ayende AND Active:true", queries.get(0));
      }
    }
  }


  public static class RecordQueriesListener implements IDocumentQueryListener {
    private final List<String> queries;

    public RecordQueriesListener(List<String> queries) {
      super();
      this.queries = queries;
    }

    @Override
    public void beforeQueryExecuted(IDocumentQueryCustomization queryCustomization) {
      queries.add(queryCustomization.toString());
    }
  }

}

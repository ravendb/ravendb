package raven.tests.mailinglist;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import raven.abstractions.basic.Reference;
import raven.abstractions.indexing.FieldIndexing;
import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RavenQueryStatistics;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;
import raven.client.indexes.AbstractIndexCreationTask;
import raven.linq.dsl.IndexExpression;
import raven.linq.dsl.expressions.AnonymousExpression;
import raven.tests.resultsTransformer.QStronglyTypedResultsTransformerTest_Order;

import com.mysema.query.annotations.QueryEntity;

public class Troy2 extends RemoteClientTest {

  @Test
  public void usingDefaultFieldWithSelectFieldsFails() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      new TesterSearch().execute(store);

      try (IDocumentSession s = store.openSession()) {
        Tester tester1 = new Tester(null, "FirstName 1", "LastName 1", "email1@test.com", "test1");
        Tester tester2 = new Tester(null, "FirstName 2", "LastName 2", "email2@test.com", "test2");

        QTroy2_Tester t = QTroy2_Tester.tester;

        s.store(tester1);
        s.store(tester2);
        s.saveChanges();

        Reference<RavenQueryStatistics> statsRef = new Reference<>();
        s.advanced().luceneQuery(Tester.class, TesterSearch.class)
            .waitForNonStaleResults()
            .statistics(statsRef)
            .usingDefaultField("Query")
            .openSubclause()
            .where("FirstName*")
            .closeSubclause()
            .andAlso()
            .whereEquals(t.email, "email1@test.com")
            .orderBy("+LastName")
            .skip(0)
            .take(10)
            .toList();

        assertEquals(1, statsRef.value.getTotalResults());

        s.advanced().luceneQuery(Tester.class, TesterSearch.class)
            .waitForNonStaleResults()
            .statistics(statsRef)
            .usingDefaultField("Query")
            .openSubclause()
            .where("FirstName*")
            .closeSubclause()
            .andAlso()
            .whereEquals(t.email, "email1@test.com")
            .orderBy("+LastName")
            .selectFields(PasswordOnly.class)
            .skip(0)
            .take(10)
            .toList();
        assertEquals(1, statsRef.value.getTotalResults());
      }
    }
  }

  public static class PasswordOnly {
    private String password;

    public String getPassword() {
      return password;
    }

    public void setPassword(String password) {
      this.password = password;
    }
  }

  public static class TesterSearch extends AbstractIndexCreationTask {

    @Override
    public String getIndexName() {
      return "Tester/Search";
    }

    @QueryEntity
    public static class SearchResult {
      private String query;
      private String fistName;
      private String lastName;
      private String email;
      private String password;
      public String getQuery() {
        return query;
      }
      public void setQuery(String query) {
        this.query = query;
      }
      public String getFistName() {
        return fistName;
      }
      public void setFistName(String fistName) {
        this.fistName = fistName;
      }
      public String getLastName() {
        return lastName;
      }
      public void setLastName(String lastName) {
        this.lastName = lastName;
      }
      public String getEmail() {
        return email;
      }
      public void setEmail(String email) {
        this.email = email;
      }
      public String getPassword() {
        return password;
      }
      public void setPassword(String password) {
        this.password = password;
      }

    }

    public TesterSearch() {

      QTroy2_TesterSearch_SearchResult s = QTroy2_TesterSearch_SearchResult.searchResult;
      QTroy2_Tester t = new QTroy2_Tester("testClass");

      map = IndexExpression.from(Tester.class)
          .select(
              new AnonymousExpression()
              .withTemplate(s.query, "new object[] { testClass.FirstName, testClass.LastName, testClass.Email }")
              .with(t.firstName)
              .with(t.lastName)
              .with(t.email)
              .with(t.password)
              );

      index(s.query, FieldIndexing.ANALYZED);
      index(s.fistName, FieldIndexing.DEFAULT);
      index(s.lastName, FieldIndexing.DEFAULT);
      index(s.email, FieldIndexing.DEFAULT);
      index(s.password, FieldIndexing.DEFAULT);
    }

  }


  @QueryEntity
  public static class Tester {


    public Tester() {
      super();
    }
    public Tester(String id, String firstName, String lastName, String email, String password) {
      super();
      this.id = id;
      this.firstName = firstName;
      this.lastName = lastName;
      this.email = email;
      this.password = password;
    }
    private String id;
    private String firstName;
    private String lastName;
    private String email;
    private String password;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public String getFirstName() {
      return firstName;
    }
    public void setFirstName(String firstName) {
      this.firstName = firstName;
    }
    public String getLastName() {
      return lastName;
    }
    public void setLastName(String lastName) {
      this.lastName = lastName;
    }
    public String getEmail() {
      return email;
    }
    public void setEmail(String email) {
      this.email = email;
    }
    public String getPassword() {
      return password;
    }
    public void setPassword(String password) {
      this.password = password;
    }


  }
}

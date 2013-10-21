package net.ravendb.tests.querying;

import static org.junit.Assert.assertEquals;

import java.util.List;

import net.ravendb.abstractions.indexing.FieldIndexing;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.tests.querying.QSearchOperatorTest_Something;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;


public class SearchOperatorTest extends RemoteClientTest {
  @QueryEntity
  public static class Something {
    private int id;
    private String myProp;
    public int getId() {
      return id;
    }
    public void setId(int id) {
      this.id = id;
    }
    public String getMyProp() {
      return myProp;
    }
    public void setMyProp(String myProp) {
      this.myProp = myProp;
    }
  }

  public static class FTSIndex extends AbstractIndexCreationTask   {
    public FTSIndex() {
      map = "from doc in docs.Somethings select new {doc.MyProp}";
      QSearchOperatorTest_Something x = QSearchOperatorTest_Something.something;
      indexes.put(x.myProp, FieldIndexing.ANALYZED);
    }
  }

  @Test
  public void dynamicLuceneQuery() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      new FTSIndex().execute(store);

      try (IDocumentSession session = store.openSession()) {
        // insert two test documents
        Something s1 = new Something();
        s1.setId(23);
        s1.setMyProp("the first string contains misspelled word sofware");
        session.store(s1);

        Something s2 = new Something();
        s2.setId(34);
        s2.setMyProp("the second string contains the word software");
        session.store(s2);

        session.saveChanges();

        // search for the keyword software
        List<Something> results = session.advanced().luceneQuery(Something.class, FTSIndex.class)
            .search("MyProp", "software").waitForNonStaleResultsAsOfLastWrite()
            .toList();
        assertEquals(1, results.size());

        results = session.advanced().luceneQuery(Something.class, FTSIndex.class)
            .search("MyProp", "software~").waitForNonStaleResultsAsOfLastWrite()
            .toList();
        assertEquals(2, results.size());


      }
    }
  }

}

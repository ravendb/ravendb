package net.ravendb.tests.indexes;

import static org.junit.Assert.assertEquals;

import java.util.List;

import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.samples.entities.GroupResult;

import org.junit.Test;


public class MapReduceIndexOnLargeDataSetTest extends RemoteClientTest {

  @Test
  public void willNotProduceAnyErrors() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      IndexDefinition indexDefinition  = new IndexDefinition();
      indexDefinition.setMap("from x in docs select new { x.Name, Count = 1}");
      indexDefinition.setReduce("from r in results group r by r.Name into g select new { Name = g.Key, Count = g.Sum(x=>x.Count) }");
      store.getDatabaseCommands().putIndex("test", indexDefinition);

      for (int i =0 ; i < 200 ;i++) {
        try (IDocumentSession s = store.openSession()) {
          for (int j =0 ; j < 25 ; j++) {
            User u = new User();
            u.setName("User #" + j);
            s.store(u);
          }
          s.saveChanges();
        }
      }

      try (IDocumentSession s = store.openSession()) {
        List<GroupResult> ret = s.query(GroupResult.class, "test" )
          .customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults(5 * 60 * 1000))
          .toList();

        assertEquals(25, ret.size());
        for (GroupResult x : ret) {
          assertEquals(200, x.getCount());
        }
      }
    }
  }

  public final static class User {
    private String name;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

  }

}

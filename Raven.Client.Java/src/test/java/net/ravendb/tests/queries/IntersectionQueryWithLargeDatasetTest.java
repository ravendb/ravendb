package net.ravendb.tests.queries;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.queries.QIntersectionQueryWithLargeDatasetTest_TestAttributes;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;


public class IntersectionQueryWithLargeDatasetTest extends RemoteClientTest {

  @Test
  public void canPerformIntersectionQuery_Remotely() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      executeTest(store);
    }
  }

  private void executeTest(IDocumentStore store) throws Exception {
    createIndexAndSampleData(store);

    // there are 10K documents, each combination of "Lorem" and "Nullam" has 100 matching documents.
    // Suspect that this may be failing because each individual slice (Lorem: L and Nullam: N)
    // has 1000 documents, which is greater than default page size of 128.

    QIntersectionQueryWithLargeDatasetTest_TestAttributes x = QIntersectionQueryWithLargeDatasetTest_TestAttributes.testAttributes;
    for (String l : lorem) {
      for (String n : nullam) {
        try (IDocumentSession session = store.openSession()) {
          List<TestAttributes> result = session.query(TestAttributes.class, "TestAttributesByAttributes")
            .where(x.attributes.containsKey("Lorem").and(x.attributes.containsValue(l)))
            .orderBy(x.id.asc())
            .intersect()
            .where(x.attributes.containsKey("Nullam").and(x.attributes.containsValue(n)))
            .toList();

            assertEquals(100, result.size());
        }
      }
    }

  }


  private void createIndexAndSampleData(IDocumentStore store) throws Exception {
    try (IDocumentSession session = store.openSession()) {
      IndexDefinition definition = new IndexDefinition();
      definition.setMap("from e in docs.TestAttributes from r in e.Attributes select new { Attributes_Key = r.Key, Attributes_Value = r.Value }");

      store.getDatabaseCommands().putIndex("TestAttributesByAttributes", definition);
      for (TestAttributes sample : getSampleData()) {
        session.store(sample);
      }

      session.saveChanges();
    }

    waitForNonStaleIndexes(store.getDatabaseCommands());
  }


  private String[] lorem =  { "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit", "Sed", "auctor", "erat" };
  private String[] nullam =  { "nec", "quam", "id", "risus", "congue", "bibendum", "Nam", "lacinia", "eros", "quis" };
  private String[] quisque = { "varius", "rutrum", "magna", "posuere", "urna", "sollicitudin", "Integer", "libero", "lacus", "tincidunt" };
  private String[] aliquam = { "erat", "volutpat", "placerat", "interdum", "felis", "luctus", "quam", "sagittis", "mattis", "Proin" };


  private List<TestAttributes> getSampleData() {
    List<TestAttributes> result = new ArrayList<>();
    for (String l : lorem) {
      for (String n : nullam) {
        for (String q : quisque) {
          for (String a : aliquam) {
            TestAttributes t = new TestAttributes();
            t.setVal(1);
            t.setAttributes(new HashMap<String, String>());
            t.getAttributes().put("Lorem", l);
            t.getAttributes().put("Nullam", n);
            t.getAttributes().put("Quisque", q);
            t.getAttributes().put("Aliquam", a);
            result.add(t);
          }
        }
      }
    }
    return result;
  }


  @QueryEntity
  public static class TestAttributes {
    private String id;
    private Map<String, String> attributes;
    private int val;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public Map<String, String> getAttributes() {
      return attributes;
    }
    public void setAttributes(Map<String, String> attributes) {
      this.attributes = attributes;
    }
    public int getVal() {
      return val;
    }
    public void setVal(int val) {
      this.val = val;
    }

  }

}

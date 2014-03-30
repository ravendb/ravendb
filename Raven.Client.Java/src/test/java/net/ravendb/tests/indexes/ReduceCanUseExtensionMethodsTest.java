package net.ravendb.tests.indexes;

import static org.junit.Assert.assertEquals;

import java.util.List;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.IndexDefinitionBuilder;
import net.ravendb.tests.indexes.QReduceCanUseExtensionMethodsTest_Result;

import org.junit.Test;

import com.mysema.query.annotations.QueryEntity;


public class ReduceCanUseExtensionMethodsTest extends RemoteClientTest {
  public static class InputData {
    private String tags;

    public InputData() {
      super();
    }

    public InputData(String tags) {
      super();
      this.tags = tags;
    }

    public String getTags() {
      return tags;
    }

    public void setTags(String tags) {
      this.tags = tags;
    }

  }
  @QueryEntity
  public static class Result {
    private String[] tags;

    public String[] getTags() {
      return tags;
    }

    public void setTags(String[] tags) {
      this.tags = tags;
    }
  }

  @Test
  public void canUseExtensionMethods() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      IndexDefinitionBuilder definitionBuilder = new IndexDefinitionBuilder();
      definitionBuilder.setMap("docs.InputDatas.Select(doc => new { " +
          "doc = doc, " +
          "tags = ((String[]) doc.Tags.Split(new char[] { " +
          "    ',' " +
          "})).Select(s => s.Trim()).Where(s => !String.IsNullOrEmpty(s)) " +
          "}).Select(this0 => new { " +
          "    Tags = Enumerable.ToArray(this0.tags) " +
          "})");
      store.getDatabaseCommands().putIndex("Hi", definitionBuilder);

      try (IDocumentSession session = store.openSession()) {
        session.store(new InputData("Little, orange, comment"));
        session.store(new InputData("only-one"));
        session.saveChanges();
      }

      waitForNonStaleIndexes(store.getDatabaseCommands());
      try (IDocumentSession session = store.openSession()) {
        QReduceCanUseExtensionMethodsTest_Result x = QReduceCanUseExtensionMethodsTest_Result.result;

        List<InputData> results = session.query(Result.class, "Hi")
            .search(x.tags, "only-one")
            .as(InputData.class)
            .toList();

        assertEquals(1, results.size());
      }
    }
  }

}

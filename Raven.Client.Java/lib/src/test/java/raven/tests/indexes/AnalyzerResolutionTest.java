package raven.tests.indexes;

import org.junit.Test;

import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;
import raven.client.indexes.IndexDefinitionBuilder;
import raven.tests.bugs.QPatching_Post;

public class AnalyzerResolutionTest extends RemoteClientTest {
  @Test
  public void can_resolve_internal_analyzer() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      QPatching_Post x = QPatching_Post.post;

      IndexDefinitionBuilder definitionBuilder =new IndexDefinitionBuilder();
      definitionBuilder.setMap("from doc in docs select new { doc.Id }");
      definitionBuilder.getAnalyzers().put(x.id, "SimpleAnalyzer");

      store.getDatabaseCommands().putIndex("test", definitionBuilder);

    }
  }
}

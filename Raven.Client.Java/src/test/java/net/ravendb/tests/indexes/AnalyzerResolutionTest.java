package net.ravendb.tests.indexes;

import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.client.indexes.IndexDefinitionBuilder;
import net.ravendb.tests.bugs.QPatchingTest_Post;

import org.junit.Test;


public class AnalyzerResolutionTest extends RemoteClientTest {
  @Test
  public void can_resolve_internal_analyzer() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      QPatchingTest_Post x = QPatchingTest_Post.post;

      IndexDefinitionBuilder definitionBuilder =new IndexDefinitionBuilder();
      definitionBuilder.setMap("from doc in docs select new { doc.Id }");
      definitionBuilder.getAnalyzers().put(x.id, "SimpleAnalyzer");

      store.getDatabaseCommands().putIndex("test", definitionBuilder);

    }
  }
}

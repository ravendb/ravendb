package raven.client.linq;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import raven.client.document.DocumentConvention;
import raven.client.indexes.QAbstractIndexCreationTaskTest_Foo;

public class LinqPathProviderTest {

  private DocumentConvention convention;
  private LinqPathProvider provider;

  public LinqPathProviderTest() {
    convention = new DocumentConvention();
    provider = new LinqPathProvider(convention);
  }

  @Test
  public void testMapCount() {
    QAbstractIndexCreationTaskTest_Foo foo = new QAbstractIndexCreationTaskTest_Foo("f");

    assertEquals("f.items.Count\\(\\)", provider.getPath(foo.items.size()).getPath());
    assertEquals("f.items.index", provider.getPath(foo.items.get("index")).getPath());
    assertEquals("f.items", provider.getPath(foo.items).getPath());
  }
}

package net.ravendb.tests.linq;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RavenQueryStatistics;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.linq.QDynamicQueriesWithStaticIndexesTest_Foo;

import org.junit.Test;


import com.mysema.query.annotations.QueryEntity;

public class DynamicQueriesWithStaticIndexesTest extends RemoteClientTest {
  @Test
  public void dynamicQueryWillInterpretFieldNamesProperly() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {

        Foo foo = new Foo();
        foo.setSomeProperty("Some Data");
        Bar bar1 = new Bar();
        foo.setBar(bar1);
        Map<String, String> map1 = new HashMap<>();
        map1.put("KeyOne", "ValueOne");
        map1.put("KeyTwo", "ValueTwo");
        bar1.setSomeDictionary(map1);
        session.store(foo);

        foo = new Foo();
        foo.setSomeProperty("Some More Data");
        session.store(foo);

        foo = new Foo();
        foo.setSomeProperty("Some Even More Data");
        Bar bar2 = new Bar();

        Map<String, String> map2 = new HashMap<>();
        map2.put("KeyThree", "ValueThree");
        bar2.setSomeDictionary(map2);
        foo.setBar(bar2);
        session.store(foo);

        foo = new Foo();
        foo.setSomeProperty("Some Even More Data");
        Map<String, String> map3 = new HashMap<>();
        map3.put("KeyFour", "ValueFour");
        Bar bar3 = new Bar();
        bar3.setSomeOtherDictionary(map3);
        foo.setBar(bar3);
        session.store(foo);

        session.saveChanges();


        IndexDefinition indexDefinition = new IndexDefinition();
        indexDefinition.setMap("from doc in docs.Foos " +
            "from docBarSomeOtherDictionaryItem in ((IEnumerable<dynamic>)doc.Bar.SomeOtherDictionary).DefaultIfEmpty() " +
            "from docBarSomeDictionaryItem in ((IEnumerable<dynamic>)doc.Bar.SomeDictionary).DefaultIfEmpty() " +
            "select new " +
            "{ " +
            "    Bar_SomeOtherDictionary_Value = docBarSomeOtherDictionaryItem.Value, " +
            "    Bar_SomeOtherDictionary_Key = docBarSomeOtherDictionaryItem.Key, " +
            "    Bar_SomeDictionary_Value = docBarSomeDictionaryItem.Value, " +
            "    Bar_SomeDictionary_Key = docBarSomeDictionaryItem.Key, " +
            "    Bar = doc.Bar " +
            "}");
        store.getDatabaseCommands().putIndex("Foos/TestDynamicQueries", indexDefinition, true);

        Reference<RavenQueryStatistics> statsRef = new Reference<>();

        QDynamicQueriesWithStaticIndexesTest_Foo f= QDynamicQueriesWithStaticIndexesTest_Foo.foo;

        List<Foo> result = session.query(Foo.class, "Foos/TestDynamicQueries")
            .where(
                f.bar.someDictionary.containsKey("KeyOne").and(f.bar.someDictionary.containsValue("ValueOne"))
                .or(f.bar.someOtherDictionary.containsKey("KeyFour").and(f.bar.someOtherDictionary.containsValue("ValueFour")))
                .or(f.bar.isNull())
                ).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults()).statistics(statsRef).toList();

        assertEquals(3, result.size());
      }
    }
  }

  @QueryEntity
  public static class Foo {
    private String someProperty;
    private Bar bar;
    public String getSomeProperty() {
      return someProperty;
    }
    public void setSomeProperty(String someProperty) {
      this.someProperty = someProperty;
    }
    public Bar getBar() {
      return bar;
    }
    public void setBar(Bar bar) {
      this.bar = bar;
    }
  }

  @QueryEntity
  public static class Bar {
    private Map<String, String> someDictionary;
    private Map<String, String> someOtherDictionary;
    public Map<String, String> getSomeDictionary() {
      return someDictionary;
    }
    public void setSomeDictionary(Map<String, String> someDictionary) {
      this.someDictionary = someDictionary;
    }
    public Map<String, String> getSomeOtherDictionary() {
      return someOtherDictionary;
    }
    public void setSomeOtherDictionary(Map<String, String> someOtherDictionary) {
      this.someOtherDictionary = someOtherDictionary;
    }

  }
}

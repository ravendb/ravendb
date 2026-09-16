using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27286(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = ["*a*b", 4])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = ["*a*b*", 4])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = ["a*b", 1])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = ["a*b*", 1])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = ["*b*a", 1])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = ["*a?b", 3])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = ["*?at*", 2])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = ["a?b", 0])]
    public void SearchWithWildcardInsideTermBehavesTheSameOnBothEngines(Options options, string pattern, int expectedCount)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Item { Name = "a-cat-b" });
            session.Store(new Item { Name = "xaéb" });
            session.Store(new Item { Name = "xa*b" });
            session.Store(new Item { Name = "a*b" });
            session.Store(new Item { Name = "b-cat-a" });
            session.SaveChanges();
        }

        new ItemsIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var results = session.Query<Item, ItemsIndex>().Search(x => x.Name, pattern).ToList();
            Assert.Equal(expectedCount, results.Count);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = ["a?b", 1])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = ["a*b", 1])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = ["?axb", 0])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, Data = ["*a?b", 4])]
    public void PatternTermMatcherIsCompatibileAcrossSearchEngines(Options options, string pattern, int expectedCount)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Item { Name = "axb" });
            session.Store(new Item { Name = "a?b" });
            session.Store(new Item { Name = "a*b" });
            session.Store(new Item { Name = "zaxb" });
            session.SaveChanges();
        }

        new ItemsIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var results = session.Query<Item, ItemsIndex>().Search(x => x.Name, pattern).ToList();
            Assert.Equal(expectedCount, results.Count);
        }
    }

    private class Item
    {
        public string Name { get; set; }
    }

    private class ItemsIndex : AbstractIndexCreationTask<Item>
    {
        public ItemsIndex()
        {
            Map = items => from item in items select new { item.Name };
            Index(x => x.Name, FieldIndexing.Search);
            Analyze(x => x.Name, "LowerCaseKeywordAnalyzer");
        }
    }
}

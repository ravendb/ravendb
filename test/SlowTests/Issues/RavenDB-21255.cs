using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21255 : RavenTestBase
{
    public RavenDB_21255(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanQueryByFullTextWithHyphens(Options options)
    {
        using var store = GetDocumentStore(options);
        new ItemIdx().Execute(store);

        using (var s = store.OpenSession())
        {
            s.Store(new Item("Dog-Cat-Snake"));
            s.SaveChanges();
        }
        
        Indexes.WaitForIndexing(store);
        
        using (var s = store.OpenSession())
        {
            var results = s.Query<Item, ItemIdx>()
                .Search(x => x.Desc, "Dog-Cat-Snake")
                .ToList();
            Assert.NotEmpty(results);
        }

        
    }

    private record Item(string Desc);

    private class ItemIdx : AbstractIndexCreationTask<Item>
    {
        public ItemIdx()
        {
            Map = items => from i in items select new { i.Desc };
            Index(x => x.Desc, FieldIndexing.Search);
        }
    }
}

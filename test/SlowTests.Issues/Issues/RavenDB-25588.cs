using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25588(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Indexes)]
    public void CanCreateIndexOnCollectionWithDoInItsName()
    {
        using var store = GetDocumentStore(new Options
        {
            ModifyDocumentStore = documentStore =>
            {
                documentStore.Conventions.FindCollectionName = t => "My." + t.Name;
            }
        });

        var index = new MyIndex
        {
            Conventions = store.Conventions
        };
        index.Execute(store);

        var indexDefinition = index.CreateIndexDefinition();
        Assert.Contains("docs[@ \"My.Item\"]", indexDefinition.Maps.First());

        using (var s = store.OpenSession())
        {
            s.Store(new Item("hello"));
            s.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        using (var s = store.OpenSession())
        {
            var x = s.Query<Item, MyIndex>()
                .Where(x => x.Name == "hello")
                .ToList();
            Assert.NotEmpty(x);
        }
    }

    private record Item(string Name);

    private class MyIndex : AbstractIndexCreationTask<Item>
    {
        public MyIndex()
        {
            Map = items =>
                from item in items
                select new { item.Name };
        }
    }
}

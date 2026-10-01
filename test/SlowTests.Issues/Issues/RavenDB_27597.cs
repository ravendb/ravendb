using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27597 : RavenTestBase
{
    public RavenDB_27597(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Tag { get; set; }
    }

    private class Items_ByNameAndTag : AbstractIndexCreationTask<Item>
    {
        public Items_ByNameAndTag()
        {
            Map = items => from i in items select new { i.Name, i.Tag };
            CompoundField(x => x.Name, x => x.Tag);
        }
    }

    private class Items_Boosted : AbstractIndexCreationTask<Item>
    {
        public Items_Boosted()
        {
            Map = items => from i in items select new { i.Name, i.Tag }.Boost(2);
            CompoundField(x => x.Name, x => x.Tag);
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void LongNonAsciiCompoundMemberIsIndexed(Options options)
    {
        var name = new string('é', 100);
        using var store = GetDocumentStore(options);
        new Items_ByNameAndTag().Execute(store);
        using (var session = store.OpenSession())
        {
            session.Store(new Item { Name = name, Tag = "red" }, "items/1");
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store, allowErrors: true);
        RavenTestHelper.AssertNoIndexErrors(store);

        using (var session = store.OpenSession())
        {
            var ids = session.Advanced.RawQuery<Item>("from index 'Items/ByNameAndTag' where Name == $name order by Tag")
                .AddParameter("name", name).ToList().Select(x => x.Id);
            Assert.Equal(new[] { "items/1" }, ids);
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void BoostedDocumentWithCompoundFieldIsIndexed(Options options)
    {
        using var store = GetDocumentStore(options);
        new Items_Boosted().Execute(store);
        using (var session = store.OpenSession())
        {
            session.Store(new Item { Name = "ann", Tag = "red" }, "items/1");
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store, allowErrors: true);
        RavenTestHelper.AssertNoIndexErrors(store);
    }
}

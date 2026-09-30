using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_27596 : RavenTestBase
{
    public RavenDB_27596(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class Items_ByChAndName : AbstractIndexCreationTask<Item>
    {
        public Items_ByChAndName()
        {
            Map = items => from i in items select new { Ch = 'A', i.Name };
            CompoundField("Ch", "Name");
        }
    }

    private class Items_ByNonAsciiChars : AbstractIndexCreationTask<Item>
    {
        public Items_ByNonAsciiChars()
        {
            Map = items => from i in items select new { E = 'é', L = 'Ł', i.Name };
            CompoundField("L", "Name");
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CharCompoundMemberMatchesItsText(Options options)
    {
        using var store = GetDocumentStore(options);
        new Items_ByChAndName().Execute(store);
        using (var session = store.OpenSession())
        {
            session.Store(new Item { Name = "ann" }, "items/1");
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        using (var session = store.OpenSession())
        {
            var ids = session.Advanced.RawQuery<Item>("from index 'Items/ByChAndName' where Ch == 'A' order by Name").ToList().Select(x => x.Id);
            Assert.Equal(new[] { "items/1" }, ids);
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void NonAsciiCharMatchesItsText(Options options)
    {
        using var store = GetDocumentStore(options);
        new Items_ByNonAsciiChars().Execute(store);
        using (var session = store.OpenSession())
        {
            session.Store(new Item { Name = "ann" }, "items/1");
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        using (var session = store.OpenSession())
        {
            foreach (var where in new[] { "where E == 'é'", "where L == 'Ł'", "where L == 'Ł' order by Name" })
            {
                var ids = session.Advanced.RawQuery<Item>($"from index 'Items/ByNonAsciiChars' {where}").ToList().Select(x => x.Id);
                Assert.Equal(new[] { "items/1" }, ids);
            }
        }
    }
}

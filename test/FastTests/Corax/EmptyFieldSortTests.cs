using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

/// <summary>
/// Guards native sort over a field that has zero distinct terms in the index (RavenDB-25281). The sort slot is
/// kept and flagged MayHaveMissingEntries, so the sort routes through InMemorySort which drains the whole bitmap
/// and treats every doc as "missing" for that field — no term tree is walked (which would NRE on a never-indexed
/// field) and no result is silently dropped. A multi-key sort whose leading key
/// is empty must therefore degenerate to the remaining tie-break key for every doc. Single-field ordering by
/// an empty field is unspecified between docs (all missing), so we only assert the full result set survives.
/// Runs Single and Sharded so the sharded local-sort/merge produces the same surviving set.
/// </summary>
public class EmptyFieldSortTests : RavenTestBase
{
    public EmptyFieldSortTests(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Tag { get; set; }   // always null -> empty terms tree for this field
        public int Rank { get; set; }
    }

    private class Items_Index : AbstractIndexCreationTask<Item>
    {
        public Items_Index()
        {
            Map = items => from i in items
                select new { i.Name, i.Tag, i.Rank };
        }
    }

    private static void Seed(IDocumentStore store)
    {
        using var s = store.OpenSession();
        for (int i = 0; i < 30; i++)
            s.Store(new Item { Id = $"items/{i}", Name = $"n{i}", Tag = null, Rank = 29 - i });
        s.SaveChanges();
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public async Task Sort_ByEmptyField_ReturnsAllDocs(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        Seed(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();

        var asc = await session.Advanced
            .AsyncRawQuery<Item>("from index 'Items/Index' order by Tag")
            .ToListAsync();
        Assert.Equal(30, asc.Count);
        Assert.Equal(30, asc.Select(x => x.Id).Distinct().Count());

        var desc = await session.Advanced
            .AsyncRawQuery<Item>("from index 'Items/Index' order by Tag desc")
            .ToListAsync();
        Assert.Equal(30, desc.Count);
        Assert.Equal(30, desc.Select(x => x.Id).Distinct().Count());
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public async Task MultiKeySort_LeadingEmptyField_FallsBackToTieBreak(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        Seed(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();

        // Tag is empty for every doc, so it is a uniform "missing" key: the order must be decided by Rank.
        var byRankAsc = await session.Advanced
            .AsyncRawQuery<Item>("from index 'Items/Index' order by Tag, Rank as long")
            .ToListAsync();
        Assert.Equal(30, byRankAsc.Count);
        Assert.Equal(Enumerable.Range(0, 30).ToList(), byRankAsc.Select(x => x.Rank).ToList());

        var byRankDesc = await session.Advanced
            .AsyncRawQuery<Item>("from index 'Items/Index' order by Tag, Rank as long desc")
            .ToListAsync();
        Assert.Equal(30, byRankDesc.Count);
        Assert.Equal(Enumerable.Range(0, 30).Reverse().ToList(), byRankDesc.Select(x => x.Rank).ToList());
    }
}

using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Tests.Infrastructure;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace SlowTests.Corax;

// Regression for SortedDrivingWithTieBreakMatch's handling of "no value" primary entries (RavenDB-25281).
// For ORDER BY <primary>, <secondary>, documents whose primary field is "no value" — whether an explicit null
// OR a missing field — form a SINGLE no-value group ordered by the SECONDARY field (null and non-existing are
// the same sort key; this matches the multi-sort coverage in RavenDB_26236 and is the documented intent). That
// group is placed before (NULLS FIRST) or after (NULLS LAST) documents that have a real primary value.
//
// Two invariants are guarded:
//  1. The no-value group is ordered by the secondary (not interleaved/merged arbitrarily).
//  2. The null and non-existing posting lists are each internally ascending but are concatenated in
//     non-ascending id order here (the missing doc has the HIGHER id, drained first); the secondary lookup
//     (Lookup.GetFor) requires globally ascending ids, so the group must be re-sorted before the secondary sort.
//     The null doc carries the LOWER secondary (Tie=1) and the missing doc the HIGHER (Tie=10), so a broken
//     re-sort would surface them in the wrong secondary order.
//
// NULLS FIRST / NULLS LAST is a Corax-only feature, so this is scoped to the Corax engine.
public class RavenDB_25281_TieBreakNullVsMissing(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [true])]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All, Data = [false])]
    public async Task NoValuePrimaryGroupIsOrderedBySecondary(Options options, bool isAutoIndex)
    {
        using var store = await CreateData(options, isAutoIndex);
        using var session = store.OpenAsyncSession();

        var from = isAutoIndex ? "from Items" : $"from index '{new ItemsIndex().IndexName}'";

        // NULLS FIRST: the no-value group comes first, ordered by the secondary (Tie asc => null(1), missing(10)),
        // then the real values by Name.
        var nullsFirst = await session.Advanced
            .AsyncRawQuery<Item>($"{from} where exists(id()) order by Name asc nulls first, Tie as long asc")
            .ToListAsync();

        Assert.Equal(4, nullsFirst.Count);
        Assert.Equal(1, nullsFirst[0].Tie);  // no-value group, secondary-sorted ascending
        Assert.Equal(10, nullsFirst[1].Tie);
        Assert.Equal("a", nullsFirst[2].Name);
        Assert.Equal("b", nullsFirst[3].Name);

        // NULLS LAST: real values first by Name, then the no-value group, still ordered by the secondary.
        var nullsLast = await session.Advanced
            .AsyncRawQuery<Item>($"{from} where exists(id()) order by Name asc nulls last, Tie as long asc")
            .ToListAsync();

        Assert.Equal(4, nullsLast.Count);
        Assert.Equal("a", nullsLast[0].Name);
        Assert.Equal("b", nullsLast[1].Name);
        Assert.Equal(1, nullsLast[2].Tie);   // no-value group, secondary-sorted ascending
        Assert.Equal(10, nullsLast[3].Tie);
    }

    private async Task<DocumentStore> CreateData(Options options, bool autoIndex)
    {
        var store = GetDocumentStore(options);
        string missingId;
        using (var session = store.OpenAsyncSession())
        {
            // Order of storage controls entry ids. The null doc is stored FIRST (lower id) with the LOWER
            // secondary; the missing doc is stored LAST (higher id) with the HIGHER secondary. PrepareNullGroup
            // drains the non-existing (missing) list before the null list, so the concatenation is [high-id,
            // low-id] — NOT globally ascending — which exercises the re-sort before the secondary lookup.
            await session.StoreAsync(new Item { Name = null, Tie = 1, Marker = "null" });
            await session.StoreAsync(new Item { Name = "a", Tie = 5, Marker = "a" });
            await session.StoreAsync(new Item { Name = "b", Tie = 6, Marker = "b" });
            var missing = new Item { Name = "to-delete", Tie = 10, Marker = "missing" };
            await session.StoreAsync(missing);
            await session.SaveChangesAsync();
            missingId = missing.Id;

            var op = await store.Operations.SendAsync(new PatchByQueryOperation(
                $"from Items where id() == '{missingId}' update {{ delete(this['Name']); }}"));
            await op.WaitForCompletionAsync();
        }

        if (autoIndex == false)
            await new ItemsIndex().ExecuteAsync(store);

        await Indexes.WaitForIndexingAsync(store);
        return store;
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int? Tie { get; set; }
        public string Marker { get; set; }
    }

    private class ItemsIndex : AbstractIndexCreationTask<Item>
    {
        public ItemsIndex()
        {
            Map = items => from i in items
                select new { i.Name, i.Tie, i.Marker };
        }
    }
}

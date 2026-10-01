using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27503 : RavenTestBase
{
    public RavenDB_27503(ITestOutputHelper output) : base(output)
    {
    }

    // SortByTerms packs the batch index into the low bits of the sort key; both tests exercise that key.
    // Defect 2: the cut to _take happens before MaybeBreakTies, so the top N is picked by batch index, not by term.
    // Needs terms equal in the 6 encoded bytes the key keeps - a 160-byte shared prefix guarantees that whatever the
    // dictionary - and an insertion order that disagrees with the term order, hence the group is stored descending.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void LimitMustNotPickTheTopNByTheTruncatedTermKey(Options options)
    {
        const int groupSize = 10, anchors = 5;
        const int limit = anchors + groupSize / 2; // the cut lands in the middle of the tie group

        using var store = GetDocumentStore(options);

        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < anchors; i++)
                bulk.Store(new Item { Key = AnchorTerm(i) }, $"items/anchor/{i}");

            for (int i = groupSize - 1; i >= 0; i--) // descending, so the batch index disagrees with the term order
                bulk.Store(new Item { Key = GroupTerm(i) }, $"items/group/{i}");
        }

        new Items_ByKey().Execute(store);
        Indexes.WaitForIndexing(store);

        var expected = Enumerable.Range(0, anchors).Select(AnchorTerm)
            .Concat(Enumerable.Range(0, groupSize).Select(GroupTerm))
            .OrderBy(x => x, StringComparer.Ordinal)
            .Take(limit)
            .ToArray();

        using var session = store.OpenSession();

        // keep documents > limit, otherwise take widens to TakeAll and nothing is cut
        var rows = Query(session, options, "where true order by Key as string", limit);

        // the set, not the order: the returned page is ordered correctly either way
        Assert.Equal(expected, rows.Select(x => x.Key).OrderBy(x => x, StringComparer.Ordinal).ToArray());
    }

    // github.com/ravendb/ravendb/issues/23570 - the same cut, reached through a filter on another field
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void FilteredPageOverIsoDateStringsReturnsTheNewestFirst(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var bulk = store.BulkInsert())
        {
            bulk.Store(new Item { Key = "2026-09-01", Category = "News" }, "items/A");
            bulk.Store(new Item { Key = "2026-08-01", Category = "News" }, "items/B");
            bulk.Store(new Item { Key = "2026-07-01", Category = "News" }, "items/C");
            bulk.Store(new Item { Key = "2026-06-01", Category = "News" }, "items/D");
        }

        new Items_ByKey().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();

        var rows = Query(session, options, "where Category = 'News' order by Key desc", limit: 2);

        Assert.Equal(new[] { "items/A", "items/B" }, rows.Select(x => x.Id).ToArray());
    }

    // 160 shared bytes then a 4-digit discriminator, every term the same length
    private static string GroupTerm(int i) => new string('a', 160) + i.ToString("D4");

    // sort before every group term and differ in byte 0, so they are in the top N on term grounds alone
    private static string AnchorTerm(int i) => $"{i}-anchor";

    // Defect 3: with no secondary comparison, equal terms came out in whatever order the sort left them.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void EqualTermsMustBeOrderedByAscendingId(Options options)
    {
        const int documents = 500;

        using var store = GetDocumentStore(options);

        // one tie group: the whole result is decided by the tie-break
        Fill(store, documents, _ => "same-term-for-everyone");

        using var session = store.OpenSession();

        var rows = Query(session, options, "where true order by Key as string");

        var expected = Enumerable.Range(0, documents).Select(i => $"items/{i:D4}").ToArray();

        Assert.Equal(expected, rows.Select(x => x.Id).ToArray());
    }

    private void Fill(IDocumentStore store, int documents, Func<int, string> key)
    {
        using (var bulk = store.BulkInsert())
        {
            // explicit padded ids so that lexicographic order == insertion order == entry id order
            for (int i = 0; i < documents; i++)
                bulk.Store(new Item { Key = key(i) }, $"items/{i:D4}");
        }

        new Items_ByKey().Execute(store);
        Indexes.WaitForIndexing(store);
    }

    // 'where true' keeps the sort in the plan and forces the in-memory batch sort instead of the sort-field scan;
    // 'as string' reaches the term comparer rather than the immune numeric one.
    private static List<Item> Query(IDocumentSession session, Options options, string rql, int? limit = null)
    {
        // RQL puts LIMIT last, after SELECT and INCLUDE
        var tail = limit is { } n ? $" limit {n}" : string.Empty;

        var rows = session.Advanced
            .RawQuery<Item>($"from index 'Items/ByKey' {rql} select id() as Id, Key include timings(){tail}")
            .Timings(out QueryTimings timings)
            .ToList();

        // guards against a vacuous pass: an elided sort or a numeric field never touches the packed key
        if (options.SearchEngineMode == RavenSearchEngineMode.Corax)
        {
            var sorting = Find(timings.QueryPlan as QueryInspectionNode, "SortingMatch");
            Assert.True(sorting != null, "no SortingMatch in the plan, the query never reached the batch sort");

            sorting.Parameters.TryGetValue("FieldType", out var fieldType);
            Assert.Equal("Sequence", fieldType);
        }

        return rows;
    }

    private static QueryInspectionNode Find(QueryInspectionNode node, string operation)
    {
        if (node == null)
            return null;
        if (node.Operation == operation)
            return node;

        foreach (var child in node.Children ?? new List<QueryInspectionNode>())
        {
            var found = Find(child, operation);
            if (found != null)
                return found;
        }

        return null;
    }

    private class Item
    {
        public string Id { get; set; }

        public string Key { get; set; }

        public string Category { get; set; }
    }

    private class Items_ByKey : AbstractIndexCreationTask<Item>
    {
        public Items_ByKey()
        {
            Map = items => from item in items
                           select new { item.Key, item.Category };
        }
    }
}

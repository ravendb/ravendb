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

public class RavenDB_27482(ITestOutputHelper output) : RavenTestBase(output)
{
    // SortByTerms packs the batch index into the low 15 bits of the sort key; all three tests exercise that key.
    private const int PackedIndexLimit = 32_768;

    // Defect 1: past 32_768 the packed index wraps, documents repeat and the order is perturbed.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void SortMustNotLoseEntriesPastThePackedIndexLimit(Options options)
    {
        // both sides of the boundary in one run, so a failure names which side broke
        var problems = new List<string>();

        problems.AddRange(CheckBoundary(options, PackedIndexLimit));     // largest batch the packing can address
        problems.AddRange(CheckBoundary(options, PackedIndexLimit + 1)); // one past it

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    private IEnumerable<string> CheckBoundary(Options options, int documents)
    {
        using var store = GetDocumentStore(options);

        // every key is distinct, so a lost or duplicated entry shows up directly in the counts
        Fill(store, documents, i => $"k-{i:D6}");

        using var session = store.OpenSession();

        // no limit, so the server asks for every entry, which is what selects the in-memory sort
        var rows = Query(session, options, "where true order by Key as string", pin: null);

        if (rows.Count != documents)
            yield return $"{documents} documents: got {rows.Count} rows";

        var distinctIds = rows.Select(x => x.Id).Distinct().Count();
        if (distinctIds != documents)
            yield return $"{documents} documents: only {distinctIds} of them are distinct, the rest are duplicates";

        var keys = rows.Select(x => x.Key).ToList();
        var unsorted = Enumerable.Range(1, Math.Max(0, keys.Count - 1))
            .FirstOrDefault(i => string.CompareOrdinal(keys[i - 1], keys[i]) > 0, -1);

        if (unsorted >= 0)
            yield return $"{documents} documents: not sorted at position {unsorted} ({keys[unsorted - 1]} then {keys[unsorted]})";
    }

    // Defect 2: the cut to _take happened before MaybeBreakTies, so the top N was picked by batch index, not by term.
    // Needs terms equal in the 6 encoded bytes the key keeps (a 160-byte shared prefix guarantees that whatever the
    // dictionary) and container ids out of term order: one indexing run inserts its terms sorted, so the group is
    // split across two runs by parity - either way the lowest ids are never {0,1,2,3,4}.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void LimitMustNotPickTheTopNByTheTruncatedTermKey(Options options)
    {
        const int groupSize = 10, anchors = 5;
        const int limit = anchors + groupSize / 2; // the cut lands in the middle of the tie group

        using var store = GetDocumentStore(options);

        new Items_ByKey().Execute(store); // index first, so each SaveChanges below is its own indexing run

        using (var session = store.OpenSession())
        {
            for (int i = 0; i < anchors; i++)
                session.Store(new Item { Key = AnchorTerm(i) }, $"items/anchor/{i}");

            for (int i = 1; i < groupSize; i += 2) // odd half
                session.Store(new Item { Key = GroupTerm(i) }, $"items/group/{i}");

            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store); // must complete first, or the two halves become one indexing run

        using (var session = store.OpenSession())
        {
            for (int i = 0; i < groupSize; i += 2) // even half
                session.Store(new Item { Key = GroupTerm(i) }, $"items/group/{i}");

            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        var expected = Enumerable.Range(0, anchors).Select(AnchorTerm)
            .Concat(Enumerable.Range(0, groupSize).Select(GroupTerm))
            .OrderBy(x => x, StringComparer.Ordinal)
            .Take(limit)
            .ToArray();

        using var read = store.OpenSession();

        // keep documents > limit, otherwise take widens to TakeAll and nothing is cut
        var rows = Query(read, options, "where true order by Key as string", pin: "InMemorySort", limit: limit);

        // the set, not the order: the returned page is ordered correctly either way
        Assert.Equal(expected, rows.Select(x => x.Key).OrderBy(x => x, StringComparer.Ordinal).ToArray());
    }

    // 160 shared bytes then a 4-digit discriminator, every term the same length
    private static string GroupTerm(int i) => new string('a', 160) + i.ToString("D4");

    // sort before every group term and differ in byte 0, so they are in the top N on term grounds alone
    private static string AnchorTerm(int i) => $"{i}-anchor";

    // Defect 3: batchTermIds.Sort(batchResults) reordered the batch by term id, so ties came out in container-page order.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void EqualTermsMustBeOrderedByAscendingId(Options options)
    {
        const int documents = 500;

        using var store = GetDocumentStore(options);

        // one tie group: the whole result is decided by the tie-break
        Fill(store, documents, _ => "same-term-for-everyone");

        using var session = store.OpenSession();

        var rows = Query(session, options, "where true order by Key as string", pin: "InMemorySort");

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

    // 'where true' keeps the sort in the plan (an Equals on the sort field would elide it); 'as string' reaches the term
    // comparer rather than the immune numeric one.
    private static List<Item> Query(IDocumentSession session, Options options, string rql, string pin, int? limit = null)
    {
        // RQL puts LIMIT last, after SELECT and INCLUDE
        var tail = limit is { } n ? $" limit {n}" : string.Empty;
        var query = session.Advanced.RawQuery<Item>($"from index 'Items/ByKey' {rql} select id() as Id, Key include timings(){tail}");

        if (pin != null)
            query = query.AddParameter("rvn_corax_sort", pin);

        var rows = query.Timings(out QueryTimings timings).ToList();

        // guards against a vacuous pass: an elided sort, a numeric field or a dropped pin never touch the packed key
        if (options.SearchEngineMode == RavenSearchEngineMode.Corax)
        {
            var sorting = Find(timings.QueryPlan as QueryInspectionNode, "SortingMatch");
            Assert.True(sorting != null, "no SortingMatch in the plan, the query never reached the batch sort");

            sorting.Parameters.TryGetValue("FieldType", out var fieldType);
            Assert.Equal("Sequence", fieldType);

            if (pin != null)
            {
                sorting.Parameters.TryGetValue("Strategy", out var strategy);
                Assert.Equal(pin, strategy);
            }
        }

        return rows;
    }

    private static QueryInspectionNode Find(QueryInspectionNode node, string operation)
    {
        if (node == null)
            return null;
        if (node.Operation == operation)
            return node;

        foreach (var child in node.Children ?? [])
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
    }

    private class Items_ByKey : AbstractIndexCreationTask<Item>
    {
        public Items_ByKey()
        {
            Map = items => from item in items
                           select new { item.Key };
        }
    }
}

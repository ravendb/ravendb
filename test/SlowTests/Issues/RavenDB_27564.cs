using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_27564 : RavenTestBase
{
    public RavenDB_27564(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }

        public long Position { get; set; }

        public bool Keep { get; set; }

        public string[] Tags { get; set; }
    }

    private class Items_ByPosition : AbstractIndexCreationTask<Item>
    {
        public Items_ByPosition()
        {
            Map = items => from i in items
                           select new
                           {
                               i.Position
                           };
        }
    }

    // one document produces one index entry per tag, so a scanned term is not a returned document
    private class Items_ByTag : AbstractIndexCreationTask<Item>
    {
        public Items_ByTag()
        {
            Map = items => from i in items
                           from t in i.Tags
                           select new
                           {
                               Tag = t
                           };
        }
    }

    // the filter rejects the first three terms the scan reaches, and the page cannot be refilled
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void FilterMustNotBeStarvedByTheTermCap(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            for (var i = 1; i <= 5; i++)
                session.Store(new Item { Id = $"items/{i}", Position = i, Keep = i > 3, Tags = new[] { "t" } });

            session.SaveChanges();
        }

        store.ExecuteIndex(new Items_ByPosition());
        Indexes.WaitForIndexing(store);

        Assert.Equal(new[] { "items/4", "items/5" },
            Ids(store, "from index 'Items/ByPosition' order by Position as long filter Keep = true limit 2"));
    }

    // items/1 owns the first five terms, so a two document page needs more than take + 1 of them
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void AFanOutIndexMustNotReturnAShortPage(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Item { Id = "items/1", Tags = new[] { "a", "b", "c", "d", "e" } });
            session.Store(new Item { Id = "items/2", Tags = new[] { "z" } });
            session.SaveChanges();
        }

        store.ExecuteIndex(new Items_ByTag());
        Indexes.WaitForIndexing(store);

        Assert.Equal(new[] { "items/1", "items/2" }, Ids(store, "from index 'Items/ByTag' order by Tag limit 2"));
        Assert.Equal(new[] { "items/2", "items/1" }, Ids(store, "from index 'Items/ByTag' order by Tag desc limit 2"));
    }

    // the scan itself survives the added conditions - this does not see the cap, which leaves no trace in the plan
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void AnOrdinaryPagedOrderByStillUsesTheScan(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            for (var i = 1; i <= 5; i++)
                session.Store(new Item { Id = $"items/{i}", Position = i, Tags = new[] { "t" } });

            session.SaveChanges();
        }

        store.ExecuteIndex(new Items_ByPosition());
        Indexes.WaitForIndexing(store);

        using var session2 = store.OpenSession();

        var results = session2.Advanced
            .RawQuery<Item>("from index 'Items/ByPosition' order by Position as long limit 2 include timings()")
            .Timings(out var timings)
            .ToList();

        Assert.Equal(new[] { "items/1", "items/2" }, results.Select(x => x.Id).ToArray());

        var plan = Assert.IsType<QueryInspectionNode>(timings.QueryPlan);
        Assert.True(PlanContains(plan, "TermNumericRangeProvider"), PlanOperations(plan));
        Assert.False(PlanContains(plan, "SortingMatch"), PlanOperations(plan));
    }

    private static string[] Ids(IDocumentStore store, string rql)
    {
        using var session = store.OpenSession();
        return session.Advanced.RawQuery<Item>(rql).ToList().Select(x => x.Id).ToArray();
    }

    private static bool PlanContains(QueryInspectionNode node, string operation)
    {
        if (node.Operation.Contains(operation))
            return true;

        if (node.Children == null)
            return false;

        return node.Children.Any(child => PlanContains(child, operation));
    }

    private static string PlanOperations(QueryInspectionNode node)
    {
        var operations = new List<string>();
        Collect(node, operations);
        return string.Join(" -> ", operations);

        static void Collect(QueryInspectionNode current, List<string> into)
        {
            into.Add(current.Operation);

            if (current.Children == null)
                return;

            foreach (var child in current.Children)
                Collect(child, into);
        }
    }
}

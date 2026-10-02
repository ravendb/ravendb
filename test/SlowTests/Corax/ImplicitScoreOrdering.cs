using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax;

public class ImplicitScoreOrdering(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void BoostedQueryIsNotOrderedByScoreWhenAutomaticOrderingIsDisabled()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.OrderByScoreAutomaticallyWhenBoostingIsInvolved)] = false.ToString();

        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Item { Name = "a" });
            session.Store(new Item { Name = "b" });
            session.SaveChanges();
        }

        new ItemsIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var results = session.Advanced
                .RawQuery<Item>($"from index '{new ItemsIndex().IndexName}' where boost(Name = 'a', 10) or Name = 'b' include timings()")
                .Timings(out QueryTimings timings)
                .ToList();

            Assert.Equal(2, results.Count);
            Assert.DoesNotContain("SortingMatch", Operations((QueryInspectionNode)timings.QueryPlan));
        }
    }

    private static IEnumerable<string> Operations(QueryInspectionNode node) =>
        (node.Children ?? new List<QueryInspectionNode>()).SelectMany(Operations).Prepend(node.Operation);

    private class ItemsIndex : AbstractIndexCreationTask<Item>
    {
        public ItemsIndex()
        {
            Map = items => from item in items
                select new { item.Name };
        }
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}

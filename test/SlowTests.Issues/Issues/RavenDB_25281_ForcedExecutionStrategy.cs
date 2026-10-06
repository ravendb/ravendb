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

namespace SlowTests.Issues
{
    // RavenDB-25281: the Corax query planner can run a query under several execution strategies
    // (BitmapPipeline, CompoundKeyLookup, CompoundSortedScan, FieldSortedScan). Which one runs is
    // normally chosen by structural candidacy plus a per-execution cost gate. The reserved query
    // parameter $rvn_corax_strategy pins a query to a specific strategy, bypassing the cost gate.
    //
    // This suite forces every strategy and asserts the forced run returns the SAME documents (and the
    // same ORDER BY-key sequence) as the BitmapPipeline baseline — i.e. every strategy is a correct
    // implementation of the same query, not just a faster one. Each test also asserts the forced
    // strategy ACTUALLY ran (read back from the plan's OptimizationHint), so a silent fallback to the
    // bitmap pipeline cannot make a correctness assertion pass vacuously.
    public class RavenDB_25281_ForcedExecutionStrategy : RavenTestBase
    {
        public RavenDB_25281_ForcedExecutionStrategy(ITestOutputHelper output) : base(output)
        {
        }

        private const int DocCount = 5_000;

        // A deliberately rare city so a compound (City, Age) prefix seek has a small driving set.
        private const string RareCity = "Vatican";
        private const int RareCityDocs = 40;

        private sealed class Item
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string City { get; set; }
            public int Age { get; set; }
        }

        private sealed class Items_Compound : AbstractIndexCreationTask<Item>
        {
            public Items_Compound()
            {
                Map = items => from i in items
                               select new { i.Name, i.City, i.Age };
                CompoundField(i => i.City, i => i.Age);
            }
        }

        private IDocumentStore GetSeededStore()
        {
            IDocumentStore store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

            var cities = new[] { "London", "Paris", "Berlin", "Madrid", "Rome" };
            var names = new[] { "alice", "bob", "carol", "dave", "erin" };
            var rng = new Random(12345);
            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < DocCount; i++)
                {
                    bulk.Store(new Item
                    {
                        Name = names[rng.Next(names.Length)],
                        City = i < RareCityDocs ? RareCity : cities[rng.Next(cities.Length)],
                        Age = rng.Next(18, 80)
                    });
                }
            }

            new Items_Compound().Execute(store);
            Indexes.WaitForIndexing(store);
            return store;
        }

        // Runs the RQL with include timings(), optionally pinning a strategy via $rvn_corax_strategy,
        // and returns the result ids (sorted, so membership compares regardless of result order), the
        // ORDER BY-key values (Age) in result order, and the strategy that actually executed.
        private static (List<string> Ids, List<long> Ages, string Strategy) Run(
            IDocumentStore store, string rql, string force, Action<IRawDocumentQuery<Item>> bind)
        {
            using IDocumentSession session = store.OpenSession();
            IRawDocumentQuery<Item> q = session.Advanced.RawQuery<Item>(rql + " include timings()").Timings(out QueryTimings timings);
            if (force != null)
                q.AddParameter("rvn_corax_strategy", force);
            bind?.Invoke(q);

            List<Item> results = q.ToList();

            var plan = (QueryInspectionNode)timings.QueryPlan;
            QueryInspectionNode compiled = FindNode(plan, "CompiledQuery");
            string strategy = null;
            compiled?.Parameters?.TryGetValue("OptimizationHint", out strategy);

            return (
                results.Select(x => x.Id).OrderBy(x => x, StringComparer.Ordinal).ToList(),
                results.Select(x => (long)x.Age).ToList(),
                strategy);
        }

        private static QueryInspectionNode FindNode(QueryInspectionNode node, string operation)
        {
            if (node == null)
                return null;
            if (node.Operation == operation)
                return node;
            if (node.Children != null)
            {
                foreach (QueryInspectionNode child in node.Children)
                {
                    QueryInspectionNode found = FindNode(child, operation);
                    if (found != null)
                        return found;
                }
            }

            return null;
        }

        // The core assertion: a query run under the forced strategy must (a) actually run that strategy,
        // (b) return exactly the same documents as the BitmapPipeline baseline, and (c) when the query has
        // an ORDER BY, emit those documents in non-decreasing Age order.
        private void AssertForcedMatchesBitmap(IDocumentStore store, string rql, string forcedStrategy, bool ordered,
            Action<IRawDocumentQuery<Item>> bind)
        {
            (List<string> Ids, List<long> Ages, string Strategy) baseline = Run(store, rql, "BitmapPipeline", bind);
            (List<string> Ids, List<long> Ages, string Strategy) forced = Run(store, rql, forcedStrategy, bind);

            // The baseline really is the bitmap pipeline, and the forced run really is the requested strategy
            // (not a silent structural fallback that would make the comparison pass for the wrong reason).
            Assert.Equal("BitmapPipeline", baseline.Strategy);
            Assert.Equal(forcedStrategy, forced.Strategy);

            // The query is non-trivial: it returns rows, so the equality below is not a vacuous 0 == 0.
            Assert.NotEmpty(baseline.Ids);

            // Same documents under both strategies.
            List<string> missing = baseline.Ids.Except(forced.Ids).ToList();
            List<string> extra = forced.Ids.Except(baseline.Ids).ToList();
            Assert.True(missing.Count == 0 && extra.Count == 0,
                $"strategy={forced.Strategy} baseline={baseline.Ids.Count} forced={forced.Ids.Count}; " +
                $"missing(in baseline, not forced)=[{string.Join(",", missing.Take(15))}] (total {missing.Count}); " +
                $"extra(in forced, not baseline)=[{string.Join(",", extra.Take(15))}] (total {extra.Count})");

            if (ordered)
            {
                // ORDER BY honored: ages are non-decreasing in the forced result's emission order.
                Assert.Equal(forced.Ages.OrderBy(x => x).ToList(), forced.Ages);
            }
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void CompoundKeyLookup_MatchesBitmapBaseline()
        {
            using IDocumentStore store = GetSeededStore();
            // City = $c AND Age = $a, where (City, Age) is exactly the compound field and together they ARE
            // the whole query — the two-equality collapse into a single composite-key seek.
            AssertForcedMatchesBitmap(store,
                "from index 'Items/Compound' where City = $c and Age = $a",
                "CompoundKeyLookup", ordered: false,
                q => { q.AddParameter("c", "London"); q.AddParameter("a", 40); });
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void CompoundSortedScan_MatchesBitmapBaseline()
        {
            using IDocumentStore store = GetSeededStore();
            // City = $c AND Age > $a ORDER BY Age — equality on the compound prefix plus a range on the
            // ordered second member. Uses the rare city so the seeked prefix is small. This is the shape that
            // historically returned 0 rows for the rare-city prefix seek; the baseline comparison catches that.
            AssertForcedMatchesBitmap(store,
                "from index 'Items/Compound' where City = $c and Age > $a order by Age as long",
                "CompoundSortedScan", ordered: true,
                q => { q.AddParameter("c", RareCity); q.AddParameter("a", 18); });
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void FieldSortedScan_NoResidual_MatchesBitmapBaseline()
        {
            using IDocumentStore store = GetSeededStore();
            // Age > $a ORDER BY Age, no second filter: a no-residual sorted tree walk.
            AssertForcedMatchesBitmap(store,
                "from index 'Items/Compound' where Age > $a order by Age as long",
                "FieldSortedScan", ordered: true,
                q => q.AddParameter("a", 18));
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void FieldSortedScan_FullScan_MatchesBitmapBaseline()
        {
            using IDocumentStore store = GetSeededStore();
            // Pure ORDER BY with no WHERE: a full-scan direct sort over the Age tree.
            AssertForcedMatchesBitmap(store,
                "from index 'Items/Compound' order by Age as long",
                "FieldSortedScan", ordered: true,
                bind: null);
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void FieldSortedScan_WithResidual_MatchesBitmapBaseline()
        {
            using IDocumentStore store = GetSeededStore();
            // Age > $a (the sort-driving range) plus a Name equality residual that the sort tree does not
            // encode — the direct scan walks Age in order and applies Name = $n as a per-entry residual.
            AssertForcedMatchesBitmap(store,
                "from index 'Items/Compound' where Age > $a and Name = $n order by Age as long",
                "FieldSortedScan", ordered: true,
                q => { q.AddParameter("a", 18); q.AddParameter("n", "alice"); });
        }
    }
}

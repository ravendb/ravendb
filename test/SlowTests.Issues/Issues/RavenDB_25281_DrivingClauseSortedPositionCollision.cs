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
    // RavenDB-25281: the de-baked residual scan lets two queries with the same op-signature share ONE compiled
    // plan, resolving fields/values per-execution. The plan's residual ClauseIndices are baked as SORTED positions
    // (the cardinality-ordered execution order) and consumed positionally, with the driving clause's sorted
    // position skipped. So the per-execution cache key (ComputeCacheKeyHash) MUST key the driving clause by its
    // sorted position.
    //
    // The regression: the driving clause was keyed by Clause.OriginalIndex (its position in the RQL text) instead
    // of its sorted position. When two executions have an identical op-signature but the driving clause lands at a
    // DIFFERENT sorted position (because the cardinality estimator reorders structurally-interchangeable clauses),
    // the OriginalIndex key collides them onto one plan. The second execution then drives the sorted scan off the
    // wrong clause and applies the wrong residual -> silently wrong results. Fixed in commit bd08b2d218a by keying
    // _exec.Executions.IndexOf(drivingClause).
    public class RavenDB_25281_DrivingClauseSortedPositionCollision : RavenTestBase
    {
        public RavenDB_25281_DrivingClauseSortedPositionCollision(ITestOutputHelper output) : base(output)
        {
        }

        private const int DocCount = 5_000;

        private sealed class Item
        {
            public string Id { get; set; }
            public int Age { get; set; }
            public int Height { get; set; }
        }

        // Plain index, NO compound field: a range on the sort field (Age) plus a range residual on a second field
        // (Height) is the FieldSortedScan-with-residual shape. Age and Height are both int ranges, so `Age > $x`
        // and `Height > $y` have the IDENTICAL op-signature (GreaterThan / Long / single-valued) and the two
        // clauses are interchangeable in the structural key — exactly the collision precondition.
        private sealed class Items_Index : AbstractIndexCreationTask<Item>
        {
            public Items_Index()
            {
                Map = items => from i in items
                               select new { i.Age, i.Height };
            }
        }

        private IDocumentStore GetSeededStore()
        {
            var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

            // Age in [18,80), Height in [150,200). The two executions only swap the RELATIVE selectivity rank of
            // Age vs Height — never the Age driving clause's absolute cardinality bucket. In BOTH executions the Age
            // clause estimates to ~2500 or ~4500 matching docs, i.e. comfortably ABOVE the 1024 cardinality-cliff
            // (QueryPrimitives.TieBreakGroupInitialCapacity). That matters: ComputeCacheKeyHash folds a cliff flag
            // (DrivingClauseCardinality <= 1024) into the key, so if the two executions landed on opposite sides of
            // the cliff they'd get distinct plans for that reason alone and the collision could never be observed.
            // Keeping both above the cliff means the ONLY differing hash input is the Age clause's sorted position.
            var rng = new Random(73456);
            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < DocCount; i++)
                {
                    bulk.Store(new Item
                    {
                        Age = rng.Next(18, 80),
                        Height = rng.Next(150, 200)
                    });
                }
            }

            new Items_Index().Execute(store);
            Indexes.WaitForIndexing(store);
            return store;
        }

        private static QueryInspectionNode FindNode(QueryInspectionNode node, string operation)
        {
            if (node == null)
                return null;
            if (node.Operation == operation)
                return node;
            foreach (var child in node.Children ?? Enumerable.Empty<QueryInspectionNode>())
            {
                var found = FindNode(child, operation);
                if (found != null)
                    return found;
            }

            return null;
        }

        // Runs the shared template, forcing FieldSortedScan so the de-baked direct-scan-with-residual path is taken
        // deterministically (no cost-gate demotion). Returns the result ids in emission order, the Age sequence
        // (to verify ORDER BY), and the strategy that actually executed.
        private static (List<string> Ids, List<int> Ages, string Strategy) Run(IDocumentStore store, int minAge, int minHeight)
        {
            using var session = store.OpenSession();
            var q = session.Advanced
                .RawQuery<Item>("from index 'Items/Index' where Age > $minAge and Height > $minHeight order by Age as long include timings()")
                .AddParameter("minAge", minAge)
                .AddParameter("minHeight", minHeight)
                .AddParameter("rvn_corax_strategy", "FieldSortedScan")
                .Timings(out QueryTimings timings);

            var results = q.ToList();
            var plan = (QueryInspectionNode)timings.QueryPlan;
            var compiled = FindNode(plan, "CompiledQuery");
            string strategy = null;
            compiled?.Parameters?.TryGetValue("OptimizationHint", out strategy);

            return (results.Select(x => x.Id).ToList(), results.Select(x => x.Age).ToList(), strategy);
        }

        private static void AssertCorrect(IDocumentStore store, List<Item> all, int minAge, int minHeight)
        {
            var (ids, ages, strategy) = Run(store, minAge, minHeight);

            // The forced strategy actually ran, so a silent fallback can't make the assertions pass vacuously.
            Assert.Equal("FieldSortedScan", strategy);

            var expected = all.Where(i => i.Age > minAge && i.Height > minHeight).OrderBy(i => i.Age).ToList();

            // Same documents.
            Assert.Equal(
                expected.Select(i => i.Id).OrderBy(x => x, StringComparer.Ordinal).ToList(),
                ids.OrderBy(x => x, StringComparer.Ordinal).ToList());

            // ORDER BY honored: ages non-decreasing in emission order.
            Assert.Equal(ages.OrderBy(x => x).ToList(), ages);

            // Non-trivial: actually returns rows (so the equality above is not 0 == 0).
            Assert.NotEmpty(ids);
        }

        // Two executions of one template, sharing a single plan-cache bucket, in which the Age (sort-driving)
        // clause lands at sorted position 0 (Age threshold selective) and then position 1 (Height threshold
        // selective). Keyed by sorted position they get distinct plans and both are correct; keyed by OriginalIndex
        // (the regression) they collide and the second silently drops/swaps the Height residual.
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void DrivingClauseAtDifferentSortedPositions_MustNotShareOnePlan()
        {
            using var store = GetSeededStore();
            List<Item> all;
            using (var session = store.OpenSession())
                all = session.Query<Item>().Customize(x => x.WaitForNonStaleResults()).Take(DocCount).ToList();

            // Exec 1: Age the MORE selective of the two (Age > 49 ~ 50% of docs, Height > 155 ~ 90%) -> Age sorts
            // first, driving clause at sorted position 0. Age card (~2500) is above the 1024 cliff. Bakes the plan.
            AssertCorrect(store, all, minAge: 49, minHeight: 155);

            // Exec 2: Age the LESS selective of the two (Age > 24 ~ 90% of docs, Height > 175 ~ 50%) -> Height sorts
            // first, the Age driving clause at sorted position 1. Age card (~4500) is STILL above the 1024 cliff, so
            // the only key difference from Exec 1 is Age's sorted position. Under the regression this reuses Exec 1's
            // plan (same OriginalIndex for Age) and applies Exec 1's residual at the wrong position -> wrong results.
            AssertCorrect(store, all, minAge: 24, minHeight: 175);

            // Run them again in the reverse warm-up order in a fresh store, so the collision is caught regardless
            // of which sorted-position plan was baked first.
            using var store2 = GetSeededStore();
            List<Item> all2;
            using (var session = store2.OpenSession())
                all2 = session.Query<Item>().Customize(x => x.WaitForNonStaleResults()).Take(DocCount).ToList();

            AssertCorrect(store2, all2, minAge: 24, minHeight: 175);
            AssertCorrect(store2, all2, minAge: 49, minHeight: 155);
        }
    }
}

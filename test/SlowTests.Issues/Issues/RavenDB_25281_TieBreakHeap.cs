using System;
using System.Collections.Generic;
using System.Globalization;
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
    // RavenDB-25281: a two-field ORDER BY under the FieldSortedScan strategy uses
    // SortedDrivingWithTieBreakMatch — it walks the primary field in sort order and, for each
    // primary term, drains the whole posting list into a per-term group and resolves the secondary
    // field to break ties. For a frequent primary term (the motivating query had ~1.3M docs under a
    // single status value) that group is huge. Rather than full-sorting the whole group on every
    // grow, the match keeps only the top-`take` survivors via a bounded max-heap once the group
    // exceeds MaxGroupSize (>= 1024). The heap only engages when the inner match receives a bounded
    // take, which the planner now threads through whenever the no-residual scan resolves a known
    // total (so statistics are still reported while the scan stays page-bounded).
    //
    // These tests build a primary group well over the 1024 truncation threshold with UNIQUE secondary
    // keys (so top-K is deterministic, no boundary-tie flakiness) and assert the FieldSortedScan run
    // returns the exact same ordered documents as the BitmapPipeline baseline — i.e. the heap-based
    // truncation selects the correct top-K and emits it in the correct order.
    public class RavenDB_25281_TieBreakHeap : RavenTestBase
    {
        public RavenDB_25281_TieBreakHeap(ITestOutputHelper output) : base(output)
        {
        }

        // Big group must exceed SortedDrivingWithTieBreakMatch.MaxGroupSize (>= 1024) so the heap
        // truncation path actually runs; 2500 leaves comfortable headroom.
        private const int BigGroupDocs = 2500;

        private sealed class Rec
        {
            public string Id { get; set; }
            public string Grp { get; set; }
            public long Score { get; set; }
            public double Rating { get; set; }
            public string Label { get; set; }
        }

        private sealed class Recs_Index : AbstractIndexCreationTask<Rec>
        {
            public Recs_Index()
            {
                Map = recs => from r in recs
                              select new { r.Grp, r.Score, r.Rating, r.Label };
            }
        }

        private IDocumentStore GetSeededStore()
        {
            IDocumentStore store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

            using (var bulk = store.BulkInsert())
            {
                // "AAA" is the alphabetically-first (and dominant, > 1024) group, so an ascending
                // primary ORDER BY draws the entire top-K page from this one group — exercising the
                // per-group heap truncation. Secondary keys are unique within the group.
                for (int i = 0; i < BigGroupDocs; i++)
                {
                    bulk.Store(new Rec
                    {
                        Grp = "AAA",
                        Score = i,
                        Rating = i + 0.5,
                        Label = "L" + i.ToString("D6", CultureInfo.InvariantCulture)
                    });
                }

                // A couple of smaller trailing groups so the walk has more than one primary term.
                foreach (var grp in new[] { "MMM", "ZZZ" })
                {
                    for (int i = 0; i < 50; i++)
                    {
                        bulk.Store(new Rec
                        {
                            Grp = grp,
                            Score = 100_000 + i,
                            Rating = 100_000 + i + 0.5,
                            Label = grp + i.ToString("D6", CultureInfo.InvariantCulture)
                        });
                    }
                }
            }

            new Recs_Index().Execute(store);
            Indexes.WaitForIndexing(store);
            return store;
        }

        // Runs the RQL with include timings(), optionally pinning a strategy via $rvn_corax_strategy,
        // and returns the ordered document ids (in result/emission order) plus the strategy that ran.
        private static (List<string> OrderedIds, string Strategy) Run(IDocumentStore store, string rql, string force)
        {
            using IDocumentSession session = store.OpenSession();
            IRawDocumentQuery<Rec> q = session.Advanced.RawQuery<Rec>(rql + " include timings()").Timings(out QueryTimings timings);
            if (force != null)
                q.AddParameter("rvn_corax_strategy", force);

            List<Rec> results = q.ToList();

            var plan = (QueryInspectionNode)timings.QueryPlan;
            QueryInspectionNode compiled = FindNode(plan, "CompiledQuery");
            string strategy = null;
            compiled?.Parameters?.TryGetValue("OptimizationHint", out strategy);

            return (results.Select(x => x.Id).ToList(), strategy);
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

        // The forced FieldSortedScan run must actually run that strategy and must return the exact
        // same ordered ids as the BitmapPipeline baseline. Secondary keys are unique, so ordering is
        // total and an exact ordered-id comparison is deterministic (no boundary-tie ambiguity).
        private void AssertFieldSortedScanMatchesBitmap(IDocumentStore store, string rql)
        {
            (List<string> OrderedIds, string Strategy) baseline = Run(store, rql, "BitmapPipeline");
            (List<string> OrderedIds, string Strategy) forced = Run(store, rql, "FieldSortedScan");

            Assert.Equal("BitmapPipeline", baseline.Strategy);
            Assert.Equal("FieldSortedScan", forced.Strategy);
            Assert.NotEmpty(baseline.OrderedIds);

            Assert.Equal(baseline.OrderedIds, forced.OrderedIds);
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void TieBreakHeap_LongSecondaryDesc_MatchesBitmapBaseline()
        {
            using IDocumentStore store = GetSeededStore();
            // The motivating shape: ORDER BY <frequent primary>, <numeric secondary> DESC with a small
            // page. All 25 results come from the dominant "AAA" group (>1024), so the per-group heap
            // picks the top-25 by Score descending.
            AssertFieldSortedScanMatchesBitmap(store,
                "from index 'Recs/Index' order by Grp, Score as long desc limit 25");
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void TieBreakHeap_LongSecondaryAsc_MatchesBitmapBaseline()
        {
            using IDocumentStore store = GetSeededStore();
            AssertFieldSortedScanMatchesBitmap(store,
                "from index 'Recs/Index' order by Grp, Score as long limit 25");
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void TieBreakHeap_DoubleSecondaryDesc_MatchesBitmapBaseline()
        {
            using IDocumentStore store = GetSeededStore();
            AssertFieldSortedScanMatchesBitmap(store,
                "from index 'Recs/Index' order by Grp, Rating as double desc limit 25");
        }

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void TieBreakHeap_StringSecondaryAsc_MatchesBitmapBaseline()
        {
            using IDocumentStore store = GetSeededStore();
            AssertFieldSortedScanMatchesBitmap(store,
                "from index 'Recs/Index' order by Grp, Label limit 25");
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

// RavenDB-25281: `Age between $lo and $hi` with BOTH bounds set to the client's open-range sentinel
// ("*" / "NULL") is rewritten by the planner to SentinelRewriteType = Exists (semantically a no-op:
// "match every document", the same as if the BETWEEN clause were absent), but ClauseType stays Between
// and PackedParamValue is left at PackedParam.None (Param1 = Param2 = NoParamValue = 0x7FFF).
//
// The bitmap/IQueryMatch resolution path (ResolveClause -> ResolveSentinelRewrittenBetween) already
// checks SentinelRewriteType and special-cases it. The entry-scan / residual-IL path did not:
// BuildScanPredicateInfoCore had no Between/SentinelRewriteType branch, so it fell through to the
// generic tail and emitted a Between residual predicate carrying ParamIndex = 0x7FFF. Once the
// entry-scan gate fires (forced here via $rvn_corax_entry_scan, since production only takes this path
// under specific cardinality heuristics), the compiled IL indexes AnalyzedSlices[0x7FFF] and crashes
// with an IndexOutOfRangeException/NullReferenceException.
public class RavenDB_25281_BetweenOpenOpenScan : RavenTestBase
{
    public RavenDB_25281_BetweenOpenOpenScan(ITestOutputHelper output) : base(output)
    {
    }

    private const int DocCount = 5_000;
    private const string RareName = "John";
    private const int RareNameDocs = 10;

    private sealed class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
        public string Status { get; set; }
    }

    private IDocumentStore GetSeededStore()
    {
        IDocumentStore store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < DocCount; i++)
            {
                bool rare = i < RareNameDocs;
                bulk.Store(new Item
                {
                    Name = rare ? RareName : $"common-{i % 50}",
                    Age = 20 + (i % 60), // every document carries a non-null Age
                    Status = rare ? "active" : (i % 2 == 0 ? "active" : "inactive"), // every John is active
                });
            }
        }

        Indexes.WaitForIndexing(store);
        return store;
    }

    // Name = $n fills slot 0 (the bitmap seed/accumulator). `Age between $lo and $hi` (both bounds the
    // open-range sentinel, a no-op) and `Status = $st` (a genuine residual predicate) are both AND'd behind
    // the entry-scan gate. The Status clause keeps the residual set non-empty (so the gate still exists and
    // BuildScanPredicateInfoCore's AlwaysTrue branch is exercised alongside a real predicate, mirroring the
    // multi-clause shape of the original crash) while the BETWEEN *,* clause must not narrow the result set.
    private const string Rql = "from Items where Name = $n and Age between $lo and $hi and Status = $st";

    private static (List<string> Ids, int EntryScanAt) Run(IDocumentStore store, long? force)
    {
        using IDocumentSession session = store.OpenSession();
        // NoCaching: the plan is re-forced across gate indices within the same test run; a client-cache
        // hit would skip server execution entirely and defeat the sweep.
        IRawDocumentQuery<Item> q = session.Advanced.RawQuery<Item>(Rql + " include timings()")
            .NoCaching()
            .Timings(out QueryTimings timings)
            .AddParameter("n", RareName)
            .AddParameter("lo", "*")
            .AddParameter("hi", "NULL")
            .AddParameter("st", "active");
        if (force.HasValue)
            q.AddParameter("rvn_corax_entry_scan", force.Value);

        List<Item> results = q.ToList();

        var plan = (QueryInspectionNode)timings.QueryPlan;
        QueryInspectionNode entryScan = FindNode(plan, "EntryScan");
        int entryScanAt = -1;
        if (entryScan?.Parameters != null
            && entryScan.Parameters.TryGetValue("Taken", out string taken)
            && string.Equals(taken, "True", StringComparison.OrdinalIgnoreCase)
            && entryScan.Parameters.TryGetValue("SwitchedAfterClauses", out string s))
        {
            int.TryParse(s, out entryScanAt);
        }

        return (results.Select(x => x.Id).OrderBy(x => x, StringComparer.Ordinal).ToList(), entryScanAt);
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

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void OpenOpenBetween_ForcedEntryScan_ReturnsNameMatchesWithoutCrashing()
    {
        using IDocumentStore store = GetSeededStore();

        // -1 disables every gate: pure bitmap pipeline. This is the correctness baseline and must equal
        // every Name='John' document, since BETWEEN *,* imposes no restriction on Age.
        (List<string> Ids, int EntryScanAt) baseline = Run(store, force: -1);
        Assert.Equal(-1, baseline.EntryScanAt);
        Assert.Equal(RareNameDocs, baseline.Ids.Count);

        // Sweep gate op-indices to find (and exercise) the entry-scan residual path for this plan. Before
        // the fix, hitting the gate crashes with IndexOutOfRangeException/NullReferenceException while
        // building/running the residual predicate for the sentinel-rewritten BETWEEN clause.
        int gate = -1;
        for (int f = 0; f <= 15; f++)
        {
            (List<string> Ids, int EntryScanAt) forced = Run(store, force: f);
            Assert.Equal(RareNameDocs, forced.Ids.Count);
            AssertSameIds(baseline.Ids, forced.Ids, forced.EntryScanAt);
            if (forced.EntryScanAt == f)
            {
                gate = f;
                break;
            }

            Assert.Equal(-1, forced.EntryScanAt);
        }

        Assert.True(gate >= 0, "expected to find an entry-scan gate for the BETWEEN *,* residual by sweeping op-indices 0..15");
    }

    private static void AssertSameIds(List<string> expected, List<string> actual, int entryScanAt)
    {
        List<string> missing = expected.Except(actual).ToList();
        List<string> extra = actual.Except(expected).ToList();
        Assert.True(missing.Count == 0 && extra.Count == 0,
            $"EntryScanAt={entryScanAt} expected={expected.Count} actual={actual.Count}; " +
            $"missing(in baseline, not actual)=[{string.Join(",", missing.Take(15))}] (total {missing.Count}); " +
            $"extra(in actual, not baseline)=[{string.Join(",", extra.Take(15))}] (total {extra.Count})");
    }
}

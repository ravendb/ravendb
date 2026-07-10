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
        public string Code { get; set; }
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
                    // Fixed-width so lexical string ordering matches numeric ordering; John's Code spans
                    // "c00".."c09" (i in [0, RareNameDocs)), giving an interior boundary for the half-open
                    // tests. Common docs straddle the bound with a low ("a"-prefixed, < Bound) and a high
                    // ("e"-prefixed, > Bound) half in roughly equal proportion, so neither Code <= Bound
                    // nor Code >= Bound is highly selective over the WHOLE index (only Name=John is) -
                    // this keeps Name as the planner's chosen seed/driving clause (index 0) instead of
                    // Code, so the BETWEEN clause lands as a genuine scan candidate (index 1+) and
                    // actually reaches the residual-scan IL under test.
                    Code = rare ? $"c{i:D2}" : (i % 2 == 0 ? $"a{i % 50:D2}" : $"e{i % 50:D2}"),
                });
            }
        }

        Indexes.WaitForIndexing(store);
        return store;
    }

    // Name = $n fills slot 0 (the bitmap seed/accumulator). The BETWEEN clause under test and
    // `Status = $st` (a genuine residual predicate) are both AND'd behind the entry-scan gate. The
    // Status clause keeps the residual set non-empty (so the gate still exists and
    // BuildScanPredicateInfoCore's sentinel-rewrite branch is exercised alongside a real predicate,
    // mirroring the multi-clause shape of the original crash).
    private const string RqlTemplate = "from Items where Name = $n and {0} between $lo and $hi and Status = $st";

    private static (List<string> Ids, int EntryScanAt) Run(IDocumentStore store, string field, object lo, object hi, long? force)
    {
        using IDocumentSession session = store.OpenSession();
        // NoCaching: the plan is re-forced across gate indices within the same test run; a client-cache
        // hit would skip server execution entirely and defeat the sweep.
        IRawDocumentQuery<Item> q = session.Advanced.RawQuery<Item>(string.Format(RqlTemplate, field) + " include timings()")
            .NoCaching()
            .Timings(out QueryTimings timings)
            .AddParameter("n", RareName)
            .AddParameter("lo", lo)
            .AddParameter("hi", hi)
            .AddParameter("st", "active");
        if (force.HasValue)
            q.AddParameter("rvn_corax_entry_scan", force.Value);

        List<Item> results = q.ToList();
        int entryScanAt = ExtractEntryScanAt(timings);
        return (results.Select(x => x.Id).OrderBy(x => x, StringComparer.Ordinal).ToList(), entryScanAt);
    }

    private static int ExtractEntryScanAt(QueryTimings timings)
    {
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

        return entryScanAt;
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
        (List<string> Ids, int EntryScanAt) baseline = Run(store, "Age", lo: "*", hi: "NULL", force: -1);
        Assert.Equal(-1, baseline.EntryScanAt);
        Assert.Equal(RareNameDocs, baseline.Ids.Count);

        // Sweep gate op-indices to find (and exercise) the entry-scan residual path for this plan. Before
        // the fix, hitting the gate crashes with IndexOutOfRangeException/NullReferenceException while
        // building/running the residual predicate for the sentinel-rewritten BETWEEN clause.
        int gate = -1;
        for (int f = 0; f <= 15; f++)
        {
            (List<string> Ids, int EntryScanAt) forced = Run(store, "Age", lo: "*", hi: "NULL", force: f);
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

    // The half-open sentinel cases can only be reached with a *string*-typed field: QueryMetadata's
    // BETWEEN type-check (VisitBetween -> AreValueTokenTypesValid) requires both resolved parameter
    // values to share a ValueTokenType, and the client's open-range sentinels ("*"/"NULL", see
    // Constants.Documents.Querying.Terms.Left/RightNullValueOfBetweenQuery) are always sent as String
    // parameters - pairing one with a Long/Double bound (e.g. a numeric Age) is rejected before the
    // query ever reaches the planner (InvalidQueryException: "Incompatible types of parameters").
    // Code is fixed-width ("c00".."c09" for John, see GetSeededStore) so lexical string ordering
    // matches the intended numeric ordering. The Name='John' subset spans "c00".."c09"; Bound sits in
    // the interior, so some Johns fall strictly below/above it and one sits exactly on it - the
    // boundary is meaningfully exercised in both the lower-open (<=) and upper-open (>=) tests.
    private const string Bound = "c04";

    // `Code between $lo and $hi` with $lo = the open-range sentinel ("*") and $hi = a real value is
    // rewritten to SentinelRewriteType = LessThanOrEqual (lower-open: "* AND hi" -> Code <= hi).
    // PackedParamValue.Param1 carries the real bound; BuildScanPredicateInfoCore must emit a
    // single-bound LessThanOrEqual scan predicate instead of falling through to the generic
    // two-sided Between tail (which would index AnalyzedSlices at the sentinel's NoParamValue slot).
    //
    // `Code between $lo and $hi` with $lo = a real value and $hi = the open-range sentinel ("NULL") is
    // rewritten to SentinelRewriteType = GreaterThanOrEqual (upper-open: "lo AND *" -> Code >= lo).
    // Both share the same Rql/Run/ExtractEntryScanAt plumbing as the open-open case above; only the
    // field and lo/hi parameter values differ.

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void LowerOpenBetween_ForcedEntryScan_BehavesAsLessThanOrEqualWithoutCrashing()
    {
        using IDocumentStore store = GetSeededStore();

        // Correctness oracle computed independently of the BETWEEN clause under test.
        List<string> expected = ExpectedIds(store, code => string.CompareOrdinal(code, Bound) <= 0);

        (List<string> Ids, int EntryScanAt) baseline = Run(store, "Code", lo: "*", hi: Bound, force: -1);
        Assert.Equal(-1, baseline.EntryScanAt);
        AssertSameIds(expected, baseline.Ids, -1);

        int gate = -1;
        for (int f = 0; f <= 15; f++)
        {
            (List<string> Ids, int EntryScanAt) forced = Run(store, "Code", lo: "*", hi: Bound, force: f);
            AssertSameIds(expected, forced.Ids, forced.EntryScanAt);
            if (forced.EntryScanAt == f)
            {
                gate = f;
                break;
            }

            Assert.Equal(-1, forced.EntryScanAt);
        }

        Assert.True(gate >= 0, "expected to find an entry-scan gate for the BETWEEN */hi residual by sweeping op-indices 0..15");
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void UpperOpenBetween_ForcedEntryScan_BehavesAsGreaterThanOrEqualWithoutCrashing()
    {
        using IDocumentStore store = GetSeededStore();

        List<string> expected = ExpectedIds(store, code => string.CompareOrdinal(code, Bound) >= 0);

        (List<string> Ids, int EntryScanAt) baseline = Run(store, "Code", lo: Bound, hi: "NULL", force: -1);
        Assert.Equal(-1, baseline.EntryScanAt);
        AssertSameIds(expected, baseline.Ids, -1);

        int gate = -1;
        for (int f = 0; f <= 15; f++)
        {
            (List<string> Ids, int EntryScanAt) forced = Run(store, "Code", lo: Bound, hi: "NULL", force: f);
            AssertSameIds(expected, forced.Ids, forced.EntryScanAt);
            if (forced.EntryScanAt == f)
            {
                gate = f;
                break;
            }

            Assert.Equal(-1, forced.EntryScanAt);
        }

        Assert.True(gate >= 0, "expected to find an entry-scan gate for the BETWEEN lo/* residual by sweeping op-indices 0..15");
    }

    // Recomputes the expected id set directly from the seeding rule (Name = John, Code = "c00".."c09"
    // for i in [0, RareNameDocs), Status = active for every John) rather than depending on the query
    // under test, so the oracle is independent of the BETWEEN rewrite being verified.
    private static List<string> ExpectedIds(IDocumentStore store, Func<string, bool> codeMatches)
    {
        using IDocumentSession session = store.OpenSession();
        List<Item> allJohns = session.Advanced.RawQuery<Item>("from Items where Name = $n and Status = $st")
            .AddParameter("n", RareName)
            .AddParameter("st", "active")
            .ToList();

        return allJohns.Where(x => codeMatches(x.Code)).Select(x => x.Id).OrderBy(x => x, StringComparer.Ordinal).ToList();
    }
}

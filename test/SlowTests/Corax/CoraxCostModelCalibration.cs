using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Corax.Querying.Matches.SortingMatches;
using Corax.Querying.Planning;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace SlowTests.Corax;

/// <summary>
/// Calibration harness for the Corax cost models / cost gates (RavenDB-25281). These are NOT pass/fail
/// correctness tests — they force each branch of a cost gate via the reserved <c>$rvn_corax_sort</c> /
/// <c>$rvn_corax_strategy</c> pins, read the real server-side <c>include timings()</c> numbers (uninstrumented,
/// the same numbers an operator sees), and emit a report from which the true cost constants are derived.
///
/// They are gated behind the RAVEN_CALIBRATE env var so they are a fast no-op in CI; set RAVEN_CALIBRATE=1
/// to run the sweep. Read the report from the test output.
///
/// Gate inventory (see also the constants they feed):
///   1. ShouldUseIndexOrderStreaming  -> IndexStreamingVsInMemorySortCostRatio (this file calibrates it)
///   2. StreamInIndexOrder bailout     -> maxScanCandidateMultiplier (this file probes the crossover)
///   3. IsDirectScanCostEffective      -> EntryScanCostMultiplier / EntryScanCountThreshold (TODO method below)
///   4. EstimateMatchesInRange         -> CalibrationBeta clamps / sample sizes (TODO method below)
/// </summary>
public class CoraxCostModelCalibration : RavenTestBase
{
    private readonly ITestOutputHelper _output;

    public CoraxCostModelCalibration(ITestOutputHelper output) : base(output) => _output = output;

    private static bool Enabled => Environment.GetEnvironmentVariable("RAVEN_CALIBRATE") == "1";

    // How many timed repetitions per measurement; we report the MIN (least noisy, closest to the real cost
    // with no GC/scheduling interference) alongside the median.
    private const int Repetitions = 7;

    /// <summary>
    /// Sort-strategy gate: for a range of (candidate fraction, limit, value cardinality) over a multi-valued
    /// sort field, force InMemorySort and IndexOrderStreaming separately and measure the SortingMatch node's
    /// own Ms. The derived ratio  (sortMs/candidates) / (streamMs/entriesStreamed)  is the real value the
    /// IndexStreamingVsInMemorySortCostRatio constant should reflect; the EntriesStreamed/candidates column
    /// shows where the maxScanCandidateMultiplier bailout should sit.
    /// </summary>
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task Calibrate_SortStrategy_StreamVsInMemory(Options options)
    {
        if (Enabled == false)
        {
            _output.WriteLine("Skipped: set RAVEN_CALIBRATE=1 to run the sort-strategy cost calibration.");
            return;
        }

        const int totalDocs = 200_000;
        using var store = await BuildItemsStore(options, totalDocs, distinctTags: 24, tagsPerDoc: 3, clustered: true);

        _output.WriteLine($"# Sort-strategy calibration  (docs={totalDocs:N0}, repetitions={Repetitions}, MIN ms)");
        _output.WriteLine("tagFreq% | candidates | take | InMemMs | StreamMs | EntriesStreamed | scan/cand | speedup | K(per-unit)");
        _output.WriteLine(new string('-', 110));

        // Pick tags whose posting list spans a range of selectivities; for each, sweep the LIMIT.
        foreach (var tag in new[] { "tag_dense", "tag_mid", "tag_rare" })
        {
            foreach (var take in new[] { 10, 25, 100, 1000 })
            {
                var inMem = await MeasureSort(store, tag, take, CoraxSortingStrategy.InMemorySort);
                var stream = await MeasureSort(store, tag, take, CoraxSortingStrategy.IndexOrderStreaming);

                double scanPerCand = stream.EntriesStreamed / (double)Math.Max(1, inMem.Candidates);
                double speedup = inMem.SortMs / Math.Max(0.0001, stream.SortMs);
                // Per-unit cost ratio: cost(sorted candidate) / cost(streamed entry).
                double perUnitK = (inMem.SortMs / Math.Max(1, inMem.Candidates))
                                  / (stream.SortMs / Math.Max(1, stream.EntriesStreamed));

                double freqPct = 100.0 * inMem.Candidates / totalDocs;
                _output.WriteLine(
                    $"{freqPct,7:F2} | {inMem.Candidates,10:N0} | {take,4} | {inMem.SortMs,7:F2} | {stream.SortMs,8:F3} | " +
                    $"{stream.EntriesStreamed,15:N0} | {scanPerCand,8:F2} | {speedup,7:F1}x | {perUnitK,8:F1}");
            }
        }

        _output.WriteLine("");
        _output.WriteLine("Interpretation: K(per-unit) is what IndexStreamingVsInMemorySortCostRatio approximates.");
        _output.WriteLine("The gate should choose streaming while  scan/cand < K(per-unit)  (i.e. speedup > 1).");
    }

    private sealed record SortMeasurement(double SortMs, long Candidates, long EntriesStreamed);

    private async Task<SortMeasurement> MeasureSort(DocumentStore store, string tag, int take, CoraxSortingStrategy strategy)
    {
        var rql = $"from index '{new ItemsIndex().IndexName}' where Tags = $t order by Tags include timings() limit {take}";

        double best = double.MaxValue;
        long candidates = 0, entriesStreamed = 0;
        for (int i = 0; i < Repetitions; i++)
        {
            using var session = store.OpenAsyncSession();
            // NoCaching is essential: without it the repeated identical queries are served from the client
            // cache, which returns the cached rows WITHOUT re-populating timings/QueryPlan (DurationInMs=0).
            var q = session.Advanced.AsyncRawQuery<ItemDoc>(rql)
                .AddParameter("t", tag)
                .AddParameter("rvn_corax_sort", strategy.ToString())
                .NoCaching();
            await q.Timings(out var timings).ToListAsync();

            var sort = FindSortingMatch((QueryInspectionNode)timings.QueryPlan);
            Assert.NotNull(sort);
            // Confirm the pin actually took (a pin that can't apply is silently ignored).
            Assert.Equal(strategy.ToString(), sort.Parameters["Strategy"]);

            double ms = sort.Parameters.TryGetValue("Ms", out var msStr)
                ? double.Parse(msStr, CultureInfo.InvariantCulture)
                : 0;
            best = Math.Min(best, ms);
            candidates = long.Parse(sort.Parameters["Incoming"], NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
            if (sort.Parameters.TryGetValue("EntriesStreamed", out var es))
                entriesStreamed = long.Parse(es, NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
        }

        if (entriesStreamed == 0)
            entriesStreamed = candidates; // InMemorySort doesn't stream; use candidates so the per-unit math stays defined.

        return new SortMeasurement(best, candidates, entriesStreamed);
    }

    private static QueryInspectionNode FindSortingMatch(QueryInspectionNode node)
    {
        if (node.Operation == "SortingMatch")
            return node;
        if (node.Children == null)
            return null;
        foreach (var child in node.Children)
        {
            var found = FindSortingMatch(child);
            if (found != null)
                return found;
        }

        return null;
    }

    /// <summary>
    /// Streaming bailout gate (<c>maxScanCandidateMultiplier</c>): force IndexOrderStreaming across a range of
    /// candidate selectivities so the over-scan ratio (EntriesStreamed / candidates) spans a wide range, and
    /// measure where streaming stops beating InMemorySort. The bailout multiplier should sit just below that
    /// crossover so the walk is abandoned only once it is genuinely losing.
    /// </summary>
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task Calibrate_StreamingBailout_OverScan(Options options)
    {
        if (Enabled == false)
        {
            _output.WriteLine("Skipped: set RAVEN_CALIBRATE=1 to run the streaming-bailout calibration.");
            return;
        }

        const int totalDocs = 200_000;
        using var store = await BuildItemsStore(options, totalDocs, distinctTags: 24, tagsPerDoc: 3, clustered: true);

        _output.WriteLine($"# Streaming-bailout calibration (docs={totalDocs:N0}, reps={Repetitions}, MIN ms)");
        _output.WriteLine("tagFreq% | candidates | take | InMemMs | StreamMs | EntriesStreamed | over-scan | speedup");
        _output.WriteLine(new string('-', 90));

        // take is half the candidate count so streaming cannot trivially early-terminate, forcing a deep walk
        // whose over-scan ratio grows as the candidate set gets rarer/more clustered.
        foreach (var tag in new[] { "tag_dense", "tag_mid", "tag_rare" })
        {
            var inMem = await MeasureSort(store, tag, take: 5, CoraxSortingStrategy.InMemorySort);
            int take = Math.Max(1, (int)(inMem.Candidates / 2));
            var inMemDeep = await MeasureSort(store, tag, take, CoraxSortingStrategy.InMemorySort);
            var stream = await MeasureSort(store, tag, take, CoraxSortingStrategy.IndexOrderStreaming);

            double overScan = stream.EntriesStreamed / (double)Math.Max(1, inMemDeep.Candidates);
            double speedup = inMemDeep.SortMs / Math.Max(0.0001, stream.SortMs);
            double freqPct = 100.0 * inMemDeep.Candidates / totalDocs;
            _output.WriteLine($"{freqPct,7:F2} | {inMemDeep.Candidates,10:N0} | {take,6} | {inMemDeep.SortMs,7:F2} | " +
                              $"{stream.SortMs,8:F3} | {stream.EntriesStreamed,15:N0} | {overScan,9:F1} | {speedup,6:F1}x");
        }

        _output.WriteLine("");
        _output.WriteLine("Set maxScanCandidateMultiplier just below the over-scan where speedup crosses 1.0.");
    }

    /// <summary>
    /// DirectScan-vs-Bitmap plan gate: force FieldSortedScan and BitmapPipeline for a top-N-by-single-valued-field
    /// query and measure end-to-end. FieldSortedScan walks the sort field's term tree and early-terminates at the
    /// limit; BitmapPipeline materializes every entry then heap-sorts. This validates (and quantifies) the gate's
    /// preference for the sorted walk. (The residual-filtered 64×/32K cost constants need gate-internal
    /// entriesToScan/bitmapCost introspection — a follow-up, like the sort gate's StreamScanEstimate.)
    /// </summary>
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task Calibrate_DirectScan_VsBitmap(Options options)
    {
        if (Enabled == false)
        {
            _output.WriteLine("Skipped: set RAVEN_CALIBRATE=1 to run the DirectScan-vs-Bitmap calibration.");
            return;
        }

        const int totalDocs = 200_000;
        using var store = await BuildItemsStore(options, totalDocs, distinctTags: 24, tagsPerDoc: 3, clustered: true);

        _output.WriteLine($"# DirectScan-vs-Bitmap calibration (docs={totalDocs:N0}, reps={Repetitions}, MIN ms end-to-end)");
        _output.WriteLine("take | DirectMs | BitmapMs | speedup | directHint | bitmapHint");
        _output.WriteLine(new string('-', 80));

        foreach (var take in new[] { 10, 25, 100, 1000, 10000 })
        {
            var direct = await MeasureScan(store, take, ExecutionStrategy.FieldSortedScan);
            var bitmap = await MeasureScan(store, take, ExecutionStrategy.BitmapPipeline);
            double speedup = bitmap.Ms / Math.Max(0.0001, direct.Ms);
            _output.WriteLine($"{take,4} | {direct.Ms,8:F3} | {bitmap.Ms,8:F3} | {speedup,6:F1}x | {direct.Hint,-16} | {bitmap.Hint}");
        }

        _output.WriteLine("");
        _output.WriteLine("DirectScan should win for small take (early termination) and converge to Bitmap as take -> indexSize.");
    }

    /// <summary>
    /// DirectScan scan-size cap (EntryScanCountThreshold=32K): a selective residual (Score &lt; $r, ~0.2%) drives
    /// entriesToScan = take/passRate past 32K, and the pin forces FieldSortedScan past the cap so we can time the
    /// over-scan. Where the forced scan stops beating Bitmap is the real ceiling — validate 32K sits below it.
    /// (Note: on a 200K index the cap never *flips a cost decision* — that needs bitmap_cost > 32K*64 ≈ 2M — but
    /// this measures whether the ceiling itself is in the right place, which is the cap's actual job.)
    /// </summary>
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task Calibrate_DirectScanCap_ScanCeiling(Options options)
    {
        if (Enabled == false)
        {
            _output.WriteLine("Skipped: set RAVEN_CALIBRATE=1 to run the DirectScan scan-cap calibration.");
            return;
        }

        const int totalDocs = 200_000;
        using var store = await BuildItemsStore(options, totalDocs, distinctTags: 24, tagsPerDoc: 3, clustered: true);

        _output.WriteLine($"# DirectScan scan-cap calibration (docs={totalDocs:N0}, reps={Repetitions}, MIN ms; Score<200 residual ~0.4%)");
        _output.WriteLine("take | scanEntries | >32K cap? | gatePick | DirectMs | BitmapMs | speedup | actualWin");
        _output.WriteLine(new string('-', 100));

        // Category < 900 drives the sorted walk (90% of docs); Score < 200 (~0.4%) is the selective residual that
        // inflates entriesToScan = take/passRate. Sweeping take walks entriesToScan through the 32K boundary.
        foreach (var take in new[] { 10, 40, 80, 160, 320 })
        {
            var (scanEntries, capExceeded, gatePick) = await ReadCapGate(store, take);
            var direct = await MeasureCap(store, take, ExecutionStrategy.FieldSortedScan);
            var bitmap = await MeasureCap(store, take, ExecutionStrategy.BitmapPipeline);
            double speedup = bitmap.Ms / Math.Max(0.0001, direct.Ms);
            string win = direct.Ms < bitmap.Ms ? "Direct" : "Bitmap";
            _output.WriteLine($"{take,4} | {scanEntries,11:N0} | {(capExceeded ? "yes" : "no"),-9} | {gatePick,-8} | " +
                              $"{direct.Ms,8:F3} | {bitmap.Ms,8:F3} | {speedup,6:F1}x | {win}");
        }

        _output.WriteLine("");
        _output.WriteLine("If gatePick=scan where actualWin=Bitmap, the gate picks the slower plan: bitmapCost=Sum(cardinalities)");
        _output.WriteLine("over-values the bitmap when one clause is tiny (it leads with the small clause).");
    }

    private string CapRql(int take) =>
        $"from index '{new ItemsIndex().IndexName}' where Category < 900 and Score < $r order by Category as long include timings() limit {take}";

    private async Task<(long scanEntries, bool capExceeded, string gatePick)> ReadCapGate(DocumentStore store, int take)
    {
        using var session = store.OpenAsyncSession();
        var q = session.Advanced.AsyncRawQuery<ItemDoc>(CapRql(take))
            .AddParameter("r", 200).NoCaching();
        await q.Timings(out var timings).ToListAsync();

        var trail = FindNode((QueryInspectionNode)timings.QueryPlan, "DecisionTrail");
        var fieldScan = trail?.Children?.FirstOrDefault(c => c.Operation == "FieldSortedScan");
        if (fieldScan == null || fieldScan.Parameters.TryGetValue("PerExecution", out var per) == false)
            return (-1, false, "n/a");
        long scan = ExtractParen(per, "entries_to_scan");
        string pick = per.Contains("→ scan", StringComparison.Ordinal) ? "scan" : "bitmap";
        return (scan, per.Contains("> cap(", StringComparison.Ordinal), pick);
    }

    private async Task<ScanMeasurement> MeasureCap(DocumentStore store, int take, ExecutionStrategy strategy)
    {
        double best = double.MaxValue;
        string hint = null;
        for (int i = 0; i < Repetitions; i++)
        {
            using var session = store.OpenAsyncSession();
            var q = session.Advanced.AsyncRawQuery<ItemDoc>(CapRql(take))
                .AddParameter("r", 200)
                .AddParameter("rvn_corax_strategy", strategy.ToString())
                .NoCaching();
            var sw = Stopwatch.StartNew();
            await q.Timings(out var timings).ToListAsync();
            sw.Stop();
            best = Math.Min(best, sw.Elapsed.TotalMilliseconds);
            var root = (QueryInspectionNode)timings.QueryPlan;
            hint = root.Parameters.TryGetValue("OptimizationHint", out var h) ? h : root.Operation;
        }

        return new ScanMeasurement(best, hint);
    }

    /// <summary>
    /// DirectScan residual-filtered cost gate (EntryScanCostMultiplier=64, EntryScanCountThreshold=32K): for a
    /// range-on-sort-field + residual query, read the gate's own entriesToScan / bitmapCost / verdict from the
    /// DecisionTrail's PerExecution string (unpinned run), then time FieldSortedScan and BitmapPipeline forced.
    /// Where the gate's pick disagrees with the actually-faster branch, the 64x multiplier (or 32K cap) is off;
    /// the real multiplier is bitmapCost/entriesToScan at the timing crossover.
    /// </summary>
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task Calibrate_DirectScanResidual_Multiplier(Options options)
    {
        if (Enabled == false)
        {
            _output.WriteLine("Skipped: set RAVEN_CALIBRATE=1 to run the DirectScan residual-multiplier calibration.");
            return;
        }

        const int totalDocs = 200_000;
        using var store = await BuildItemsStore(options, totalDocs, distinctTags: 24, tagsPerDoc: 3, clustered: true);

        _output.WriteLine($"# DirectScan residual-multiplier calibration (docs={totalDocs:N0}, reps={Repetitions}, MIN ms)");
        _output.WriteLine("hi | take | scanEntries | bitmapCost | gate(N*64 vs M) | gatePick | DirectMs | BitmapMs | actualWin | M_real(bitmap/scan)");
        _output.WriteLine(new string('-', 120));

        foreach (var hi in new[] { 50, 200, 1000 })
        {
            foreach (var take in new[] { 10, 100, 1000 })
            {
                var (scanEntries, bitmapCost, gatePick) = await ReadResidualGate(store, hi, take);
                var direct = await MeasureResidual(store, hi, take, ExecutionStrategy.FieldSortedScan);
                var bitmap = await MeasureResidual(store, hi, take, ExecutionStrategy.BitmapPipeline);
                string actualWin = direct.Ms < bitmap.Ms ? "Direct" : "Bitmap";
                double mReal = scanEntries > 0 ? bitmapCost / (double)scanEntries : -1;
                _output.WriteLine($"{hi,4} | {take,4} | {scanEntries,11:N0} | {bitmapCost,10:N0} | {gatePick,-10} | " +
                                  $"{direct.Ms,8:F3} | {bitmap.Ms,8:F3} | {actualWin,-9} | {mReal,8:F1}");
            }
        }

        _output.WriteLine("");
        _output.WriteLine("EntryScanCostMultiplier should approximate M_real on the rows where actualWin flips Direct<->Bitmap.");
    }

    // Residual-filtered shape: range on the single-valued sort field (Category) drives the scan, Bucket=$b is the
    // per-document residual. order by Category makes FieldSortedScan the structural candidate.
    private string ResidualRql(int take) =>
        $"from index '{new ItemsIndex().IndexName}' where Category < $hi and Bucket = $b order by Category as long include timings() limit {take}";

    private async Task<(long scanEntries, long bitmapCost, string gatePick)> ReadResidualGate(DocumentStore store, int hi, int take)
    {
        using var session = store.OpenAsyncSession();
        // Unpinned: let the gate decide so PerExecution carries the entries_to_scan/bitmap_cost numbers + verdict.
        var q = session.Advanced.AsyncRawQuery<ItemDoc>(ResidualRql(take))
            .AddParameter("hi", hi).AddParameter("b", 3).NoCaching();
        await q.Timings(out var timings).ToListAsync();

        var trail = FindNode((QueryInspectionNode)timings.QueryPlan, "DecisionTrail");
        var fieldScan = trail?.Children?.FirstOrDefault(c => c.Operation == "FieldSortedScan");
        if (fieldScan == null || fieldScan.Parameters.TryGetValue("PerExecution", out var per) == false)
            return (-1, -1, "n/a");

        long scan = ExtractParen(per, "entries_to_scan");
        long bitmap = ExtractParen(per, "bitmap_cost");
        string pick = per.Contains("→ scan") ? "scan" : "bitmap";
        return (scan, bitmap, pick);
    }

    /// <summary>
    /// Range-estimate gate (EstimateMatchesInRange, CalibrationBeta clamp [0.25, 4]): for wide ranges over a
    /// high-cardinality field (so the estimator samples edges + extrapolates the middle), compare its EstimatedRows
    /// against the actual TotalResults. Repeating the same query shows the per-clause calibration EWMA converging;
    /// if the estimate stays badly biased the Beta clamp / sample sizes are off.
    /// </summary>
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task Calibrate_RangeEstimate_Accuracy(Options options)
    {
        if (Enabled == false)
        {
            _output.WriteLine("Skipped: set RAVEN_CALIBRATE=1 to run the range-estimate accuracy calibration.");
            return;
        }

        const int totalDocs = 200_000;
        using var store = await BuildItemsStore(options, totalDocs, distinctTags: 24, tagsPerDoc: 3, clustered: true);

        _output.WriteLine($"# Range-estimate accuracy calibration (docs={totalDocs:N0})");
        _output.WriteLine("field    | dist     | hi | actual | iter1_est | err1 | iter5_est | err5 | sampled (rangeTerms/sampledTerms)");
        _output.WriteLine(new string('-', 110));

        // Score = uniform 0..50K; SkewScore = triangular peaked at 25K (sparse edges, dense middle).
        // Both span far past the 512+256 edge samples, so the middle must be extrapolated (Beta's domain).
        foreach (var (field, dist) in new[] { ("Score", "uniform"), ("SkewScore", "triangular") })
        {
            foreach (var hi in new[] { 2000, 10000, 25000, 40000 })
            {
                string sampling = null;
                long actual = 0, est1 = 0, est5 = 0;
                for (int iter = 1; iter <= 5; iter++)
                {
                    using var session = store.OpenAsyncSession();
                    var q = session.Advanced.AsyncRawQuery<ItemDoc>(
                            $"from index '{new ItemsIndex().IndexName}' where {field} < $hi include timings() limit 1")
                        .AddParameter("hi", hi).Statistics(out var stats).NoCaching();
                    await q.Timings(out var timings).ToListAsync();

                    actual = stats.TotalResults;
                    var clause = FindWithParam((QueryInspectionNode)timings.QueryPlan, "EstimatedRows");
                    long est = clause != null ? ExtractFormatted(clause.Parameters["EstimatedRows"]) : -1;
                    if (iter == 1)
                    {
                        est1 = est;
                        if (clause != null && clause.Parameters.TryGetValue("EstRangeTerms", out var rt))
                            sampling = $"{rt}/{(clause.Parameters.TryGetValue("EstSampledTerms", out var st) ? st : "?")}";
                    }
                    if (iter == 5) est5 = est;
                }

                double err1 = actual > 0 ? (double)est1 / actual : -1;
                double err5 = actual > 0 ? (double)est5 / actual : -1;
                _output.WriteLine($"{field,-9}| {dist,-9}| {hi,6} | {actual,8:N0} | {est1,10:N0} | {err1,5:F2} | {est5,10:N0} | {err5,5:F2} | {sampling}");
            }
        }

        _output.WriteLine("");
        _output.WriteLine("err = estimate/actual (1.0 = perfect). The triangular rows stress middle-extrapolation;");
        _output.WriteLine("if its err is far worse than uniform, the CalibrationBeta shrinkage / sample sizes need tuning.");
    }

    /// <summary>
    /// Range-estimate self-correction: the per-clause calibration EWMA only updates on an UNBOUNDED fill
    /// (QueryPrimitives.CtxFillFromTreeScan observes the tally only when OpLimit == long.MaxValue). The earlier
    /// accuracy probe used `limit 1` (bounded driving fill) so the EWMA never saw feedback — the 1.6x it showed
    /// is the COLD-START (beta=1) estimate. Here the skewed range runs as an AND clause (unbounded scratch fill)
    /// so the EWMA is fed; repeating the query should drag the estimate toward actual if self-correction works.
    /// </summary>
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task Calibrate_RangeEstimate_Convergence(Options options)
    {
        if (Enabled == false)
        {
            _output.WriteLine("Skipped: set RAVEN_CALIBRATE=1 to run the range-estimate convergence calibration.");
            return;
        }

        const int totalDocs = 200_000;
        using var store = await BuildItemsStore(options, totalDocs, distinctTags: 24, tagsPerDoc: 3, clustered: true);

        _output.WriteLine($"# Range-estimate self-correction (SkewScore < 10000, fed via AND so the EWMA observes)");

        // Actual cardinality of the skewed clause alone (the quantity the clause estimate should converge toward).
        long actual;
        using (var s = store.OpenAsyncSession())
        {
            await s.Advanced.AsyncRawQuery<ItemDoc>(
                    $"from index '{new ItemsIndex().IndexName}' where SkewScore < 10000 limit 1")
                .Statistics(out var st).NoCaching().ToListAsync();
            actual = st.TotalResults;
        }
        _output.WriteLine($"actual(SkewScore<10000) = {actual:N0}");
        _output.WriteLine("iter | clauseEstimate | err");
        _output.WriteLine(new string('-', 40));

        // AND with Bucket=$b so the SkewScore range is an unbounded scratch fill -> ObserveTreeScanTally fires.
        for (int iter = 1; iter <= 8; iter++)
        {
            using var session = store.OpenAsyncSession();
            var q = session.Advanced.AsyncRawQuery<ItemDoc>(
                    $"from index '{new ItemsIndex().IndexName}' where SkewScore < $hi and Bucket = $b include timings()")
                .AddParameter("hi", 10000).AddParameter("b", 3).NoCaching();
            await q.Timings(out var timings).ToListAsync();

            var clause = FindClauseByField((QueryInspectionNode)timings.QueryPlan, "SkewScore");
            long est = clause != null && clause.Parameters.TryGetValue("EstimatedRows", out var e) ? ExtractFormatted(e) : -1;
            double err = actual > 0 ? (double)est / actual : -1;
            _output.WriteLine($"{iter,4} | {est,14:N0} | {err,5:F2}");
        }

        _output.WriteLine("");
        _output.WriteLine("If err moves from ~1.6 toward 1.0 the EWMA self-corrects (no static Beta change needed);");
        _output.WriteLine("if it stays pinned, the cold-start estimate / Beta clamp / sample sizes are the lever.");
    }

    private static QueryInspectionNode FindClauseByField(QueryInspectionNode node, string fieldName)
    {
        if (node.Parameters != null
            && node.Parameters.TryGetValue("FieldName", out var f) && f == fieldName
            && node.Parameters.ContainsKey("EstimatedRows"))
            return node;
        if (node.Children == null)
            return null;
        foreach (var child in node.Children)
        {
            var found = FindClauseByField(child, fieldName);
            if (found != null)
                return found;
        }

        return null;
    }

    private static QueryInspectionNode FindWithParam(QueryInspectionNode node, string paramKey)
    {
        if (node.Parameters != null && node.Parameters.ContainsKey(paramKey))
            return node;
        if (node.Children == null)
            return null;
        foreach (var child in node.Children)
        {
            var found = FindWithParam(child, paramKey);
            if (found != null)
                return found;
        }

        return null;
    }

    private static long ExtractFormatted(string n) =>
        long.Parse(n, NumberStyles.AllowThousands, CultureInfo.InvariantCulture);

    private static long ExtractParen(string s, string marker)
    {
        int i = s.IndexOf(marker + "(", StringComparison.Ordinal);
        if (i < 0) return -1;
        int start = i + marker.Length + 1;
        int end = s.IndexOf(')', start);
        return end < 0 ? -1 : long.Parse(s.AsSpan(start, end - start), provider: CultureInfo.InvariantCulture);
    }

    private async Task<ScanMeasurement> MeasureResidual(DocumentStore store, int hi, int take, ExecutionStrategy strategy)
    {
        double best = double.MaxValue;
        string hint = null;
        for (int i = 0; i < Repetitions; i++)
        {
            using var session = store.OpenAsyncSession();
            var q = session.Advanced.AsyncRawQuery<ItemDoc>(ResidualRql(take))
                .AddParameter("hi", hi).AddParameter("b", 3)
                .AddParameter("rvn_corax_strategy", strategy.ToString())
                .NoCaching();
            var sw = Stopwatch.StartNew();
            await q.Timings(out var timings).ToListAsync();
            sw.Stop();
            best = Math.Min(best, sw.Elapsed.TotalMilliseconds);
            var root = (QueryInspectionNode)timings.QueryPlan;
            hint = root.Parameters.TryGetValue("OptimizationHint", out var h) ? h : root.Operation;
        }

        return new ScanMeasurement(best, hint);
    }

    private static QueryInspectionNode FindNode(QueryInspectionNode node, string operation)
    {
        if (node.Operation == operation)
            return node;
        if (node.Children == null)
            return null;
        foreach (var child in node.Children)
        {
            var found = FindNode(child, operation);
            if (found != null)
                return found;
        }

        return null;
    }

    private sealed record ScanMeasurement(double Ms, string Hint);

    private async Task<ScanMeasurement> MeasureScan(DocumentStore store, int take, ExecutionStrategy strategy)
    {
        // Full-scan top-N by a single-valued numeric field — the shape that makes FieldSortedScan a candidate.
        var rql = $"from index '{new ItemsIndex().IndexName}' order by Category as long include timings() limit {take}";

        double best = double.MaxValue;
        string hint = null;
        for (int i = 0; i < Repetitions; i++)
        {
            using var session = store.OpenAsyncSession();
            var q = session.Advanced.AsyncRawQuery<ItemDoc>(rql)
                .AddParameter("rvn_corax_strategy", strategy.ToString())
                .NoCaching();
            var sw = Stopwatch.StartNew();
            await q.Timings(out var timings).ToListAsync();
            sw.Stop();
            best = Math.Min(best, sw.Elapsed.TotalMilliseconds);
            var root = (QueryInspectionNode)timings.QueryPlan;
            hint = root.Parameters.TryGetValue("OptimizationHint", out var h) ? h : root.Operation;
        }

        return new ScanMeasurement(best, hint);
    }

    private async Task<DocumentStore> BuildItemsStore(Options options, int totalDocs, int distinctTags, int tagsPerDoc, bool clustered)
    {
        var store = GetDocumentStore(options);
        await new ItemsIndex().ExecuteAsync(store);

        // Three "probe" tags with controlled frequency so the sweep covers dense/mid/rare candidate sets,
        // plus filler tags. When clustered=true a doc's probe tag correlates with its insertion order, which
        // is the pathological case for ascending IndexOrderStreaming (the over-scan the inflation EWMA learns).
        var rng = new Random(20250618);
        var fillerTags = Enumerable.Range(0, distinctTags).Select(i => $"g{i:D2}").ToArray();

        var bulk = store.BulkInsert();
        await using (bulk)
        {
            for (int i = 0; i < totalDocs; i++)
            {
                var tags = new List<string>(tagsPerDoc + 1);

                // probe tag: dense ~40%, mid ~8%, rare ~0.5%
                double r = clustered ? (double)i / totalDocs : rng.NextDouble();
                if (r < 0.40) tags.Add("tag_dense");
                else if (r < 0.48) tags.Add("tag_mid");
                else if (r < 0.485) tags.Add("tag_rare");

                for (int t = 0; t < tagsPerDoc; t++)
                    tags.Add(fillerTags[rng.Next(fillerTags.Length)]);

                await bulk.StoreAsync(new ItemDoc
                {
                    Tags = tags.Distinct().ToArray(),
                    // Single-valued numeric fields for the DirectScan-vs-Bitmap gate: Category (0..999, uniform)
                    // is the range-driving + sort field, Bucket (0..9) is the residual filter (~10% selectivity).
                    Category = i % 1000,
                    Bucket = rng.Next(10),
                    // High-cardinality (50K distinct, uniform) so partial ranges span far more than the
                    // RangeBottomSample(512)+RangeTopSample(256) edge samples and the estimator must sample+extrapolate
                    // the middle — the path the CalibrationBeta clamp governs.
                    Score = rng.Next(50_000),
                    // SKEWED variant: triangular, peaked at 25K — low/high values (the EDGE samples) are sparse while
                    // the middle is dense, so uniform middle-extrapolation from the edge samples under-counts unless
                    // Beta's shrinkage toward the global average corrects it. This is where Beta actually matters.
                    SkewScore = (rng.Next(50_000) + rng.Next(50_000)) / 2
                });
            }
        }

        await Indexes.WaitForIndexingAsync(store);
        return store;
    }

    private class ItemDoc
    {
        public string Id { get; set; }
        public string[] Tags { get; set; }
        public int Category { get; set; }
        public int Bucket { get; set; }
        public int Score { get; set; }
        public int SkewScore { get; set; }
    }

    private class ItemsIndex : AbstractIndexCreationTask<ItemDoc>
    {
        public ItemsIndex()
        {
            Map = docs => from doc in docs
                select new { doc.Tags, doc.Category, doc.Bucket, doc.Score, doc.SkewScore };
        }
    }
}

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Tests.Infrastructure;

namespace FastTests.Corax;

/// <summary>
/// Quantifies how much of a Corax query's wall-clock is the SORT stage, to size the headroom
/// of a hypothetical compiled-IL sort path (RavenDB-25281). The single-field sort is
/// already JIT-monomorphized via the <c>SortBy&lt;TEntryComparer,TFwdIt,TBackIt&gt;</c> function-pointer
/// dispatch (SortingMatch.cs) and the multi-field path is generic up to 3 comparers
/// (SortingMultiMatch.SortBatch&lt;TComparer2,TComparer3&gt;), so there is no per-row interpreter to compile
/// away the way the bitmap PlanOp[] stack machine had. This benchmark measures the delta between the
/// same WHERE run with and without ORDER BY across the distinct sort code paths (Integer-Lookup stream,
/// Sequence/CompactTree stream, top-N stream, multi-field tie-break). The sorted/baseline delta is the
/// only slice an emitted comparator could attack; the rest is term/posting-list I/O that IL cannot help.
///
/// Measured (20k docs, WHERE matches 10k, median of 60 iterations, Release):
///   WHERE only (baseline)        111.16 ms   10000 results   --
///   ORDER BY int (Seq)           109.73 ms   10000 results   -1.3% (noise)
///   ORDER BY int LIMIT 50          0.58 ms      50 results   top-N stream short-circuits materialization (~190x)
///   ORDER BY string (Name)       112.27 ms   10000 results   +1.0% (noise)
///   ORDER BY double (Price)      111.71 ms   10000 results   +0.5% (noise)
///   ORDER BY 2-field (tie-break) 109.59 ms   10000 results   -1.4% (noise)
/// Conclusion: for a full-result query the sort stage is within measurement noise of the unsorted run —
/// the comparison loop is already devirtualized/inlined, so a compiled-IL comparator has ~no headroom on
/// the single/2-3 field paths. The large LIMIT win is a streaming (early-exit) effect, orthogonal to how
/// the comparator is dispatched. The only dispatch IL could remove is ORDER BY with 4+ fields (the
/// _nextComparers[] interface tail past NextComparerOffset=3), which is rare and still I/O-bound.
/// </summary>
public class SortOverheadBenchmark : RavenTestBase
{
    public SortOverheadBenchmark(Xunit.ITestOutputHelper output) : base(output) { }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying, Skip = "Benchmark — too slow for CI. Run manually.")]
    public async Task MeasureSortOverhead()
    {
        const int docCount = 20_000;
        const int warmup = 5;
        const int iterations = 60;

        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        await SeedData(store, docCount);

        // Each pair shares the same WHERE so the delta isolates the sort stage.
        // "from BenchDocs where Status = 'active'" matches ~half the set; the sort then orders that half.
        var cases = new (string name, string rql)[]
        {
            ("WHERE only (baseline)",      "from BenchDocs where Status = 'active'"),
            ("ORDER BY int (Seq)",         "from BenchDocs where Status = 'active' order by Seq as long"),
            ("ORDER BY int LIMIT 50",      "from BenchDocs where Status = 'active' order by Seq as long limit 50"),
            ("ORDER BY string (Name)",     "from BenchDocs where Status = 'active' order by Name"),
            ("ORDER BY double (Price)",    "from BenchDocs where Status = 'active' order by Price as double"),
            ("ORDER BY 2-field (tieB)",    "from BenchDocs where Status = 'active' order by Category, Seq as long"),
        };

        Output.WriteLine($"docs={docCount}, warmup={warmup}, iterations={iterations}, median ms");
        Output.WriteLine($"{"Case",-30} {"Median ms",-12} {"Results",-10} {"Δ vs baseline",-14} {"Sort %",-8}");
        Output.WriteLine(new string('-', 80));

        double baseline = -1;
        foreach (var (name, rql) in cases)
        {
            var (ms, count) = await BenchQuery(store, rql, warmup, iterations);
            if (baseline < 0)
            {
                baseline = ms;
                Output.WriteLine($"{name,-30} {ms,8:F3}ms {count,8}   {"--",-14} {"--",-8}");
                continue;
            }

            double delta = ms - baseline;
            double sortPct = ms > 0 ? delta / ms * 100 : 0;
            Output.WriteLine($"{name,-30} {ms,8:F3}ms {count,8}   {delta,8:F3}ms     {sortPct,6:F1}%");
        }
    }

    private async Task<(double medianMs, int resultCount)> BenchQuery(IDocumentStore store, string rql, int warmup, int iterations)
    {
        for (int w = 0; w < warmup; w++)
        {
            using var session = store.OpenAsyncSession();
            await session.Advanced.AsyncRawQuery<dynamic>(rql).ToListAsync();
        }

        var times = new double[iterations];
        int count = 0;
        for (int i = 0; i < iterations; i++)
        {
            var sw = Stopwatch.StartNew();
            using var session = store.OpenAsyncSession();
            var results = await session.Advanced.AsyncRawQuery<dynamic>(rql).ToListAsync();
            sw.Stop();
            times[i] = sw.Elapsed.TotalMilliseconds;
            count = results.Count;
        }

        Array.Sort(times);
        return (times[times.Length / 2], count);
    }

    private async Task SeedData(IDocumentStore store, int count)
    {
        for (int batch = 0; batch < count; batch += 1000)
        {
            using var session = store.OpenAsyncSession();
            int end = Math.Min(batch + 1000, count);
            for (int i = batch; i < end; i++)
            {
                await session.StoreAsync(new BenchDoc
                {
                    Name = $"doc-{i:D6}",
                    Category = $"cat-{i % 7}",
                    Status = i % 2 == 0 ? "active" : "inactive",
                    Seq = i,
                    Price = i * 1.5
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);
    }

    private class BenchDoc
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Category { get; set; }
        public string Status { get; set; }
        public int Seq { get; set; }
        public double Price { get; set; }
    }
}

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

/// <summary>
/// Same-pipeline repeatability benchmark: measures query latencies through
/// the Corax bitmap pipeline with two independent store instances.
/// All queries exercise the same CompiledQuery path — no path comparison.
/// Outputs timing comparison between the two runs.
/// </summary>
public class CompiledQueryBenchmark : RavenTestBase
{
    public CompiledQueryBenchmark(Xunit.ITestOutputHelper output) : base(output) { }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying, Skip = "Benchmark — too slow for CI. Run manually.")]
    public async Task CompareStreamingVsBitmap()
    {
        const int docCount = 10_000;
        const int warmup = 3;
        const int iterations = 50;

        var optionsOld = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        using var storeOld = GetDocumentStore(optionsOld);
        await SeedData(storeOld, docCount);

        // Same configuration; both use the bitmap pipeline.
        var optionsNew = Options.ForSearchEngine(RavenSearchEngineMode.Corax);

        using var storeNew = GetDocumentStore(optionsNew);
        await SeedData(storeNew, docCount);

        var queries = new (string name, string rql)[]
        {
            ("Term: Status=active", "from BenchDocs where Status = 'active'"),
            ("AND: Status∩Category", "from BenchDocs where Status = 'active' and Category = 'cat-0'"),
            ("OR: Cat0∪Cat1", "from BenchDocs where Category = 'cat-0' or Category = 'cat-1'"),
            ("3-way AND", "from BenchDocs where Status = 'active' and Category = 'cat-0' and Tag = 'tag-0'"),
            ("AND+Range", "from BenchDocs where Category = 'cat-0' and Price > 500"),
            ("Mixed (OR)∩AND", "from BenchDocs where (Category = 'cat-0' or Category = 'cat-1') and Status = 'active'"),
            ("startsWith", "from BenchDocs where startsWith(Name, 'doc-000')"),
            ("IN clause", "from BenchDocs where Category in ('cat-0', 'cat-1', 'cat-2')"),
        };

        Output.WriteLine($"{"Query",-35} {"Run 1 (ms)",-12} {"Run 2 (ms)",-12} {"Ratio",-10} {"Results",-10}");
        Output.WriteLine(new string('-', 80));

        foreach (var (name, rql) in queries)
        {
            var (ms1, count1) = await BenchQuery(storeOld, rql, warmup, iterations);
            var (ms2, count2) = await BenchQuery(storeNew, rql, warmup, iterations);

            double ratio = ms1 / Math.Max(ms2, 0.001);
            Output.WriteLine($"{name,-35} {ms1,8:F2}ms {ms2,8:F2}ms {ratio,8:F2}x {count1,8}");

            Assert.Equal(count1, count2);
        }
    }

    private async Task<(double avgMs, int resultCount)> BenchQuery(IDocumentStore store, string rql, int warmup, int iterations)
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
        return (times[times.Length / 2], count);  // median eliminates outliers
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
                    Name = $"doc-{i:D5}",
                    Category = $"cat-{i % 5}",
                    Status = i % 2 == 0 ? "active" : "inactive",
                    Tag = $"tag-{i % 10}",
                    Price = i * 3
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
        public string Tag { get; set; }
        public int Price { get; set; }
    }
}

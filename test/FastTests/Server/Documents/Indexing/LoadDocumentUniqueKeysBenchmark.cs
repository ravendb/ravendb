using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.Indexing;

/// <summary>
/// Cost of LoadDocument when the loaded documents are not shared. One index build per shape:
///   unique: every outer document loads its own referenced document (all keys unique)
///   shared: every outer document loads one of a few shared documents (RAVEN_LDU_SHARED, default 20)
///   middle: every referenced document is loaded by a few outer documents, spread over the batch
///
/// Opt-in: RAVEN_REFERENCE_BENCH=1. Other environment variables:
///   RAVEN_LDU_SHAPE (unique|shared|middle), RAVEN_LDU_OUTER, RAVEN_LDU_DOCBYTES, RAVEN_LDU_ENGINE (Corax|Lucene),
///   RAVEN_LDU_CAP, RAVEN_LDU_LABEL, RAVEN_LDU_OUT (file a tab-separated line is appended to).
/// Memory is sampled during the build (RAVEN_LDU_RETAIN=1 samples live managed bytes after a full collection instead).
/// </summary>
public class LoadDocumentUniqueKeysBenchmark(ITestOutputHelper output) : RavenTestBase(output)
{
    private sealed class Owner
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Payload { get; set; }
        public int Number { get; set; }
    }

    private sealed class Company
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string OwnerId { get; set; }
    }

    private sealed class CompanyView
    {
        public string Name { get; set; }
        public string OwnerName { get; set; }
    }

    private sealed class CompaniesLoadingOwner : AbstractIndexCreationTask<Company>
    {
        public CompaniesLoadingOwner()
        {
            Map = companies => from c in companies
                let owner = LoadDocument<Owner>(c.OwnerId)
                select new { c.Name, OwnerName = owner.Name };
        }
    }

    private static int Setting(string name, int fallback) => int.TryParse(Environment.GetEnvironmentVariable(name), out var value) ? value : fallback;

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void LoadingOwnersBuild(Options options)
    {
        if (Environment.GetEnvironmentVariable("RAVEN_REFERENCE_BENCH") != "1")
            return;

        var engineFilter = Environment.GetEnvironmentVariable("RAVEN_LDU_ENGINE");
        if (string.IsNullOrEmpty(engineFilter) == false && string.Equals(engineFilter, options.SearchEngineMode.ToString(), StringComparison.OrdinalIgnoreCase) == false)
            return;

        var shape = Environment.GetEnvironmentVariable("RAVEN_LDU_SHAPE") ?? "unique";
        var outer = Setting("RAVEN_LDU_OUTER", 100_000);
        var docBytes = Setting("RAVEN_LDU_DOCBYTES", 300);
        var sharedCount = Setting("RAVEN_LDU_SHARED", 20);

        // referenced document of outer document i
        int referenced;
        Func<int, int> target;
        switch (shape)
        {
            case "unique":
                referenced = outer;
                target = i => i;
                break;
            case "shared":
                referenced = sharedCount;
                target = i => i % sharedCount;
                break;
            case "middle":
                referenced = (int)(outer / 3.5);
                target = i => i % referenced;
                break;
            default:
                throw new ArgumentException(shape);
        }

        using var store = GetDocumentStore(options);
        var payload = new string('x', docBytes);

        using (var bulk = store.BulkInsert())
        {
            for (var r = 0; r < referenced; r++)
                bulk.Store(new Owner { Id = $"owners/{r}", Name = $"Owner {r}", Payload = payload, Number = r });
            for (var i = 0; i < outer; i++)
                bulk.Store(new Company { Id = $"companies/{i}", Name = $"Company {i}", OwnerId = $"owners/{target(i)}" });
        }

        // let the pending flush finish, it makes the batches tiny
        Thread.Sleep(TimeSpan.FromSeconds(Setting("RAVEN_LDU_SETTLE", 8 + outer / 50_000)));

        // everything written, nothing indexed yet
        for (var i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        var process = Process.GetCurrentProcess();
        process.Refresh();
        var baseManaged = GC.GetTotalMemory(true);
        var baseNative = NativeMemory.TotalAllocatedMemory;
        var baseWorkingSet = process.WorkingSet64;
        var baseAllocated = GC.GetTotalAllocatedBytes(true);
        var highestField = typeof(Raven.Server.Documents.Indexes.Static.CurrentIndexingScope).GetField("HighestNumberOfKeptLoadedDocuments", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);
        highestField?.SetValue(null, 0);
        if (int.TryParse(Environment.GetEnvironmentVariable("RAVEN_LDU_CAP"), out var cap))
            typeof(Raven.Server.Documents.Indexes.Static.CurrentIndexingScope).GetField("MaxKeptLoadedDocuments", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static)?.SetValue(null, cap);

        long maxManaged = 0, maxNative = 0, maxWorkingSet = 0, maxLive = 0;
        var retain = Environment.GetEnvironmentVariable("RAVEN_LDU_RETAIN") == "1";
        var stop = false;
        var sampler = new Thread(() =>
        {
            var self = Process.GetCurrentProcess();
            while (Volatile.Read(ref stop) == false)
            {
                maxManaged = Math.Max(maxManaged, GC.GetTotalMemory(false));
                maxNative = Math.Max(maxNative, NativeMemory.TotalAllocatedMemory);
                self.Refresh();
                maxWorkingSet = Math.Max(maxWorkingSet, self.WorkingSet64);
                if (retain)
                {
                    maxLive = Math.Max(maxLive, GC.GetTotalMemory(true));
                    Thread.Sleep(700);
                }
                else
                    Thread.Sleep(50);
            }
        }) { IsBackground = true };

        var watch = Stopwatch.StartNew();
        sampler.Start();
        new CompaniesLoadingOwner().Execute(store);
        Indexes.WaitForIndexing(store, timeout: TimeSpan.FromMinutes(30));
        watch.Stop();
        Volatile.Write(ref stop, true);
        sampler.Join();
        var allocatedProcess = GC.GetTotalAllocatedBytes(false) - baseAllocated;

        var performance = store.Maintenance.Send(new GetIndexPerformanceStatisticsOperation([nameof(CompaniesLoadingOwner)])).Single().Performance;
        var allocatedByIndex = performance.Sum(x => x.AllocatedManagedBytes?.SizeInBytes ?? 0);
        var batches = performance.Length;
        var reasons = new System.Collections.Generic.Dictionary<string, int>();
        void Collect(Raven.Client.Documents.Indexes.IndexingPerformanceOperation operation)
        {
            if (operation == null)
                return;
            foreach (var reason in new[] { operation.MapDetails?.BatchCompleteReason, operation.ReferenceDetails?.BatchCompleteReason })
                if (string.IsNullOrEmpty(reason) == false)
                {
                    var key = reason.Length > 140 ? reason[..140] : reason;
                    reasons[key] = (reasons.TryGetValue(key, out var seen) ? seen : 0) + 1;
                }
            foreach (var child in operation.Operations ?? [])
                Collect(child);
        }
        foreach (var entry in performance)
            Collect(entry.Details);
        var stats = store.Maintenance.Send(new GetIndexStatisticsOperation(nameof(CompaniesLoadingOwner)));

        // correctness: the index saw every owner
        using (var session = store.OpenSession())
        {
            var expected = Enumerable.Range(0, outer).Count(i => target(i) == 5 % referenced);
            var found = session.Query<CompanyView, CompaniesLoadingOwner>().Count(x => x.OwnerName == $"Owner {5 % referenced}");
            Assert.Equal(expected, found);
            Assert.Equal(0, stats.ErrorsCount);
            Assert.Equal(outer, stats.EntriesCount);
        }

        const double mb = 1024.0 * 1024.0;
        var fields = new object[]
        {
            Environment.GetEnvironmentVariable("RAVEN_LDU_LABEL") ?? "", options.SearchEngineMode, shape, outer, referenced, docBytes,
            (allocatedByIndex / mb).ToString("F1"), (allocatedProcess / mb).ToString("F1"),
            ((maxManaged - baseManaged) / mb).ToString("F1"), retain ? ((maxLive - baseManaged) / mb).ToString("F1") : "n/a", ((maxNative - baseNative) / mb).ToString("F1"), ((maxWorkingSet - baseWorkingSet) / mb).ToString("F1"),
            (maxWorkingSet / mb).ToString("F0"), watch.Elapsed.TotalSeconds.ToString("F2"), batches,
            highestField != null ? highestField.GetValue(null) : "n/a",
            string.Join("; ", reasons.Select(x => $"{x.Value}x {x.Key}")).Replace('\t', ' ')
        };
        var line = string.Join('\t', fields);
        output.WriteLine("label\tengine\tshape\touter\treferenced\tdocBytes\tallocIndexMB\tallocProcessMB\tpeakManagedDeltaMB\tpeakLiveDeltaMB\tpeakNativeDeltaMB\tpeakWsDeltaMB\tpeakWsMB\tseconds\tperfEntries\tmaxKept\treasons");
        output.WriteLine(line);
        File.AppendAllText(Environment.GetEnvironmentVariable("RAVEN_LDU_OUT") ?? Path.Combine(Path.GetTempPath(), "ldu-bench.tsv"), line + Environment.NewLine);
    }
}

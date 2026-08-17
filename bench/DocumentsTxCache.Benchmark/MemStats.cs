using System;
using System.Globalization;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Tests.Infrastructure;

namespace DocumentsTxCache.Benchmark;

// Alternative to BenchmarkDotNet's NativeMemoryProfiler (which needs an elevated, out-of-process run and
// is unsupported with InProcessEmitToolchain): drive sustained modify load while polling the server's own
// /admin/debug/memory/stats endpoint on a background thread, and record the peak of the memory gauges.
// UnmanagedAllocations == NativeMemory.TotalAllocatedMemory, so a baseline commit that decompresses all
// 300 collections' last documents shows up as a native peak the optimized build does not have.
public static class MemStats
{
    private const int NumberOfCollections = 300;
    private const int LoadBursts = 200; // each burst = ModificationsPerInvocation commits

    // one compression mode per process invocation: RavenTestBase cannot be safely instantiated twice
    // in the same process (its xUnit-logger base ctor throws on the second instance).
    public static async Task Run(StorageMode mode)
    {
        await using var tests = new ActualTests(new ConsoleTestOutputHelper());
        tests.Initialize(NumberOfCollections, mode);

        var statsUrl = $"{tests.ServerUrl}/admin/debug/memory/stats?includeThreads=false&includeMappings=false";
        using var http = new HttpClient();

        long peakUnmanaged = 0, peakManaged = 0, peakWorkingSet = 0;
        var samples = 0;
        var stop = false;

        var sampler = Task.Run(() =>
        {
            while (Volatile.Read(ref stop) == false)
            {
                try
                {
                    var json = http.GetStringAsync(statsUrl).GetAwaiter().GetResult();
                    using var doc = JsonDocument.Parse(json);
                    var mem = doc.RootElement.GetProperty("MemoryInformation");
                    peakUnmanaged = Math.Max(peakUnmanaged, HumaneToBytes(mem.GetProperty("UnmanagedAllocations").GetString()));
                    peakManaged = Math.Max(peakManaged, HumaneToBytes(mem.GetProperty("ManagedAllocations").GetString()));
                    peakWorkingSet = Math.Max(peakWorkingSet, HumaneToBytes(mem.GetProperty("WorkingSet").GetString()));
                    samples++;
                }
                catch
                {
                    // ignore transient sampling errors
                }

                Thread.Sleep(20);
            }
        });

        for (var i = 0; i < LoadBursts; i++)
            tests.ModifyDocumentInLoop();

        Volatile.Write(ref stop, true);
        sampler.Wait();

        Console.WriteLine(
            $"[MEMSTATS] mode={mode} samples={samples} " +
            $"peakUnmanagedBytes={peakUnmanaged} peakManagedBytes={peakManaged} peakWorkingSetBytes={peakWorkingSet} " +
            $"peakUnmanagedMB={peakUnmanaged / 1024.0 / 1024.0:F1} peakManagedMB={peakManaged / 1024.0 / 1024.0:F1} peakWorkingSetMB={peakWorkingSet / 1024.0 / 1024.0:F1}");
    }

    // parses both humane sizes ("22.5 MBytes") and plain byte counts ("1288490188")
    private static long HumaneToBytes(string s)
    {
        if (string.IsNullOrWhiteSpace(s))
            return 0;

        var parts = s.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (double.TryParse(parts[0], NumberStyles.Any, CultureInfo.InvariantCulture, out var num) == false)
            return 0;

        var multiplier = parts.Length > 1
            ? parts[1] switch
            {
                "KBytes" => 1024d,
                "MBytes" => 1024d * 1024,
                "GBytes" => 1024d * 1024 * 1024,
                "TBytes" => 1024d * 1024 * 1024 * 1024,
                _ => 1d
            }
            : 1d;

        return (long)(num * multiplier);
    }
}

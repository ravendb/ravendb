using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Server.Config;
using Sparrow.LowMemory;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

[Collection(SpatialSortMemoryCollection.Name)]
public class SpatialSortMemory(ITestOutputHelper output) : RavenTestBase(output)
{
    private const int Documents = 25_000;

    // what a query holds apart from the entries
    private const long FixedBytes = 320 * 1024;

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying | RavenTestCategory.Spatial)]
    public void SortingBySpatialDistanceDoesNotHoldBuffersOfTheSizeOfTheResult()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        new PlacesIndex().Execute(store);
        var random = new Random(1);
        using (var bulk = store.BulkInsert())
        {
            for (var i = 0; i < Documents; i++)
                bulk.Store(new Place { Latitude = 50 + random.NextDouble() * 5, Longitude = 3 + random.NextDouble() * 5 });
        }

        Indexes.WaitForIndexing(store);

        // Native memory held by the query: the peak of the process-wide allocation counter, sampled while it runs.
        const string rql = "from index 'PlacesIndex' order by spatial.distance(Coordinates, spatial.point(52.09, 5.12)) limit 0, 20";

        // the pooled contexts keep memory from the indexing: release it first, or the counter never moves
        LowMemoryNotification.Instance.SimulateLowMemoryNotification();
        WaitUntilNativeMemoryIsStable();

        var baseline = NativeMemory.TotalAllocatedMemory;
        var peak = baseline;
        var stop = false;
        var sampler = new Thread(() =>
        {
            while (Volatile.Read(ref stop) == false)
            {
                // without the barrier the JIT reads the counter once
                Thread.MemoryBarrier();
                peak = Math.Max(peak, NativeMemory.TotalAllocatedMemory);
            }
        });
        sampler.Start();

        List<Place> results;
        try
        {
            using var session = store.OpenSession();
            results = session.Advanced.RawQuery<Place>(rql).ToList();
        }
        finally
        {
            Volatile.Write(ref stop, true);
            sampler.Join();
        }

        Assert.Equal(20, results.Count);

        var held = peak - baseline;
        output.WriteLine($"held unmanaged peak: {held / 1024.0:N0} KB for {Documents:N0} entries = {held / (double)Documents:F1} bytes per entry");

        // only the entry ids have to be kept: the term buffers are not needed to sort by distance
        Assert.True(held < FixedBytes + Documents * 24L, $"The query held {held:N0} bytes for {Documents:N0} entries");
    }

    // memory given back by earlier tests is released in the background: wait for the baseline to settle
    private static void WaitUntilNativeMemoryIsStable()
    {
        var last = -1L;
        var stable = 0;
        for (var i = 0; i < 100 && stable < 5; i++)
        {
            Thread.Sleep(200);
            var current = NativeMemory.TotalAllocatedMemory;
            stable = current == last ? stable + 1 : 0;
            last = current;
        }
    }

    private class Place
    {
        public string Id { get; set; }
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }

    private class PlacesIndex : AbstractIndexCreationTask<Place>
    {
        public PlacesIndex()
        {
            Map = places => from p in places select new { Coordinates = CreateSpatialField(p.Latitude, p.Longitude) };
        }
    }
}

// the test reads the process-wide native allocation counter, so no other test may run meanwhile
[CollectionDefinition(Name, DisableParallelization = true)]
public class SpatialSortMemoryCollection
{
    public const string Name = nameof(SpatialSortMemoryCollection);
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Server.Collections;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Voron.Global;
using Voron.Impl;
using Xunit;
using VectorEmbeddingType = Voron.Data.Graphs.VectorEmbeddingType;

namespace FastTests.Voron.Graphs;

public class VectorSearchRetrieverTests(ITestOutputHelper output) : StorageTest(output)
{
    private const string TreeName = "test";

    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public void EntriesOfOneNodeKeepItsDistanceAcrossFillCalls(bool filtered, bool largePostingList)
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        var random = new Random(1);
        // Node 1 holds many entries and each of the 54 other nodes holds one. Entry ids grow by random gaps so that a large list does not fit in a container page.
        var multiple = Enumerable.Range(0, largePostingList ? 6000 : 5).Select(i => (long)i * 400 + random.Next(1, 400)).ToArray();
        var single = Enumerable.Range(1, 54).Select(i => multiple[^1] + i).ToArray();
        Build(treeName, random, 8, [(multiple, RandomVector(random, 8)), .. single.Select(entry => (new[] { entry }, RandomVector(random, 8)))]);

        var all = multiple.Concat(single).ToArray();
        var filter = new GrowableBitArray(Allocator, all.Max() + 1);
        foreach (var entry in all)
            filter.Add(entry);
        filter.Count = all.Length;

        using var tx = Env.ReadTransaction();
        using (var state = new Hnsw.SearchState(tx.LowLevelTransaction, treeName))
        {
            var form = state.GetNodeByIndex(state.GetNodeIndexById(1)).PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask;
            Assert.Equal(largePostingList ? Constants.Graphs.VectorId.PostingList : Constants.Graphs.VectorId.SmallPostingList, form);
        }

        var query = MemoryMarshal.AsBytes(RandomVector(random, 8).AsSpan()).ToArray();
        var withLargeBuffer = Drain(tx, treeName, query, filtered ? filter : null, 1024);
        var withOneEntryBuffer = Drain(tx, treeName, query, filtered ? filter : null, 1);

        Assert.Equal(all.Length, withLargeBuffer.Count);
        Assert.DoesNotContain(withOneEntryBuffer, e => float.IsNaN(e.Distance));
        Assert.Equal(withLargeBuffer.Select(e => (e.Entry, BitConverter.SingleToInt32Bits(e.Distance))), withOneEntryBuffer.Select(e => (e.Entry, BitConverter.SingleToInt32Bits(e.Distance))));
    }

    // One node in twenty: a walk reads more of the filter's members than its working set keeps. One node in two: it reads many more than
    // requested and returns the nearest of them.
    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    [InlineData(20)]
    [InlineData(2)]
    public void FilteredSearchReturnsTheNearestRequestedMatches(int stride)
    {
        using var name = Slice.From(Allocator, TreeName, out var treeName);
        const int count = 2000, requested = 10;
        var random = new Random(1);
        Build(treeName, random, 16, Enumerable.Range(1, count).Select(i => (new long[] { i }, RandomVector(random, 16))).ToArray());
        var filter = new GrowableBitArray(Allocator, count + 1);
        for (var entry = 1; entry <= count; entry += stride)
            filter.Add(entry);
        filter.Count = count / stride;
        var query = MemoryMarshal.AsBytes(RandomVector(random, 16).AsSpan()).ToArray();

        using var tx = Env.ReadTransaction();
        using var retriever = Hnsw.ApproximateNearest(tx.LowLevelTransaction, treeName, requested, query, 0f, filter);
        var entries = new long[count];
        var returned = new List<long>();
        for (int read; (read = retriever.Fill(entries, new float[count], filter)) > 0;)
            returned.AddRange(entries[..read]);

        Assert.Equal(requested, returned.Count);
        Assert.All(returned, entry => Assert.True(filter.Contains(entry)));
        var nearest = Drain(tx, treeName, query, filter, count).Take(requested).Select(e => e.Entry);
        Assert.True(returned.Intersect(nearest).Count() >= requested / 2, "the search returns at least half of the nearest filter members");
    }

    // The filter holds more than 1024 matches and none of them has a vector, so no walk holds a match and the search stops at the read budget of
    // large filters: half the filter's matches, at most a fifth of the graph.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void FilteredSearchWithoutMatchesInTheGraphStopsAtTheReadBudget()
    {
        using var name = Slice.From(Allocator, TreeName, out var treeName);
        const int count = 4000, matches = 1200;
        var random = new Random(1);
        Build(treeName, random, 16, Enumerable.Range(1, count).Select(i => (new long[] { i }, RandomVector(random, 16))).ToArray());
        var filter = new GrowableBitArray(Allocator, count + matches + 1);
        for (var entry = count + 1; entry <= count + matches; entry++)
            filter.Add(entry);
        filter.Count = matches;
        var query = MemoryMarshal.AsBytes(RandomVector(random, 16).AsSpan()).ToArray();

        using var tx = Env.ReadTransaction();
        using var retriever = Hnsw.ApproximateNearest(tx.LowLevelTransaction, treeName, 10, query, 0f, filter);
        Assert.Equal(0, retriever.Fill(new long[count], new float[count], filter));
        Assert.InRange(retriever.CandidatesProcessed, Math.Min(matches / 2, count / 5), count - 1);
    }

    // A binary query is searched as given, so an earlier search with the same query vector leaves the distances it read cached in the state.
    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    [InlineData(true)]
    [InlineData(false)]
    public void FilteredSearchAfterAnotherWithTheSameQueryReturnsTheRequestedMatches(bool exactFirst)
    {
        using var name = Slice.From(Allocator, TreeName, out var treeName);
        const int count = 500, requested = 10;
        var random = new Random(1);
        Build(treeName, random, 16, Enumerable.Range(1, count).Select(i => (new long[] { i }, RandomVector(random, 16))).ToArray(), VectorEmbeddingType.Binary);
        var filter = new GrowableBitArray(Allocator, count + 1);
        for (var entry = 1; entry <= count; entry += 2)
            filter.Add(entry);
        filter.Count = count / 2;
        Memory<byte> query = MemoryMarshal.AsBytes(RandomVector(random, 16).AsSpan()).ToArray();

        using var tx = Env.ReadTransaction();
        using var state = new Hnsw.SearchState(tx.LowLevelTransaction, treeName);
        Count(exactFirst ? Hnsw.ExactNearest(state, requested, query, 0f) : Hnsw.ApproximateNearest(state, requested, query, 0f, filter), filter);

        Assert.Equal(requested, Count(Hnsw.ApproximateNearest(state, requested, query, 0f, filter), filter));
    }

    private static int Count(Hnsw.VectorSearchRetriever retriever, GrowableBitArray filter)
    {
        using (retriever)
        {
            var entries = new long[256];
            var distances = new float[256];
            var count = 0;
            for (int read; (read = retriever.Fill(entries, distances, filter)) > 0;)
                count += read;
            return count;
        }
    }

    private List<(long Entry, float Distance)> Drain(Transaction tx, Slice treeName, byte[] query, GrowableBitArray? filter, int bufferSize)
    {
        using var retriever = Hnsw.ExactNearest(tx.LowLevelTransaction, treeName, 1000, query, 0f);
        var entries = new long[bufferSize];
        var distances = new float[bufferSize];
        var drained = new List<(long, float)>();
        for (int read; (read = retriever.Fill(entries, distances, filter)) > 0;)
            drained.AddRange(entries[..read].Zip(distances[..read]));
        return drained;
    }

    private void Build(Slice treeName, Random random, int dimensions, (long[] Entries, float[] Vector)[] nodes,
        VectorEmbeddingType embeddingType = VectorEmbeddingType.Single)
    {
        using var tx = Env.WriteTransaction();
        Hnsw.Create(tx.LowLevelTransaction, treeName, dimensions * sizeof(float), 12, 16, embeddingType);
        using (var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, treeName, random))
        {
            foreach (var (entries, vector) in nodes)
            {
                foreach (var entry in entries)
                    registration.Register(entry, MemoryMarshal.AsBytes(vector.AsSpan()));
            }

            registration.Commit(CancellationToken.None);
        }

        tx.Commit();
    }

    private static float[] RandomVector(Random random, int dimensions) => Enumerable.Range(0, dimensions).Select(_ => (float)(random.NextDouble() * 2 - 1)).ToArray();
}

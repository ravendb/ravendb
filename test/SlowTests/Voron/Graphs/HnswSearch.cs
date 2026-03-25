using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using FastTests.Voron;
using Sparrow.Server.Collections;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Graphs;

public class HnswSearch(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void FilteredFillWithLargePostingListDecodesCorrectRange()
    {
        using var _ = Slice.From(Allocator, nameof(FilteredFillWithLargePostingListDecodesCorrectRange), out var treeName);
        const int vectorSize = 4;
        const int vectorSizeInBytes = vectorSize * sizeof(float);
        const int singleEntryCount = 10;
        const int largePostingListCount = 10357;
        const int totalEntries = singleEntryCount + largePostingListCount;
        float[] queryVector = [1f, 0f, 0f, 0f];
        float[] farVector = [0f, 0f, 0f, 1f];

        using (var wTx = Env.WriteTransaction())
        {
            Hnsw.Create(wTx.LowLevelTransaction, treeName, vectorSizeInBytes, 3, 16, VectorEmbeddingType.Single);

            using (var registration = Hnsw.RegistrationFor(wTx.LowLevelTransaction, treeName, new Random(42)))
            {
                for (int i = 1; i <= singleEntryCount; i++)
                {
                    var closeVector = new float[] { 1f, 0.001f * i, 0f, 0f };
                    registration.Register(i, MemoryMarshal.Cast<float, byte>(closeVector));
                }

                for (int i = singleEntryCount + 1; i <= totalEntries; i++)
                    registration.Register(i, MemoryMarshal.Cast<float, byte>(farVector));

                registration.Commit(CancellationToken.None);
            }

            wTx.Commit();
        }

        using (var rTx = Env.ReadTransaction())
        {
            var qV = MemoryMarshal.Cast<float, byte>(queryVector);
            using var nearest = Hnsw.ExactNearest(rTx.LowLevelTransaction, treeName,
                numberOfCandidates: totalEntries, qV.ToArray(), minimumSimilarity: 0f, hasFilterMatch: true);

            var filter = new GrowableBitArray(Allocator, totalEntries + 1);
            using var __ = filter;
            for (int i = 1; i <= totalEntries; i++)
                filter.Add(i);
            filter.Count = totalEntries;

            var matches = new long[128];
            var distances = new float[128];
            List<long> allResults = new();

            int read;
            do
            {
                read = nearest.Fill(matches, distances, filter);
                allResults.AddRange(matches[..read]);
            } while (read != 0);

            var expectedIds = Enumerable.Range(1, totalEntries).Select(i => (long)i).ToHashSet();
            var actualIds = allResults.ToHashSet();
            var missing = expectedIds.Except(actualIds);
            Assert.Empty(missing);
            var unexpected = actualIds.Except(expectedIds);
            Assert.Empty(unexpected);
        }
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void FilteredFillWithSmallPostingListLargerThanMatchesBufferDoesNotInfiniteLoop()
    {
        using var _ = Slice.From(Allocator, nameof(FilteredFillWithSmallPostingListLargerThanMatchesBufferDoesNotInfiniteLoop), out var treeName);
        const int vectorSize = 4;
        const int vectorSizeInBytes = vectorSize * sizeof(float);
        const int entriesWithSameVector = 8;
        const int matchesBufferSize = 4;
        float[] commonVector = [1.0f, 2.0f, 3.0f, 4.0f];

        using (var wTx = Env.WriteTransaction())
        {
            Hnsw.Create(wTx.LowLevelTransaction, treeName, vectorSizeInBytes, 3, 16, VectorEmbeddingType.Single);

            using var registration = Hnsw.RegistrationFor(wTx.LowLevelTransaction, treeName, new Random(42));
            for (int i = 1; i <= entriesWithSameVector; i++)
                registration.Register(i, MemoryMarshal.Cast<float, byte>(commonVector));

            registration.Commit(CancellationToken.None);
            wTx.Commit();
        }

        using (var rTx = Env.ReadTransaction())
        {
            var qV = MemoryMarshal.Cast<float, byte>(commonVector);
            using var nearest = Hnsw.ExactNearest(rTx.LowLevelTransaction, treeName,
                numberOfCandidates: 100, qV.ToArray(), minimumSimilarity: 0f, hasFilterMatch: true);

            var filter = new GrowableBitArray(Allocator, entriesWithSameVector + 1);
            using var __ = filter;

            for (int i = 1; i <= entriesWithSameVector; i++)
                filter.Add(i);
            filter.Count = entriesWithSameVector;

            var matches = new long[matchesBufferSize];
            var distances = new float[matchesBufferSize];
            List<long> allResults = new();

            int read;
            do
            {
                read = nearest.Fill(matches, distances, filter);
                allResults.AddRange(matches[..read]);
            } while (read != 0);

            Assert.Equal(entriesWithSameVector, allResults.Count);
            allResults.Sort();
            Assert.Equal(Enumerable.Range(1, entriesWithSameVector).Select(x => (long)x), allResults);
        }
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineData(1241232)]
    [InlineData(237493337)]
    public void CanReturnAllOfVector(int seed)
    {
        using var _ = Slice.From(Allocator, nameof(CanReturnAllOfVector), out var treeName);
        const int vectorSize = 1536;
        const int vectorSizeInBytes = vectorSize * sizeof(float);
        const int numberOfEntries = 1024;
        var random = new Random(seed);
        Dictionary<long, float[]> storage = new();
        for (int i = 1; i <= numberOfEntries; ++i)
            storage.Add(i, Enumerable.Range(0, vectorSize).Select(_ => GetNextDim()).ToArray());

        using (var wTx = Env.WriteTransaction())
        {
            Hnsw.Create(wTx.LowLevelTransaction, treeName, vectorSizeInBytes, 3, 16, VectorEmbeddingType.Single);

            using (var registration = Hnsw.RegistrationFor(wTx.LowLevelTransaction, treeName, random))
            {
                foreach (var (id, vector) in storage)
                    registration.Register(id, MemoryMarshal.Cast<float, byte>(vector));

                registration.Commit(CancellationToken.None);
            }

            wTx.Commit();
        }

        using (var rTx = Env.ReadTransaction())
        {
            var qV = MemoryMarshal.Cast<float, byte>(storage[random.Next(1, storage.Count + 1)]);
            using var nearest = Hnsw.ExactNearest(rTx.LowLevelTransaction, treeName, 1024, qV.ToArray(), 0f, false);

            var totalReturned = 0;
            var matches = new long[64];
            var distances = new float[64];
            List<long> returnedDocuments = new();

            var read = 0;
            do
            {
                read = nearest.Fill(matches, distances, filter: null);
                totalReturned += read;
                returnedDocuments.AddRange(matches[..read]);
            } while (read != 0);

            Assert.Equal(numberOfEntries, totalReturned);
        }


        float GetNextDim() => random.NextSingle() * (random.Next() % 2 == 0 ? 1 : -1);
    }
}

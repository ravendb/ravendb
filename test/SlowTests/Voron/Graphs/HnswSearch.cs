using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using FastTests.Voron;
using FastTests.Voron.FixedSize;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Xunit;

namespace SlowTests.Voron.Graphs;

public class HnswSearch(ITestOutputHelper output) : StorageTest(output)
{
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

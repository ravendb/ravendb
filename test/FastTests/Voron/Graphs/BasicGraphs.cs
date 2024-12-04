using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics.Tensors;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using FastTests.Voron.FixedSize;
using Raven.Server.Documents.Indexes.VectorSearch;
using Tests.Infrastructure;
using Voron.Data.Graphs;
using Voron.Global;
using Xunit;
using Xunit.Abstractions;
using Random = System.Random;
using VectorEmbeddingType = Voron.Data.Graphs.VectorEmbeddingType;
using VectorOptions = Raven.Client.Documents.Indexes.Vector.VectorOptions;

namespace FastTests.Voron.Graphs;

public class BasicGraphs(ITestOutputHelper output) : StorageTest(output)
{
    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    public void CanTransformIdsFromNormalToInternalAndReverse(int seed)
    {
        var random = new Random(seed);
        var N512 = Vector512<long>.Count;
        var N256 = Vector256<long>.Count;
        
        var sizeOfArray = N512 + N256 + 1; //Vec512 + Vec256 + Scalar
        var arrayToProcess = Enumerable.Range(0, sizeOfArray).Select(_ => random.NextInt64(1, long.MaxValue >> 2)).ToArray();
        var encoded = arrayToProcess.Select(Hnsw.Registration.EntryIdToInternalEntryId).ToArray();

        foreach (var id in encoded)
            Assert.Equal(id & Constants.Graphs.VectorId.EnsureIsSingleMask, 0);
        
        Hnsw.Registration.InternalEntryIdToEntryId(encoded);
        Assert.Equal(arrayToProcess, encoded);
    }
    
    [RavenFact(RavenTestCategory.Voron)]
    public void CanCreateEmptyGraph()
    {
        using (var txw = Env.WriteTransaction())
        {
            Hnsw.Create(txw.LowLevelTransaction, "test", 16, 3, 12, VectorEmbeddingType.Single);

            txw.Commit();
        }

        using (var txr = Env.ReadTransaction())
        {
            var state = new Hnsw.SearchState(txr.LowLevelTransaction, "test");
            var options = state.Options;
            Assert.Equal(12, options.NumberOfCandidates);
            Assert.Equal(3, options.NumberOfEdges);
            Assert.Equal(0, options.CountOfVectors);
            float[] v1 = [0.1f, 0.2f, 0.3f, 0.4f];

            using var nearest = Hnsw.ApproximateNearest(txr.LowLevelTransaction, "test",
                numberOfCandidates: 32,
                MemoryMarshal.Cast<float, byte>(v1));
            Span<float> scores = new float[32];
            Span<long> docs = new long[32];
            var r = nearest.Fill(docs, scores);
            Assert.Equal(0, r);
        }
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineData(1199463143)]
    public void BasicSearchBigVec(int seed)
    {
        Random hnswRandom = new Random(seed);

        float[] v1 = Enumerable.Range(0, 1536).Select(_ => hnswRandom.NextSingle() * (hnswRandom.NextInt64() % 2 == 0 ? 1f : -1f)).ToArray();
        float[] v2 = Enumerable.Range(0, 1536).Select(_ => hnswRandom.NextSingle() * (hnswRandom.NextInt64() % 2 == 0 ? 1f : -1f)).ToArray();
        // nearest to v2, then v1
        float[] v3 = v2.Select(x => x + 0.05f).ToArray();

        
        Assert.False(float.IsNaN(TensorPrimitives.CosineSimilarity(v1,v1)));
        Assert.False(float.IsNaN(TensorPrimitives.CosineSimilarity(v1,v2)));
        Assert.False(float.IsNaN(TensorPrimitives.CosineSimilarity(v1,v3)));
        Assert.False(float.IsNaN(TensorPrimitives.CosineSimilarity(v2,v2)));
        Assert.False(float.IsNaN(TensorPrimitives.CosineSimilarity(v2,v3)));
        Assert.False(float.IsNaN(TensorPrimitives.CosineSimilarity(v3,v3)));
        
        using (var txw = Env.WriteTransaction())
        {
            Hnsw.Create(txw.LowLevelTransaction, "test", 1536 * sizeof(float), 3, 12, VectorEmbeddingType.Single);

            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test", hnswRandom))
            {
                registration.Register(1, MemoryMarshal.Cast<float, byte>(v1));
                registration.Register(2, MemoryMarshal.Cast<float, byte>(v2));
                registration.Commit();
            }
            
            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test", hnswRandom))
            {
                registration.Register(3, MemoryMarshal.Cast<float, byte>(v1));
                registration.Commit();
            }

            txw.Commit();
        }

        using (var txr = Env.ReadTransaction())
        {
            var state = new Hnsw.SearchState(txr.LowLevelTransaction, "test");
            var options = state.Options;
            Assert.Equal(12, options.NumberOfCandidates);
            Assert.Equal(3, options.NumberOfEdges);
            Assert.Equal(2, options.CountOfVectors);
        }

        using (var txr = Env.ReadTransaction())
        {
            Span<long> matches = new long[8];
            Span<float> distances = new float[8];
            using var nearest = Hnsw.ApproximateNearest(txr.LowLevelTransaction, "test", numberOfCandidates: 32, MemoryMarshal.Cast<float, byte>(v3));
            int read = nearest.Fill(matches, distances);
             Assert.Equal(3, read);
            Assert.False(distances.Slice(0, read).ToArray().Any(float.IsNaN));
            Assert.Equal(2, matches[0]);
            Assert.Equal(1, matches[1]);
            Assert.Equal(3, matches[2]);
        }
    }
    
    [RavenTheory(RavenTestCategory.Voron)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    public void BasicSearch(int seed)
    {
        Random hnswRandom = new Random(seed);
        float[] v1 = [0.1f, 0.2f, 0.3f, 0.4f];
        float[] v2 = [0.15f, 0.25f, 0.35f, 0.45f];

        // nearest to v2, then v1
        float[] v3 = [0.25f, 0.35f, 0.45f, 0.55f];

        using (var txw = Env.WriteTransaction())
        {
            Hnsw.Create(txw.LowLevelTransaction, "test", 16, 3, 12, VectorEmbeddingType.Single);

            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test", hnswRandom))
            {
                registration.Register(4, MemoryMarshal.Cast<float, byte>(v1));
                registration.Register(8, MemoryMarshal.Cast<float, byte>(v2));
                registration.Commit();
            }
            
            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test", hnswRandom))
            {
                registration.Register(12, MemoryMarshal.Cast<float, byte>(v1));
                registration.Commit();
            }

            txw.Commit();
        }

        using (var txr = Env.ReadTransaction())
        {
            var state = new Hnsw.SearchState(txr.LowLevelTransaction, "test");
            var options = state.Options;
            Assert.Equal(12, options.NumberOfCandidates);
            Assert.Equal(3, options.NumberOfEdges);
            Assert.Equal(2, options.CountOfVectors);
        }

        using (var txr = Env.ReadTransaction())
        {
            Span<long> matches = new long[8];
            Span<float> distances = new float[8];
            using var nearest = Hnsw.ApproximateNearest(txr.LowLevelTransaction, "test",
                numberOfCandidates: 32,
                MemoryMarshal.Cast<float, byte>(v3));
            int read = nearest.Fill(matches, distances);
            Assert.Equal(3, read);
            Assert.Equal(8, matches[0]);
            Assert.Equal(4, matches[1]);
            Assert.Equal(12, matches[2]);
        }
    }
    
    [RavenTheory(RavenTestCategory.Voron)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    public void CanAddAndRemove(int seed)
    {
        Random hnswRandom = new Random(seed);
        float[] v1 = [0.1f, 0.2f, 0.3f, 0.4f];
        float[] v2 = [0.15f, 0.25f, 0.35f, 0.45f];

        // nearest to v2, then v1
        float[] v3 = [0.25f, 0.35f, 0.45f, 0.55f];
        long entryIdToRemove = 4;
        byte[] vectorHashToRemove = default;
        
        using (var txw = Env.WriteTransaction())
        {
            Hnsw.Create(txw.LowLevelTransaction, "test", 16, 3, 12, VectorEmbeddingType.Single);

            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test", hnswRandom))
            {
                vectorHashToRemove = registration.Register(entryIdToRemove, MemoryMarshal.Cast<float, byte>(v1)).ToSpan().ToArray();
                registration.Register(8, MemoryMarshal.Cast<float, byte>(v2));
                registration.Commit();
            }
            
            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test", hnswRandom))
            {
                registration.Register(12, MemoryMarshal.Cast<float, byte>(v1));
                registration.Commit();
            }

            txw.Commit();
        }

        using (var txw = Env.WriteTransaction())
        {
            Hnsw.Create(txw.LowLevelTransaction, "test", 16, 3, 12, VectorEmbeddingType.Single);

            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test", hnswRandom))
            {
                registration.Remove(entryIdToRemove, vectorHashToRemove);
                registration.Commit();
            }

            txw.Commit();
        }

        using (var txr = Env.ReadTransaction())
        {
            Span<long> matches = new long[8];
            Span<float> distances = new float[8];
            using var nearest = Hnsw.ApproximateNearest(txr.LowLevelTransaction, "test",
                numberOfCandidates: 32,
                MemoryMarshal.Cast<float, byte>(v3));
            int read = nearest.Fill(matches, distances);
            Assert.Equal(2, read);
            Assert.Equal(8, matches[0]);
            Assert.Equal(12, matches[1]);
        }
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    public void CanCalculateGoodDistances(int seed)
    {
        var random = new Random(seed);
        var vecOpt = new VectorOptions()
        {
            SourceEmbeddingType = Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType.Text,
            DestinationEmbeddingType = Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType.Single
        };
        using var v1 = GenerateEmbeddings.FromText(vecOpt, "Cat has brown eyes.");
        using var v2 = GenerateEmbeddings.FromText(vecOpt, "Apple usually is red.");
        using var vQ = GenerateEmbeddings.FromText(vecOpt, "animal");

        using (var wTx = Env.WriteTransaction())
        {
            Hnsw.Create(wTx.LowLevelTransaction, nameof(CanCalculateGoodDistances), v1.Length, 3, 12, VectorEmbeddingType.Single);
            using (var registration = Hnsw.RegistrationFor(wTx.LowLevelTransaction, nameof(CanCalculateGoodDistances), random))
            {
                registration.Register(1, v1.GetEmbedding());
                registration.Register(2, v2.GetEmbedding());
                registration.Commit();
            }
            
            wTx.Commit();
        }

        using (var rTx = Env.ReadTransaction())
        {
            using var search = Hnsw.ApproximateNearest(rTx.LowLevelTransaction, nameof(CanCalculateGoodDistances), 12, vQ.GetEmbedding());
            var distances = new float[16];
            var matches = new long[16];
            var read = search.Fill(matches, distances);
            Assert.Equal(2, read);
            var v1Pos = matches.AsSpan().Slice(0, read).IndexOf(1L);
            Assert.True(int.IsPositive(v1Pos));
            Assert.Equal(Hnsw.CosineSimilaritySingles(v1.GetEmbedding(), vQ.GetEmbedding()), distances[v1Pos]);
            var v2Pos = matches.AsSpan().Slice(0, read).IndexOf(2L);
            Assert.True(int.IsPositive(v2Pos));
            Assert.Equal(Hnsw.CosineSimilaritySingles(v2.GetEmbedding(), vQ.GetEmbedding()), distances[v2Pos]);
        }
    }
    
    
    [RavenTheory(RavenTestCategory.Voron)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineData(387668521)]
    public void CanHandleLargePostingLists(int seed)
    {
        Random hnswRandom = new Random(seed);
        float[] v1 = [0.1f, 0.2f, 0.3f, 0.4f];
        float[] v2 = [0.15f, 0.25f, 0.35f, 0.45f];

        // nearest to v2, then v1
        float[] v3 = [0.25f, 0.35f, 0.45f, 0.55f];

        List<(long entryId, byte[] vectorHash)> elementInGraph = new();
        using (var txw = Env.WriteTransaction())
        {
            Hnsw.Create(txw.LowLevelTransaction, "test", 16, 3, 12, VectorEmbeddingType.Single);

            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test", hnswRandom))
            {
                for (int i = 1; i <= 20_000; i++)
                {
                    var id = i;
                    var vec = registration.Register(id, MemoryMarshal.Cast<float, byte>(v1));
                    elementInGraph.Add((id, vec.ToSpan().ToArray()));
                }
                
                registration.Commit();
            }

            txw.Commit();
        }

        List<long> readFromGraph = new();
        using (var txr = Env.ReadTransaction())
        {
            Span<long> matches = new long[500];
            Span<float> distances = new float[500];
            using var nearest = Hnsw.ApproximateNearest(txr.LowLevelTransaction, "test",
                numberOfCandidates: 32,
                MemoryMarshal.Cast<float, byte>(v3));
            var read = 0;
            readFromGraph.Clear();
            do
            {
                read = nearest.Fill(matches, distances);
                readFromGraph.AddRange(matches.Slice(0, read));
            } while (read > 0);
            
            Assert.Equal(elementInGraph.Select(x => x.entryId), readFromGraph);
        }
        
        //reduce to 100 elements
        var toRemove = elementInGraph.ToArray()[100..];
        using (var txw = Env.WriteTransaction())
        {
            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test", hnswRandom))
            {
                foreach (var el in toRemove)
                    registration.Remove(el.entryId, el.vectorHash);
                registration.Commit();
            }
            
            txw.Commit();
        }
        
        using (var txr = Env.ReadTransaction())
        {
            Span<long> matches = new long[500];
            Span<float> distances = new float[500];
            using var nearest = Hnsw.ApproximateNearest(txr.LowLevelTransaction, "test",
                numberOfCandidates: 32,
                MemoryMarshal.Cast<float, byte>(v3));
            readFromGraph.Clear();
            var read = 0;
            do
            {
                read = nearest.Fill(matches, distances);
                readFromGraph.AddRange(matches.Slice(0, read));
            } while (read > 0);
            
            Assert.Equal(100, readFromGraph.Count);
        }
        
        toRemove = elementInGraph.ToArray()[1..];
        using (var txw = Env.WriteTransaction())
        {
            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test", hnswRandom))
            {
                foreach (var id in toRemove)
                    registration.Remove(id.entryId, id.vectorHash);
                
                registration.Commit();
            }
            
            txw.Commit();
        }
        
        using (var txr = Env.ReadTransaction())
        {
            Span<long> matches = new long[100];
            Span<float> distances = new float[100];
            using var nearest = Hnsw.ApproximateNearest(txr.LowLevelTransaction, "test",
                numberOfCandidates: 32,
                MemoryMarshal.Cast<float, byte>(v3));
            readFromGraph.Clear();
            var read = 0;
            do
            {
                read = nearest.Fill(matches, distances);
                readFromGraph.AddRange(matches.Slice(0, read));
            } while (read > 0);
            
            Assert.Equal(1, readFromGraph.Count);
        }
        
        using (var txw = Env.WriteTransaction())
        {
            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test", hnswRandom))
            {
                registration.Remove(elementInGraph[0].entryId, elementInGraph[0].vectorHash);
                registration.Commit();
            }
            
            txw.Commit();
        }
        
        using (var txr = Env.ReadTransaction())
        {
            Span<long> matches = new long[4];
            Span<float> distances = new float[4];
            using var nearest = Hnsw.ApproximateNearest(txr.LowLevelTransaction, "test",
                numberOfCandidates: 32,
                MemoryMarshal.Cast<float, byte>(v3));
            var read = nearest.Fill(matches, distances);
            Assert.Equal(0, read);
        }
    }

    private static void Fill(float[] f, int seed)
    {
        var random = new Random(seed); 
        for (int i = 0; i < f.Length; i++)
        {
            f[i] = random.NextSingle();
        }
    }


    [RavenTheory(RavenTestCategory.Voron)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    public void WithLargeVectors(int seed)
    {
        Random hnswRandom = new Random(seed);
        float[] v1 = new float[768];
        float[] v2 = new float[768];

        // nearest to v2, then v1
        float[] v3 = new float[768];

        Fill(v1, 123);
        Fill(v2, 321);
        Fill(v3, 481);

        using (var txw = Env.WriteTransaction())
        {
            Hnsw.Create(txw.LowLevelTransaction, "test", v1.Length * 4, 3, 12, VectorEmbeddingType.Single);

            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test", hnswRandom))
            {
                registration.Register(4, MemoryMarshal.Cast<float, byte>(v1));
                registration.Register(8, MemoryMarshal.Cast<float, byte>(v2));
                registration.Commit();
            }

            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test", hnswRandom))
            {
                registration.Register(12, MemoryMarshal.Cast<float, byte>(v1));
                registration.Commit();
            }

            txw.Commit();
        }

        using (var txr = Env.ReadTransaction())
        {
            var state = new Hnsw.SearchState(txr.LowLevelTransaction, "test");
            var options = state.Options;
            Assert.Equal(12, options.NumberOfCandidates);
            Assert.Equal(3, options.NumberOfEdges);
            Assert.Equal(2, options.CountOfVectors);
        }

        using (var txr = Env.ReadTransaction())
        {
            Span<long> matches = new long[8];
            Span<float> distances = new float[8];
            using var nearest = Hnsw.ApproximateNearest(txr.LowLevelTransaction, "test",
                numberOfCandidates: 32,
                MemoryMarshal.Cast<float, byte>(v3));
            int read = nearest.Fill(matches, distances);
            Assert.Equal(3, read);
        }
    }
}

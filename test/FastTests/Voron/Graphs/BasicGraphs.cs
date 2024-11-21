using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron.Data.Graphs;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Graphs;

public class BasicGraphs(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void CanCreateEmptyGraph()
    {
        using (var txw = Env.WriteTransaction())
        {
            Hnsw.Create(txw.LowLevelTransaction, "test", 16, 3, 12);

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

    [RavenFact(RavenTestCategory.Voron)]
    public void BasicSearch()
    {
        float[] v1 = [0.1f, 0.2f, 0.3f, 0.4f];
        float[] v2 = [0.15f, 0.25f, 0.35f, 0.45f];

        // nearest to v2, then v1
        float[] v3 = [0.25f, 0.35f, 0.45f, 0.55f];

        using (var txw = Env.WriteTransaction())
        {
            Hnsw.Create(txw.LowLevelTransaction, "test", 16, 3, 12);

            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test"))
            {
                registration.Register(4, MemoryMarshal.Cast<float, byte>(v1));
                registration.Register(8, MemoryMarshal.Cast<float, byte>(v2));
                registration.Commit();
            }
            
            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test"))
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
    
    

    [RavenFact(RavenTestCategory.Voron)]
    public void CanAddAndRemove()
    {
        float[] v1 = [0.1f, 0.2f, 0.3f, 0.4f];
        float[] v2 = [0.15f, 0.25f, 0.35f, 0.45f];

        // nearest to v2, then v1
        float[] v3 = [0.25f, 0.35f, 0.45f, 0.55f];
        long entryIdToRemove = 4;
        ByteString vectorHashToRemove = default;
        
        using (var txw = Env.WriteTransaction())
        {
            Hnsw.Create(txw.LowLevelTransaction, "test", 16, 3, 12);

            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test"))
            {
                vectorHashToRemove = registration.Register(entryIdToRemove, MemoryMarshal.Cast<float, byte>(v1));
                registration.Register(8, MemoryMarshal.Cast<float, byte>(v2));
                registration.Commit();
            }
            
            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test"))
            {
                registration.Register(12, MemoryMarshal.Cast<float, byte>(v1));
                registration.Commit();
            }

            txw.Commit();
        }

        using (var txw = Env.WriteTransaction())
        {
            Hnsw.Create(txw.LowLevelTransaction, "test", 16, 3, 12);

            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test"))
            {
                registration.Remove(entryIdToRemove, vectorHashToRemove.ToSpan());
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



    [RavenFact(RavenTestCategory.Voron)]
    public void CanHandleLargePostingLists()
    {
        float[] v1 = [0.1f, 0.2f, 0.3f, 0.4f];
        float[] v2 = [0.15f, 0.25f, 0.35f, 0.45f];

        // nearest to v2, then v1
        float[] v3 = [0.25f, 0.35f, 0.45f, 0.55f];

        List<(long entryId, ByteString vectorHash)> elementInGraph = new();
        using (var txw = Env.WriteTransaction())
        {
            Hnsw.Create(txw.LowLevelTransaction, "test", 16, 3, 12);

            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test"))
            {
                for (int i = 0;i < 20_000; i++)
                {
                    var id = (i + 1) * 4;
                    var vec = registration.Register(id, MemoryMarshal.Cast<float, byte>(v1));
                    elementInGraph.Add((id, vec));
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
            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test"))
            {
                foreach (var el in toRemove)
                    registration.Remove(el.entryId, el.vectorHash.ToSpan());
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
            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test"))
            {
                foreach (var id in toRemove)
                    registration.Remove(id.entryId, id.vectorHash.ToSpan());
                
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
            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test"))
            {
                registration.Remove(elementInGraph[0].entryId, elementInGraph[0].vectorHash.ToSpan());
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


    [RavenFact(RavenTestCategory.Voron)]
    public void WithLargeVectors()
    {
        float[] v1 = new float[768];
        float[] v2 = new float[768];

        // nearest to v2, then v1
        float[] v3 = new float[768];

        Fill(v1, 123);
        Fill(v2, 321);
        Fill(v3, 481);

        using (var txw = Env.WriteTransaction())
        {
            Hnsw.Create(txw.LowLevelTransaction, "test", v1.Length * 4, 3, 12);

            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test"))
            {
                registration.Register(4, MemoryMarshal.Cast<float, byte>(v1));
                registration.Register(8, MemoryMarshal.Cast<float, byte>(v2));
                registration.Commit();
            }

            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test"))
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

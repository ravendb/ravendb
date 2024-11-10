using System;
using System.Runtime.InteropServices;
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
            }
            
            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test"))
            {
                registration.Register(12, MemoryMarshal.Cast<float, byte>(v1));
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
}

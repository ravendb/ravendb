using System;
using System.Runtime.InteropServices;
using Tests.Infrastructure;
using Voron.Data.Graphs;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Graphs;

public class GraphsVectorRemovals(ITestOutputHelper output) : StorageTest(output)
{
    private const string TreeName = "test";
    private const int VectorSizeInBytes = 4 * sizeof(float);
    
    [RavenFact(RavenTestCategory.Voron)]
    public void CanRemoveSingle()
    {
        float[] v1 = [0.1f, 0.2f, 0.3f, 0.4f];
        var v1AsBytes = MemoryMarshal.Cast<float, byte>(v1);
        var entryId = 1 << 2;
        using (var wTx = Env.WriteTransaction())
        {
            Hnsw.Create(wTx.LowLevelTransaction, TreeName, VectorSizeInBytes, 3, 12);
            using (var registration = Hnsw.RegistrationFor(wTx.LowLevelTransaction, TreeName))
            {
                registration.Register(entryId, v1AsBytes);
            }

            wTx.Commit();
        }

        using (var rTx = Env.ReadTransaction())
        {
            using var s = Hnsw.ExactNearest(rTx.LowLevelTransaction, TreeName, 1, v1AsBytes);
            Span<long> docs = new long[4];
            Span<float> distances = new float[4];
            var r = s.Fill(docs, distances);
            Assert.Equal(1, r);
        }

        using (var wTx = Env.WriteTransaction())
        {
            Hnsw.Create(wTx.LowLevelTransaction, TreeName, VectorSizeInBytes, 3, 12);
            using (var registration = Hnsw.RegistrationFor(wTx.LowLevelTransaction, TreeName))
            {
                registration.Remove(entryId, v1AsBytes);
            }
            
            wTx.Commit();
        }
        
        using (var rTx = Env.ReadTransaction())
        {
            using var s = Hnsw.ApproximateNearest(rTx.LowLevelTransaction, TreeName, 12, v1AsBytes);
            Span<long> docs = new long[4];
            Span<float> distances = new float[4];
            var r = s.Fill(docs, distances);
            Assert.Equal(0, r);
        }
    }
    
    [RavenFact(RavenTestCategory.Voron)]
    public void CanRemoveDoubleSingle()
    {
        float[] v1 = [0.1f, 0.2f, 0.3f, 0.4f];
        float[] v2 = [0.2f, 0.3f, 0.4f, 0.1f];
        var v1AsBytes = MemoryMarshal.Cast<float, byte>(v1);
        var v2AsBytes = MemoryMarshal.Cast<float, byte>(v2);
        var entryId1 = 1 << 2;
        var entryId2 = 2 << 2;
        using (var wTx = Env.WriteTransaction())
        {
            Hnsw.Create(wTx.LowLevelTransaction, "test", VectorSizeInBytes, 3, 12);
            using (var registration = Hnsw.RegistrationFor(wTx.LowLevelTransaction, "test"))
            {
                registration.Register(entryId1, v1AsBytes);
                registration.Register(entryId2, v2AsBytes);
            }

            wTx.Commit();
        }

        using (var rTx = Env.ReadTransaction())
        {
            using var s = Hnsw.ExactNearest(rTx.LowLevelTransaction, TreeName, 2, v1AsBytes);
            Span<long> docs = new long[4];
            Span<float> distances = new float[4];
            var r = s.Fill(docs, distances);
            Assert.Equal(2, r);
        }

        using (var wTx = Env.WriteTransaction())
        {
            using (var registration = Hnsw.RegistrationFor(wTx.LowLevelTransaction, "test"))
            {
                registration.Remove(entryId1, v1AsBytes);
            }
            wTx.Commit();
        }
        
        using (var rTx = Env.ReadTransaction())
        {
            using var s = Hnsw.ApproximateNearest(rTx.LowLevelTransaction, TreeName, 12, v1AsBytes);
            Span<long> docs = new long[4];
            Span<float> distances = new float[4];
            var r = s.Fill(docs, distances);
            Assert.Equal(1, r);
            Assert.Equal(entryId2, docs[0]);
        }
    }
    
    
}

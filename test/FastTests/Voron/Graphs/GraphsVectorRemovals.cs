using System;
using System.Runtime.InteropServices;
using Sparrow.Server;
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
        ByteString vectorHash = default;
        using (var wTx = Env.WriteTransaction())
        {
            Hnsw.Create(wTx.LowLevelTransaction, TreeName, VectorSizeInBytes, 3, 12);
            using (var registration = Hnsw.RegistrationFor(wTx.LowLevelTransaction, TreeName))
            {
                vectorHash = registration.Register(entryId, v1AsBytes);
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
                registration.Remove(entryId, vectorHash.ToSpan());
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
        ByteString v1Id = default;
        var entryId2 = 2 << 2;
        using (var wTx = Env.WriteTransaction())
        {
            Hnsw.Create(wTx.LowLevelTransaction, "test", VectorSizeInBytes, 3, 12);
            using (var registration = Hnsw.RegistrationFor(wTx.LowLevelTransaction, "test"))
            {
                v1Id = registration.Register(entryId1, v1AsBytes);
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
                registration.Remove(entryId1, v1Id.ToSpan());
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

    [RavenFact(RavenTestCategory.Voron)]
    public void CanExpandNodeFromSingleToSmallPostingList()
    {
        var v1Id = AddElement(1, register: true);
        var v2Id = AddElement(2, register: true);
        var result = Read();
        Assert.Equal(2, result.Read);
        Assert.Equal(result.Docs[..2], new[] { EntryId(1), EntryId(2)});
        
        RemoveElement(2, v2Id);
        result = Read();
        Assert.Equal(1, result.Read);
        Assert.Equal(EntryId(1), result.Docs[0]);

        long EntryId(int i) => i << 2; 

        ByteString AddElement(int id, bool register = false)
        {
            float[] v1 = [0.1f, 0.2f, 0.3f, 0.4f];
            var v1AsBytes = MemoryMarshal.Cast<float, byte>(v1);
            ByteString vecId = default;
            using (var wTx = Env.WriteTransaction())
            {
                if (register)
                    Hnsw.Create(wTx.LowLevelTransaction, "test", VectorSizeInBytes, 3, 12);

                using (var registration = Hnsw.RegistrationFor(wTx.LowLevelTransaction, "test"))
                {
                    vecId = registration.Register(EntryId(id), v1AsBytes);
                }

                wTx.Commit();
            }

            return vecId;
        }
        void RemoveElement(int id, ByteString vecId)
        {
            float[] v1 = [0.1f, 0.2f, 0.3f, 0.4f];
            var v1AsBytes = MemoryMarshal.Cast<float, byte>(v1);
            
            using (var wTx = Env.WriteTransaction())
            {
                using (var registration = Hnsw.RegistrationFor(wTx.LowLevelTransaction, "test"))
                {
                    registration.Remove(EntryId(id), vecId.ToSpan());
                }

                wTx.Commit();
            }
        }

        (int Read, long[] Docs, float[] Distances) Read()
        {
            float[] v1 = [0.1f, 0.2f, 0.3f, 0.4f];
            var v1AsBytes = MemoryMarshal.Cast<float, byte>(v1);
            
            using (var rTx = Env.ReadTransaction())
            {
                using var s = Hnsw.ExactNearest(rTx.LowLevelTransaction, TreeName, 12, v1AsBytes);
                var docs = new long[4];
                var distances = new float[4];
                var r = s.Fill(docs, distances);
                return (r, docs, distances);
            }
        }
    }
    
    [RavenFact(RavenTestCategory.Voron)]
    public void CanRemoveBasedOnEntryIdAndVectorHashAddress()
    {
        float[] v1 = [.4f, .4f, .4f, .4f];
        var v1AsBytes = MemoryMarshal.Cast<float, byte>(v1);
        var entryId = 4;
        ByteString vectorTermContainer = default;
        using (var txw = Env.WriteTransaction())
        {
            Hnsw.Create(txw.LowLevelTransaction, "test", 16, 3, 12);

            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test"))
            {
                vectorTermContainer = registration.Register(4, MemoryMarshal.Cast<float, byte>(v1));
            }
            
            txw.Commit();
        }
        
        using (var txr = Env.ReadTransaction())
        {
            Span<long> matches = new long[8];
            Span<float> distances = new float[8];
            using var nearest = Hnsw.ApproximateNearest(txr.LowLevelTransaction, "test",
                numberOfCandidates: 32,
                v1AsBytes);
            int read = nearest.Fill(matches, distances);
            Assert.Equal(1, read);
        }

        using (var txw = Env.WriteTransaction())
        {
            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, "test"))
                registration.Remove(entryId, vectorTermContainer.ToSpan());

            txw.Commit();
        }
        
        using (var txr = Env.ReadTransaction())
        {
            Span<long> matches = new long[8];
            Span<float> distances = new float[8];
            using var nearest = Hnsw.ApproximateNearest(txr.LowLevelTransaction, "test",
                numberOfCandidates: 32,
                v1AsBytes);
            int read = nearest.Fill(matches, distances);
            Assert.Equal(0, read);
        }
    }
}

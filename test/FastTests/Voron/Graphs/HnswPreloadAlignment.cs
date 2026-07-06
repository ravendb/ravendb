using System.Collections.Generic;
using Sparrow;
using Tests.Infrastructure;
using Voron.Data.Graphs;
using Xunit;

namespace FastTests.Voron.Graphs;

public unsafe class HnswPreloadAlignment(ITestOutputHelper output) : StorageTest(output)
{
    // When an unfound edge makes edgesIndexes shorter than edgesList, the preload
    // collector must batch the node behind edgesIndexes[i], not the positionally
    // misaligned edgesList[i]. Getting this wrong preloads the wrong vector and the
    // needed one is demand-loaded via a singleton Container.Get that NREs under eviction.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void CollectVectorsToPreload_batches_the_node_behind_the_index()
    {
        byte[] loaded = new byte[1];
        fixed (byte* p = loaded)
        {
            Hnsw.Node a = new() { NodeId = 100 };
            a.SetVector(default, new UnmanagedSpan(p, 1)); // VectorLoaded == true
            Hnsw.Node b = new() { NodeId = 200 };          // unfound edge — skipped, never in edgesIndexes
            Hnsw.Node c = new() { NodeId = 300 };          // resolved, vector NOT loaded

            Hnsw.Node[] nodes = [a, b, c];
            int[] edgesIndexes = [0, 2]; // compacted: B was skipped

            List<long> batch = [];
            Hnsw.Registration.CollectVectorsToPreload(edgesIndexes, nodes, batch);

            Assert.Equal(new List<long> { 300 }, batch);
        }
    }
}

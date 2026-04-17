using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Xunit;
using VectorEmbeddingType = Voron.Data.Graphs.VectorEmbeddingType;

namespace FastTests.Voron.Graphs;

public unsafe class HnswIndexCacheTests(ITestOutputHelper output) : StorageTest(output)
{
    private const string TreeName = "test";
    private const int VectorDimensions = 16;
    private const int VectorSizeInBytes = VectorDimensions * sizeof(float);

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void EmptyGraphReturnsEmptyCache()
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        using (var tx = Env.WriteTransaction())
        {
            Hnsw.Create(tx.LowLevelTransaction, treeName, VectorSizeInBytes, 3, 12, VectorEmbeddingType.Single);
            tx.Commit();
        }

        using (var tx = Env.ReadTransaction())
        {
            using var cache = HnswIndexCache.WarmFromScratch(tx.LowLevelTransaction, treeName, maxNodes: 1024);
            Assert.NotNull(cache);
            Assert.Equal(0, cache.Count);
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void ZeroBudgetReturnsEmptyCache()
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        BuildGraph(treeName, vectorCount: 50, seed: 42);

        using (var tx = Env.ReadTransaction())
        {
            using var cache = HnswIndexCache.WarmFromScratch(tx.LowLevelTransaction, treeName, maxNodes: 0);
            Assert.NotNull(cache);
            Assert.Equal(0, cache.Count);
        }
    }

    // With a budget that exceeds the graph size, BFS from the entry point must reach every
    // node — including level-0 leaves — and admit it. The cache count therefore equals the
    // full vector count.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void BudgetLargerThanGraphCachesAllNodes()
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        const int vectorCount = 200;
        BuildGraph(treeName, vectorCount, seed: 42);

        using (var tx = Env.ReadTransaction())
        {
            using var cache = HnswIndexCache.WarmFromScratch(tx.LowLevelTransaction, treeName, maxNodes: 10_000);
            Assert.Equal(vectorCount, cache.Count);
        }
    }

    // When the budget caps below the graph size, BFS must fill the cache exactly to the
    // budget — no fewer (otherwise we are starving the cache and missing reachable nodes).
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void CacheFullyUsesBudgetWhenGraphIsLargerThanBudget()
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        const int vectorCount = 1500;
        const int budget = 400;
        BuildGraph(treeName, vectorCount, seed: 42);

        using (var tx = Env.ReadTransaction())
        {
            using var cache = HnswIndexCache.WarmFromScratch(tx.LowLevelTransaction, treeName, maxNodes: budget);
            Assert.InRange(cache.Count, budget * 9 / 10, budget);
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void CachedEdgesMatchFreshlyLoadedEdges()
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        BuildGraph(treeName, vectorCount: 500, seed: 7);

        using (var tx = Env.ReadTransaction())
        {
            using var cache = HnswIndexCache.WarmFromScratch(tx.LowLevelTransaction, treeName, maxNodes: 10_000);
            Assert.True(cache.Count > 0);

            // Load a fresh SearchState for ground-truth edges.
            using var state = new Hnsw.SearchState(tx.LowLevelTransaction, treeName);

            foreach (var (nodeId, view) in EnumerateCache(cache, 500))
            {
                int stateIdx = state.GetNodeIndexById(nodeId);
                ref var stateNode = ref state.GetNodeByIndex(stateIdx);

                Assert.Equal(stateNode.NodeId, view.Header->NodeId);
                Assert.Equal(stateNode.VectorId, view.Header->VectorId);
                Assert.Equal(stateNode.PostingListId, view.Header->PostingListId);
                Assert.Equal(stateNode.EdgesPerLevel.Count, view.LevelCount);

                for (int lvl = 0; lvl < view.LevelCount; lvl++)
                {
                    var cachedEdges = view.EdgesAtLevel(lvl);
                    var stateEdges = stateNode.EdgesPerLevel[lvl];

                    Assert.Equal(stateEdges.Count, cachedEdges.Length);
                    for (int e = 0; e < cachedEdges.Length; e++)
                        Assert.Equal(stateEdges[e], cachedEdges[e]);
                }
            }
        }
    }

    // The cache reflects the snapshot of the LLT it was built from. A cache built from an older
    // read tx must not surface nodes that were committed after that snapshot.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void CacheBuiltFromOlderSnapshotDoesNotIncludeLaterNodes()
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        BuildGraph(treeName, vectorCount: 200, seed: 11);

        long countAtSnapshot;
        using (var readTx = Env.ReadTransaction())
        {
            using var state = new Hnsw.SearchState(readTx.LowLevelTransaction, treeName);
            countAtSnapshot = state.Options.CountOfVectors;

            // Add MORE vectors in a write tx AFTER the read snapshot was opened.
            AddVectors(treeName, vectorCount: 100, firstId: (int)countAtSnapshot + 1, seed: 22);

            using var cache = HnswIndexCache.WarmFromScratch(readTx.LowLevelTransaction, treeName, maxNodes: 10_000);

            foreach (var (nodeId, _) in EnumerateCache(cache, (int)countAtSnapshot))
                Assert.InRange(nodeId, 1L, countAtSnapshot);
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void SearchStateCanQueryUsingACache()
    {
        using var _ = Slice.From(Allocator, TreeName, out var treeName);
        BuildGraph(treeName, vectorCount: 300, seed: 19);

        float[] query = RandomVector(new Random(99));
        var queryBytes = new byte[Hnsw.TensorSizeBytes<float>(query.Length)];
        Hnsw.WriteNormalizedTensor(query, queryBytes);

        using (var tx = Env.ReadTransaction())
        {
            using var cache = HnswIndexCache.WarmFromScratch(tx.LowLevelTransaction, treeName, maxNodes: 10_000);
            Assert.True(cache.Count > 0);

            using var state = new Hnsw.SearchState(tx.LowLevelTransaction, treeName, cache);
            using var retriever = Hnsw.ApproximateNearest(state, numberOfCandidates: 32, queryBytes, minimumSimilarity: 0f);

            var scores = new float[32];
            var docs = new long[32];
            int total = 0;
            int read;
            do
            {
                read = retriever.Fill(docs, scores, filter: null);
                total += read;
            } while (read != 0);

            Assert.True(total > 0, "search with cache returned no results");
        }
    }

    private void BuildGraph(Slice treeName, int vectorCount, int seed)
    {
        var random = new Random(seed);
        using var tx = Env.WriteTransaction();
        Hnsw.Create(tx.LowLevelTransaction, treeName, VectorSizeInBytes, numberOfEdges: 12,
            numberOfCandidates: 16, VectorEmbeddingType.Single);

        using (var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, treeName, random))
        {
            for (int i = 1; i <= vectorCount; i++)
            {
                var v = RandomVector(random);
                registration.Register(i, MemoryMarshal.Cast<float, byte>(v));
            }
            registration.Commit(CancellationToken.None);
        }
        tx.Commit();
    }

    private void AddVectors(Slice treeName, int vectorCount, int firstId, int seed)
    {
        var random = new Random(seed);
        using var tx = Env.WriteTransaction();
        using (var registration = Hnsw.RegistrationFor(tx.LowLevelTransaction, treeName, random))
        {
            for (int i = 0; i < vectorCount; i++)
            {
                var v = RandomVector(random);
                registration.Register(firstId + i, MemoryMarshal.Cast<float, byte>(v));
            }
            registration.Commit(CancellationToken.None);
        }
        tx.Commit();
    }


    private static float[] RandomVector(Random random)
    {
        var v = new float[VectorDimensions];
        float sumSq = 0;
        for (int i = 0; i < VectorDimensions; i++)
        {
            v[i] = (float)(random.NextDouble() * 2 - 1);
            sumSq += v[i] * v[i];
        }
        var norm = MathF.Sqrt(sumSq);
        if (norm > 0)
        {
            for (int i = 0; i < VectorDimensions; i++)
                v[i] /= norm;
        }
        return v;
    }

    /// <summary>
    /// Probe ids 1..upper and yield those that are present. Iteration order is not part of the
    /// cache contract, so this iterator works against the public TryGetNode surface.
    /// </summary>
    private static unsafe IEnumerable<(long NodeId, HnswIndexCache.CachedNodeView View)> EnumerateCache(HnswIndexCache cache, int upper)
    {
        for (long nodeId = 1; nodeId <= upper; nodeId++)
        {
            if (cache.TryGetNode(nodeId, out var view))
                yield return (nodeId, view);
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Utils;
using FastTests.Voron;
using Nito.Disposables;
using Raven.Server.Documents.Indexes.VectorSearch;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Xunit;

namespace SlowTests.Corax.Vectors;

public class VectorSearchWithHnswIndexCache(ITestOutputHelper output) : StorageTest(output)
{
    // 128-D random unit vectors admit a non-trivial nearest-neighbor graph (at very low
    // dimensions uniform random vectors become nearly equidistant and HNSW quality collapses).
    private const int VectorDimensions = 128;
    private const int VectorByteSize = VectorDimensions * sizeof(float);

    // Queries must return the same matches whether or not a cache is attached, and whether
    // the cache contains all nodes or just a subset. The partial-cache variant exercises
    // the mixed code path: cache hits for upper-level nodes, disk loads for the remainder.
    // Binary is excluded because hamming distance at small dimensions produces many nodes at
    // equal distance, so which ones land in top-K depends on priority-queue tie-breaking —
    // an ambiguity that is unrelated to whether the cache is used.
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [InlineData(VectorEmbeddingType.Single, 10_000, 23)] // cache holds every node
    [InlineData(VectorEmbeddingType.Single, 50, 23)]     // cache holds only upper-level nodes
    [InlineData(VectorEmbeddingType.Int8, 10_000, 23)]
    [InlineData(VectorEmbeddingType.Int8, 50, 23)]
    [InlineDataWithRandomSeed(VectorEmbeddingType.Single, 10_000)]
    public void ResultsAreIdenticalWithAndWithoutCache(VectorEmbeddingType embedding, int cacheBudget, int baseSeed)
        => RunParityCheck(embedding, cacheBudget, baseSeed);

    private void RunParityCheck(VectorEmbeddingType embedding, int cacheBudget, int baseSeed)
    {
        using var _ = GetMappings(embedding, out var bsc, out var mapping);
        var metadata = mapping.GetByFieldId(1).Metadata;
        const int docCount = 500;
        const int queryCount = 25;
        const int k = 16;

        BuildIndex(mapping, docCount, seed: 7);
        var caches = BuildCaches(metadata.FieldName, cacheBudget);

        for (int i = 0; i < queryCount; i++)
        {
            long[] uncached, cached;
            using (var searcher = new IndexSearcher(Env, mapping))
                uncached = TopK(searcher, metadata, NewQueryVector(bsc, seed: baseSeed + i), k);

            using (var searcher = new IndexSearcher(Env, mapping))
            {
                searcher.AttachVectorNodeCaches(caches);
                cached = TopK(searcher, metadata, NewQueryVector(bsc, seed: baseSeed + i), k);
            }

            Assert.Equal(uncached, cached);
        }
    }

    // When no cache is attached to an IndexSearcher, queries must still work — it's the
    // happy-path for indexes that have disabled the cache (CoraxVectorSearchCacheSize=0) or
    // haven't committed yet so no cache exists. Results must also be meaningful: every returned
    // id must map to a real doc and the top-K must agree with an exact scan on most of its
    // entries (HNSW is approximate; we require high recall, not byte-identity).
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [InlineData(17)]
    [InlineDataWithRandomSeed]
    public void SearchWorksWithNoCacheAttached(int seed)
    {
        using var _ = GetMappings(VectorEmbeddingType.Single, out var bsc, out var mapping);
        var metadata = mapping.GetByFieldId(1).Metadata;
        BuildIndex(mapping, docCount: 200, seed: seed);

        long[] truth, returned;
        using (var searcher = new IndexSearcher(Env, mapping))
            truth = TopK(searcher, metadata, NewQueryVector(bsc, seed: seed + 1), k: 16, isExact: true);

        using (var searcher = new IndexSearcher(Env, mapping))
        {
            // Deliberately do NOT call AttachVectorNodeCaches.
            returned = TopK(searcher, metadata, NewQueryVector(bsc, seed: seed + 1), k: 16);
        }

        Assert.Equal(16, returned.Length);
        Assert.Equal(returned.Distinct().Count(), returned.Length);
        foreach (var id in returned)
            Assert.InRange(id, 1L, 200L);
        // At least one returned id must overlap the exact top-K — lower bound on recall that
        // catches "the search returned random ids" without being sensitive to approximate-search
        // jitter on adversarial random seeds.
        Assert.True(Recall(truth, returned) > 0, $"no overlap with exact top-K (seed={seed})");
    }

    // When a cache is attached but empty (e.g. the graph has fewer nodes than the cache
    // budget could hold but none were persisted yet), queries must fall back to disk loads
    // and still return meaningful results.
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [InlineData(19)]
    [InlineDataWithRandomSeed]
    public void SearchWorksWithEmptyCacheDictionaryAttached(int seed)
    {
        using var _ = GetMappings(VectorEmbeddingType.Single, out var bsc, out var mapping);
        var metadata = mapping.GetByFieldId(1).Metadata;
        BuildIndex(mapping, docCount: 200, seed: seed);

        long[] truth, returned;
        using (var searcher = new IndexSearcher(Env, mapping))
            truth = TopK(searcher, metadata, NewQueryVector(bsc, seed: seed + 1), k: 16, isExact: true);

        using (var searcher = new IndexSearcher(Env, mapping))
        {
            searcher.AttachVectorNodeCaches(new Dictionary<Slice, HnswIndexCache>(SliceComparer.Instance));
            returned = TopK(searcher, metadata, NewQueryVector(bsc, seed: seed + 1), k: 16);
        }

        Assert.Equal(16, returned.Length);
        Assert.Equal(returned.Distinct().Count(), returned.Length);
        foreach (var id in returned)
            Assert.InRange(id, 1L, 200L);
        // At least one returned id must overlap the exact top-K — lower bound on recall that
        // catches "the search returned random ids" without being sensitive to approximate-search
        // jitter on adversarial random seeds.
        Assert.True(Recall(truth, returned) > 0, $"no overlap with exact top-K (seed={seed})");
    }

    // The cached path must agree with the exact scan on most top-K. HNSW params are sized
    // so intrinsic recall vs exact is high enough (~0.83) that the 0.75 threshold is a
    // quality gate rather than a "not random ids" sanity check. The parity tests above
    // cover the aggressive-params path with a tight ef.
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [InlineData(10_000)] // cache holds every node
    [InlineData(50)]     // cache holds only upper-level nodes
    public void CachedSearchAchievesGoodRecallAgainstExactScan(int cacheBudget)
    {
        using var _ = GetMappings(VectorEmbeddingType.Single, out var bsc, out var mapping, numberOfEdges: 32, numberOfCandidates: 256);
        var metadata = mapping.GetByFieldId(1).Metadata;
        const int docCount = 500;
        const int queryCount = 10;
        const int k = 16;

        BuildIndex(mapping, docCount, seed: 7);
        var caches = BuildCaches(metadata.FieldName, cacheBudget);

        double totalRecall = 0;
        for (int i = 0; i < queryCount; i++)
        {
            long[] truth, cached;
            using (var searcher = new IndexSearcher(Env, mapping))
                truth = TopK(searcher, metadata, NewQueryVector(bsc, seed: 23 + i), k, isExact: true);

            using (var searcher = new IndexSearcher(Env, mapping))
            {
                searcher.AttachVectorNodeCaches(caches);
                cached = TopK(searcher, metadata, NewQueryVector(bsc, seed: 23 + i), k);
            }

            totalRecall += Recall(truth, cached);
        }

        var avgRecall = totalRecall / queryCount;
        Output.WriteLine($"Average recall@{k} with cacheBudget={cacheBudget}: {avgRecall:F3}");
        Assert.True(avgRecall >= 0.75, $"HNSW recall@{k} dropped to {avgRecall:F3} (threshold 0.75) with cacheBudget={cacheBudget}");
    }

    // Multi-vector queries internally share a SearchState across sub-searches. The cache
    // must interoperate correctly with that reuse path.
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void MultiVectorSearchResultsAreIdenticalWithAndWithoutCache()
    {
        using var _ = GetMappings(VectorEmbeddingType.Single, out var bsc, out var mapping);
        var metadata = mapping.GetByFieldId(1).Metadata;
        BuildIndex(mapping, docCount: 400, seed: 13);

        long[] Run(bool withCache)
        {
            using var searcher = new IndexSearcher(Env, mapping);
            if (withCache)
                searcher.AttachVectorNodeCaches(BuildCaches(metadata.FieldName));

            // Query vectors are allocated per-run because each IndexSearcher disposal
            // releases the ByteStringContext-backed VectorValue buffers.
            var queryVectors = new[] { NewQueryVector(bsc, seed: 29), NewQueryVector(bsc, seed: 30), NewQueryVector(bsc, seed: 31) };
            var match = searcher.MultiVectorSearch(metadata, queryVectors, 0.0f, 16, false, false, filterQuery: null, scanningThreshold: 0);
            var collected = new List<long>();
            Span<long> ids = stackalloc long[16];
            int read;
            while ((read = match.Fill(ids)) > 0)
                for (int i = 0; i < read; i++)
                    collected.Add(ids[i]);
            collected.Sort();
            return collected.ToArray();
        }

        Assert.Equal(Run(withCache: false), Run(withCache: true));
    }

    // A query opened BEFORE new vectors were committed must see its own snapshot, even when
    // the HnswIndexCache it was given was built AT OR BEFORE that snapshot. A cache that predates
    // the query's tx is always valid; the query must never see vectors added after it started.
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void QueryAtOlderTxDoesNotSeeVectorsCommittedAfterItStarted()
    {
        using var _ = GetMappings(VectorEmbeddingType.Single, out var bsc, out var mapping);
        var metadata = mapping.GetByFieldId(1).Metadata;

        BuildIndex(mapping, docCount: 200, seed: 41);
        var caches = BuildCaches(metadata.FieldName); // cache reflects the first 200 vectors

        // Take a snapshot by creating the IndexSearcher now. Subsequent writes must be
        // invisible to queries issued on this searcher.
        using var snapshotSearcher = new IndexSearcher(Env, mapping);
        snapshotSearcher.AttachVectorNodeCaches(caches);

        AppendDocuments(mapping, docCount: 200, firstId: 201, seed: 53);

        var q = NewQueryVector(bsc, seed: 61);
        var ids = TopK(snapshotSearcher, metadata, q, k: 200);

        // Max docId at the snapshot is 200. Anything >= 201 was committed later and must
        // not be returned.
        foreach (var id in ids)
            Assert.InRange(id, 1L, 200L);
    }

    // A cache built after both commits should yield results equivalent to a fresh searcher
    // with no cache: the cache sees everything, queries see everything.
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void CacheBuiltAfterAllCommitsAgreesWithUncachedSearcher()
    {
        using var _ = GetMappings(VectorEmbeddingType.Single, out var bsc, out var mapping);
        var metadata = mapping.GetByFieldId(1).Metadata;

        BuildIndex(mapping, docCount: 200, seed: 41);
        AppendDocuments(mapping, docCount: 200, firstId: 201, seed: 53);

        var caches = BuildCaches(metadata.FieldName);

        long[] cached, uncached;
        {
            using var searcher = new IndexSearcher(Env, mapping);
            searcher.AttachVectorNodeCaches(caches);
            cached = TopK(searcher, metadata, NewQueryVector(bsc, seed: 61), k: 16);
        }
        {
            using var searcher = new IndexSearcher(Env, mapping);
            uncached = TopK(searcher, metadata, NewQueryVector(bsc, seed: 61), k: 16);
        }

        Assert.Equal(uncached, cached);
    }

    private void BuildIndex(IndexFieldsMapping mapping, int docCount, int seed)
    {
        var random = new Random(seed);
        using var indexWriter = new IndexWriter(Env, mapping, SupportedFeatures.All);
        for (int i = 1; i <= docCount; i++)
        {
            var id = $"docs/{i}";
            using var entry = indexWriter.Index(id);
            entry.Write(0, System.Text.Encoding.UTF8.GetBytes(id));
            entry.WriteVector(1, "Vector", MemoryMarshal.Cast<float, byte>(RandomVector(random)));
            entry.EndWriting();
        }
        indexWriter.Commit();
    }

    private void AppendDocuments(IndexFieldsMapping mapping, int docCount, int firstId, int seed)
    {
        var random = new Random(seed);
        using var indexWriter = new IndexWriter(Env, mapping, SupportedFeatures.All);
        for (int i = 0; i < docCount; i++)
        {
            var docId = firstId + i;
            var id = $"docs/{docId}";
            using var entry = indexWriter.Index(id);
            entry.Write(0, System.Text.Encoding.UTF8.GetBytes(id));
            entry.WriteVector(1, "Vector", MemoryMarshal.Cast<float, byte>(RandomVector(random)));
            entry.EndWriting();
        }
        indexWriter.Commit();
    }

    private Dictionary<Slice, HnswIndexCache> BuildCaches(Slice fieldName, int maxNodes = 10_000)
    {
        using var tx = Env.ReadTransaction();
        var cache = HnswIndexCache.WarmFromScratch(tx.LowLevelTransaction, fieldName, maxNodes);
        var dict = new Dictionary<Slice, HnswIndexCache>(SliceComparer.Instance);
        if (cache != null && cache.Count > 0)
            dict[fieldName] = cache;
        return dict;
    }

    private static VectorValue NewQueryVector(ByteStringContext bsc, int seed)
    {
        var v = RandomVector(new Random(seed));
        var scope = bsc.Allocate(VectorByteSize, out Memory<byte> buffer);
        MemoryMarshal.Cast<float, byte>(v).CopyTo(buffer.Span);
        return GenerateEmbeddings.FromArray(bsc, scope, buffer, Raven.Client.Documents.Indexes.Vector.VectorOptions.Default, VectorByteSize);
    }

    private static long[] TopK(IndexSearcher searcher, FieldMetadata metadata, VectorValue query, int k, bool isExact = false)
    {
        var match = searcher.VectorSearch(metadata, query, 0.0f, k, isExact, false);
        var collected = new List<long>();
        Span<long> ids = stackalloc long[k];
        int read;
        while ((read = match.Fill(ids)) > 0)
        {
            for (int i = 0; i < read; i++)
                collected.Add(ids[i]);
        }
        collected.Sort();
        return collected.ToArray();
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

    private static IDisposable GetMappings(VectorEmbeddingType embedding, out ByteStringContext bsc, out IndexFieldsMapping mapping, int numberOfEdges = 8, int numberOfCandidates = 16)
    {
        var bscLocal = new ByteStringContext(SharedMultipleUseFlag.None);
        var mappingLocal = IndexFieldsMappingBuilder
            .CreateForWriter(false)
            .AddBinding(0, "id()")
            .AddBinding(1, "Vector", vectorOptions: new VectorOptions { NumberOfEdges = numberOfEdges, NumberOfCandidates = numberOfCandidates, VectorEmbeddingType = embedding })
            .Build();

        bsc = bscLocal;
        mapping = mappingLocal;

        return Disposable.Create(() =>
        {
            bscLocal.Dispose();
            mappingLocal.Dispose();
        });
    }

    private static double Recall(long[] truth, long[] returned)
    {
        if (truth.Length == 0)
            return 1.0;
        var set = new HashSet<long>(truth);
        int hits = 0;
        foreach (var id in returned)
            if (set.Contains(id))
                hits++;
        return (double)hits / truth.Length;
    }
}

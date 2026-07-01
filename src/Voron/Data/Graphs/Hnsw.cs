using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Threading;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Compression;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Server.Tensors;
using Sparrow.Server.Utils;
using Voron.Data.BTrees;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Global;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;
using Container = Voron.Data.Containers.Container;

namespace Voron.Data.Graphs;

public unsafe partial class Hnsw
{
    public static void Create(LowLevelTransaction llt, string name, int vectorSizeBytes, int numberOfEdges, int numberOfCandidates, VectorEmbeddingType embeddingType)
    {
        using var _ = Slice.From(llt.Allocator, name, out var slice);
        Create(llt, slice, vectorSizeBytes, numberOfEdges, numberOfCandidates, embeddingType);
    }

    public static void Create(LowLevelTransaction llt, Slice name, int vectorSizeBytes, int numberOfEdges, int numberOfCandidates, VectorEmbeddingType embeddingType)
    {
        var tree = llt.Transaction.CreateTree(name);
        if (tree.State.Header.NumberOfEntries is not 0)
            return; // already created

        // global creation for all HNSWs in the database
        var vectorsContainerId = CreateHnswGlobalState(llt);
        ContainerId storage = Container.Create(llt);
        tree.LookupFor<Int64LookupKey>(NodeIdToLocationSlice);
        tree.LookupFor<Int64LookupKey>(NodesByVectorIdSlice);

        var similarityMethod = embeddingType switch
        {
            VectorEmbeddingType.Single => SimilarityMethod.CosineSimilaritySingles,
            VectorEmbeddingType.Int8 => SimilarityMethod.CosineSimilarityI8,
            VectorEmbeddingType.Binary => SimilarityMethod.HammingDistance,
            _ => throw new InvalidOperationException($"Unexpected value of {nameof(VectorEmbeddingType)}: {embeddingType}")
        };

        // VectorEmbeddingType.Single is stored in the NormalizedTensor layout: unit vector followed by a trailing float L2 norm. Callers pass the raw payload size (dims * sizeof(float)); the trailing-norm bytes are added here so callers do not need to know about the layout.
        if (embeddingType == VectorEmbeddingType.Single)
            vectorSizeBytes += MagnitudeSizeInBytes;

        var options = new Options
        {
            Version = Constants.Graphs.HnswVersion.CurrentVersion,
            VectorSizeBytes = vectorSizeBytes,
            CountOfVectors = 0,
            Container = storage,
            NumberOfEdges = numberOfEdges,
            NumberOfCandidates = numberOfCandidates,
            SimilarityMethod = similarityMethod
        };
        using (tree.DirectAdd(OptionsSlice, sizeof(Options), out var output))
        {
            Unsafe.Write(output, options);
        }
    }

    /// <summary>
    /// Returns the distance kernel to use for a given HNSW <see cref="Options"/>. For
    /// <see cref="SimilarityMethod.CosineSimilaritySingles"/> the kernel depends on the on-disk
    /// version: at or above <see cref="Constants.Graphs.HnswVersion.SinglesWithL2Norm"/> the
    /// operands are pre-normalized (NormalizedTensor layout) and the dot-only kernel applies;
    /// below that version the operands are raw vectors and norms are recomputed per call.
    /// </summary>
    internal static delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, float> GetDistanceKernel(in Options options)
    {
        return options.SimilarityMethod switch
        {
            SimilarityMethod.CosineSimilaritySingles =>
                options.Version >= Constants.Graphs.HnswVersion.SinglesWithL2Norm
                    ? &CosineDistanceSinglesNormalized
                    : &CosineDistanceSingles,
            SimilarityMethod.CosineSimilarityI8 => &CosineDistanceI8,
            SimilarityMethod.HammingDistance => &HammingDistance,
            _ => throw new ArgumentOutOfRangeException(nameof(options.SimilarityMethod), options.SimilarityMethod, null)
        };
    }

    private static ContainerId ReadGlobalVectorsContainerId(LowLevelTransaction llt)
    {
        var config = llt.Transaction.ReadTree(HnswGlobalConfigSlice);
        var read = config.DirectRead(VectorsContainerIdSlice);
        return new ContainerId(Unsafe.Read<long>(read));
    }

    private static ContainerId CreateHnswGlobalState(LowLevelTransaction llt)
    {
        llt.Transaction.CompactTreeFor(VectorsIdByHashSlice);
        var config = llt.Transaction.CreateTree(HnswGlobalConfigSlice);
        var read = config.DirectRead(VectorsContainerIdSlice);
        if (read is not null)
            return new ContainerId(Unsafe.Read<long>(read));

        ContainerId vectorsContainerId = Container.Create(llt);
        config.Add(VectorsContainerIdSlice, (long)vectorsContainerId);
        return vectorsContainerId;
    }

    public partial class SearchState : IDisposable
    {
        private readonly PriorityQueue<int, float> _candidatesQ = new();
        private readonly PriorityQueue<int, float> _nearestEdgesQ = new();
        private readonly Dictionary<long, int> _nodeIdToIdx = new();
        private NativeList<Node> _nodes = default;
        private NativeList<int> _newNodes = default;
        private readonly Tree _tree;
        private readonly Lookup<Int64LookupKey> _nodeIdToLocations;
        public readonly LowLevelTransaction Llt;
        // _visitsCounter versions Node.Visited; every traversal start bumps it so the visited
        // set is reset in O(1). _queryVectorVersion versions Node.QueryDistanceVersion; it is
        // bumped by OnQueryVector only when the query vector changes, so a node's cached distance
        // survives across traversals that reuse the same query vector. Both start at 1 so
        // freshly-loaded nodes (fields default to 0) never spuriously match.
        private int _visitsCounter = 1;
        private int _queryVectorVersion = 1;
        private Memory<byte> _lastQueryVector;
        public readonly delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, float> SimilarityCalc;
        public readonly bool IsEmpty;
        private readonly HnswIndexCache _nodeCache;
        // Fixed at construction. Lookups gate the shared cache on this so a reader at
        // storage tx R never observes an entry stamped by a later commit.
        private readonly long _readerTxId;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope? _queryNormalizationScope;

        public Span<Node> Nodes => _nodes.ToSpan();
        public Tree Tree => _tree;

        public int CreatedNodes => _newNodes.Count;

        private List<NativeList<Node>> _retiredNodeStores;

        // Grow the node array by copying into a fresh buffer and keeping the old one alive (released on
        // dispose) instead of freeing it, so a parallel-placement worker dereferencing a ref into the
        // array never observes a buffer freed by a grow mid-dispatch (RavenDB-26809).
        private void GrowNodesRetainingOldBuffer()
        {
            var grown = new NativeList<Node>();
            grown.Initialize(Llt.Allocator, Math.Max(InitialNodesCapacityWithCache, _nodes.Capacity * 2));
            grown.AddRangeUnsafe(_nodes.ToSpan());
            (_retiredNodeStores ??= []).Add(_nodes);
            // Publish the copied buffer before the new pointer becomes observable, so a concurrent
            // reader that sees the new pointer also sees the copied contents (weak-memory archs).
            Thread.MemoryBarrier();
            _nodes = grown;
        }

        // Force a grow of the node array backing store; returns true if the buffer moved. Test-only.
        internal bool ForceGrowNodesForTesting()
        {
            var before = (nint)_nodes.RawItems;
            GrowNodesRetainingOldBuffer();
            return (nint)_nodes.RawItems != before;
        }

        // Test-only: grow the node array, then claim a buffer of the vacated capacity and fill it with a
        // poison pattern. If the grow freed the old buffer (the RavenDB-26809 bug) the allocator reuses
        // that same memory and a dangling reader sees poison; if it was retained (the fix) the reader is
        // unaffected. The poison buffer is retained so it occupies the region past the reader's resume.
        internal bool GrowNodesAndPoisonVacatedForTesting()
        {
            var before = (nint)_nodes.RawItems;
            int vacatedCapacity = _nodes.Capacity;
            GrowNodesRetainingOldBuffer();

            var poison = new NativeList<Node>();
            poison.Initialize(Llt.Allocator, vacatedCapacity);
            poison.ToFullCapacitySpan().Fill(new Node { NodeId = -1, VectorId = -1 });
            (_retiredNodeStores ??= []).Add(poison);

            return (nint)_nodes.RawItems != before;
        }

        // Number of grown-from node buffers kept alive (un-disposed) until dispose.
        internal int RetiredNodeStorageCountForTesting => _retiredNodeStores?.Count ?? 0;

        public int GetCreatedNodeIndex(int index) => _newNodes[index];

        public Options Options;

        public SearchState(LowLevelTransaction llt, string name) : this(llt, SliceFromString(llt, name))
        {
        }

        private static Slice SliceFromString(LowLevelTransaction llt, string name)
        {
            Slice.From(llt.Allocator, name, out var slice);
            return slice;
        }
        
        public Lookup<Int64LookupKey> NodeIdsByVectorId => _tree.LookupFor<Int64LookupKey>(Hnsw.NodesByVectorIdSlice);

        public SearchState(LowLevelTransaction llt, Slice name) : this(llt, name, null)
        {
        }

        public SearchState(LowLevelTransaction llt, Slice name, ref Memory<byte> queryVector) : this(llt, name, null)
        {
            if (IsEmpty)
                return;
            if (TryNormalizeQueryVector(Options, Llt.Allocator, ref queryVector, out var scope))
                _queryNormalizationScope = scope;
        }

        public SearchState(LowLevelTransaction llt, Slice name, HnswIndexCache nodeCache)
        {
            Llt = llt;
            _nodeCache = nodeCache;
            _readerTxId = llt.Id;
            _tree = llt.Transaction.ReadTree(name);

            if (_tree is null || _tree.TryGetLookupFor(NodeIdToLocationSlice, out _nodeIdToLocations) == false)
            {
                IsEmpty = true;
                return;
            }

            // Options must come from disk: CountOfVectors (and the derived MaxLevel) advances
            // with every commit, while the cache snapshot is frozen at warm-up time. Using the
            // cached value would route the search from a level the entry point never reached.
            var optionsPtr = _tree.DirectRead(OptionsSlice);
            Options = Unsafe.Read<Options>(optionsPtr);
            SimilarityCalc = _nodeCache != null ? _nodeCache.SimilarityCalc : GetDistanceKernel(Options);

            if (_nodeCache != null)
            {
                // Skip ~7 NativeList<Node>.Grow doublings (and Dictionary rehashes) for the typical
                // per-query visit set when a shared cache is attached. ByteStringMemoryCache.Allocate
                // showed 36.5s of 165s wall in NativeList<Node>.Grow under load; pre-sizing kills it.
                _nodes.Initialize(llt.Allocator, InitialNodesCapacityWithCache);
                _nodeIdToIdx.EnsureCapacity(InitialNodesCapacityWithCache);
            }

            // Lazy lookup only: LoadNodeIndexes / GetNodeIndexById probe the shared cache
            // on demand. Eagerly materializing the entire cache into a per-query SearchState
            // dominated query wall (~49% in dotnet-trace) because the prepop work isn't
            // amortized — each Corax query builds its own IndexSearcher and SearchState.
            // Call PrepopulateFromNodeCache() explicitly for long-lived/batch SearchStates only.
        }

        private const int InitialNodesCapacityWithCache = 256;

        public float MinimumSimilarityToDistance(float minimumSimilarity)
        {
            switch (Options.SimilarityMethod)
            {
                case SimilarityMethod.CosineSimilaritySingles:
                case SimilarityMethod.CosineSimilarityI8:
                    return 2f * (1.0f - minimumSimilarity);
                case SimilarityMethod.HammingDistance:
                    return Options.VectorSizeBytes * 8 * (1f - minimumSimilarity); // number_of_bits * minimum_similarity
                default:
                    throw new InvalidDataException($"Unknown similarity method {Options.SimilarityMethod}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float DistanceToScore(float score)
        {
            switch (Options.SimilarityMethod)
            {
                case SimilarityMethod.CosineSimilaritySingles:
                case SimilarityMethod.CosineSimilarityI8:
                    return 1 - score;
                case SimilarityMethod.HammingDistance:
                    return ((Options.VectorSizeBytes * 8) - score) / (8f * Options.VectorSizeBytes); // number_of_bits * minimum_similarity
                default:
                    throw new InvalidDataException($"Unknown similarity method {Options.SimilarityMethod}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DistancesToScores(Span<float> distances)
        {
            switch (Options.SimilarityMethod)
            {
                case SimilarityMethod.CosineSimilaritySingles:
                case SimilarityMethod.CosineSimilarityI8:
                    DistanceToScoreCosine(distances);
                    break;
                case SimilarityMethod.HammingDistance:
                    DistanceToScoreHamming(distances, Options.VectorSizeBytes);
                    break;
                default:
                    throw new InvalidDataException($"Unknown similarity method {Options.SimilarityMethod}");
            }
        }

        public void FlushOptions()
        {
            using (_tree.DirectAdd(OptionsSlice, sizeof(Options), out var dst))
            {
                Unsafe.Write(dst, Options);
            }
        }

        public int RegisterVectorNode(long newNodeId, long vectorId)
        {
            int nodeIndex = AllocateNodeIndex(newNodeId);

            _newNodes.Add(Llt.Allocator, nodeIndex);
            _nodes[nodeIndex].VectorId = vectorId;

            _nodeIdToIdx[newNodeId] = nodeIndex;
            return nodeIndex;
        }

        private int AllocateNodeIndex(long nodeId)
        {
            if (_nodes.HasCapacityFor(1) == false)
                GrowNodesRetainingOldBuffer();
            int nodeIndex = _nodes.Count;
            _nodes.AddUnsafe(new Node { NodeId = nodeId });
            return nodeIndex;
        }

        /// <summary>
        /// Materialize one local Node per cache entry up front so every dictionary probe in the
        /// search hot path takes the cached fast path. Vector spans are resolved through the
        /// shared LLT here, eliminating the per-candidate Container.Get walk; edge ids are
        /// pre-translated to local node indexes for each level. Suitable for batch and long-lived
        /// SearchStates only — per-query SearchStates would pay the prepop cost without
        /// amortizing it across queries.
        /// </summary>
        public void PrepopulateFromNodeCache()
        {
            if (_nodeCache is null)
                return;

            // Pass 1: allocate a local Node and copy cache pointers.
            _nodeCache.ForEachNode(_readerTxId, (nodeId, view) =>
            {
                int idx = AllocateNodeIndex(nodeId);
                CopyNodeFromCache(in view, ref _nodes[idx]);
                _nodeIdToIdx[nodeId] = idx;
            });

            // Pass 2: resolve vector spans so QueryDistance miss path skips Container.Get.
            var self = this;
            for (int i = 0; i < _nodes.Count; i++)
            {
                ref var node = ref _nodes[i];
                if (node.VectorLoaded)
                    continue;
                var span = NodeReader.ReadVector(node.VectorId, in self);
                node.SetVector(in Options, span);
            }

            // Pass 3: pre-translate edges per level into EdgesIndexesPerLevel so ResolveEdgeIndexes
            // takes its fast path on first visit.
            var translation = new NativeList<int>();
            for (int i = 0; i < _nodes.Count; i++)
            {
                ref var node = ref _nodes[i];
                int levels = node.GetLevelCount();
                node.EdgesIndexesPerLevel.SetCapacity(Llt.Allocator, levels);
                for (int lvl = 0; lvl < levels; lvl++)
                {
                    var edges = node.EdgesAtLevel(lvl);
                    translation.ResetAndEnsureCapacity(Llt.Allocator, edges.Length);
                    for (int e = 0; e < edges.Length; e++)
                    {
                        if (_nodeIdToIdx.TryGetValue(edges[e], out var idx) == false)
                        {
                            // Edge target not in cache: leave EdgesIndexesPerLevel[lvl] empty so
                            // ResolveEdgeIndexes falls through to the slow path on first visit.
                            translation.Clear();
                            break;
                        }
                        translation.AddUnsafe(idx);
                    }
                    if (translation.Count == edges.Length)
                        node.EdgesIndexesPerLevel[lvl].ResetAndCopyFrom(Llt.Allocator, translation.ToSpan());
                }
            }
            translation.Dispose(Llt.Allocator);
        }

        public bool TryGetLocationForNode(long nodeId, out long locationId) =>
            _nodeIdToLocations.TryGetValue(nodeId, out locationId);

        public void RegisterNodeLocation(long nodeId, long locationId) =>
            _nodeIdToLocations.Add(nodeId, locationId);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref Node GetNodeByIndex(int index)
        {
            ref var n = ref _nodes[index];
            Debug.Assert(n.NodeId is not 0, "n.NodeId is not 0");
            return ref n;
        }

        public void ReadNode(long nodeId, out NodeReader n)
        {
            if (TryGetLocationForNode(nodeId, out var nodeLocation) is false)
                throw new InvalidOperationException($"Unable to find node id {nodeId}");
            n = Node.Decode(Llt, nodeLocation);
        }

        /// <summary>
        /// Populate a local node from the shared HnswIndexCache. Edge slices are copied into
        /// local <see cref="NativeList{T}"/>s because NativeLists can't be shared across
        /// allocator boundaries. Vector span is left default — <see cref="Node.GetVectorUnmanagedSpan"/>
        /// lazily resolves it via <c>Llt</c>, which is consistent with the cache because the
        /// publication invariant guarantees the cache reflects state visible to this
        /// transaction's snapshot.
        /// </summary>
        private void CopyNodeFromCache(in HnswIndexCache.CachedNodeView cached, ref Node local)
        {
            local.NodeId = cached.Header->NodeId;
            local.PostingListId = cached.Header->PostingListId;
            local.VectorId = cached.Header->VectorId;
            // Per-query scratch (Visited, QueryDistance*) stays at default; vector span is
            // resolved on first use.

            // Bind the cache-resident topology into the Node so EdgesAtLevel / EdgeCountAtLevel
            // read directly from the cache buffer. EdgesPerLevel is left empty: indexing-side
            // code paths never traverse a Node loaded through this method.
            local.CachedHeader = cached.Header;
            local.CachedOffsets = cached.Offsets;
            local.CachedEdges = cached.Edges;
        }

        /// <summary>
        /// Resolves edge indexes for a node at the specified level, caching the result in
        /// <see cref="Node.EdgesIndexesPerLevel"/>. On cache hit the expensive
        /// <see cref="LoadNodeIndexes"/> dictionary lookups are skipped entirely.
        /// May grow the nodes array — callers must re-fetch any outstanding node refs afterward.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ResolveEdgeIndexes(int nodeIndex, int level, ref NativeList<long> nodeIds, ref NativeList<int> indexes)
        {
            ref var node = ref GetNodeByIndex(nodeIndex);

            // Fast path: the resolved indexes for this level are already cached on the node.
            // The slow path (miss) is in a NoInlining helper so only this copy is inlined into
            // callers inside the search loop.
            int edgeCount = node.EdgeCountAtLevel(level);
            if (node.EdgesIndexesPerLevel.Count > level &&
                node.EdgesIndexesPerLevel[level].Count == edgeCount)
            {
                indexes.ResetAndCopyFrom(Llt.Allocator, node.EdgesIndexesPerLevel[level].ToSpan());
                return;
            }

            ResolveEdgeIndexesSlow(nodeIndex, level, ref nodeIds, ref indexes);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void ResolveEdgeIndexesSlow(int nodeIndex, int level, ref NativeList<long> nodeIds, ref NativeList<int> indexes)
        {
            ref var node = ref GetNodeByIndex(nodeIndex);
            nodeIds.ResetAndCopyFrom(Llt.Allocator, node.EdgesAtLevel(level));
            LoadNodeIndexes(ref nodeIds, ref indexes);

            node = ref GetNodeByIndex(nodeIndex);
            node.EdgesIndexesPerLevel.SetCapacity(Llt.Allocator, level + 1);
            node.EdgesIndexesPerLevel[level].ResetAndCopyFrom(Llt.Allocator, indexes.ToSpan());
        }

        /// <summary>
        /// This accepts a list of node ids (mutable, we do destructive updates to it) and translate
        /// that to a list of the indexes in the nodes array. If needed, it will load the nodes
        /// from the disk in a batch oriented manner.
        /// </summary>
        private void LoadNodeIndexes(ref NativeList<long> nodeIds, ref NativeList<int> indexes)
        {
            indexes.ResetAndEnsureCapacity(Llt.Allocator, nodeIds.Count);
            for (int i = 0; i < nodeIds.Count; i++)
            {
                if (_nodeIdToIdx.TryGetValue(nodeIds[i], out var index))
                {
                    indexes.AddUnsafe(index);
                    nodeIds[i] = -1;
                    continue;
                }

                // Check the shared HnswIndexCache before going to disk
                if (_nodeCache != null && _nodeCache.TryGetNode(nodeIds[i], _readerTxId, out var cached))
                {
                    var localIdx = AllocateNodeIndex(nodeIds[i]);
                    CopyNodeFromCache(in cached, ref _nodes[localIdx]);
                    _nodeIdToIdx[nodeIds[i]] = localIdx;
                    indexes.AddUnsafe(localIdx);
                    nodeIds[i] = -1;
                    continue;
                }
            }

            if (indexes.Count == nodeIds.Count)
                return;

            var matches = indexes.Count;
            var keys = nodeIds.ToSpan();
            keys.Sort();
            keys = keys[matches..]; // discard all those we already found
            for (int i = 0; i < keys.Length; i++)
            {
                var nodeIdx = AllocateNodeIndex(keys[i]);
                _nodes[nodeIdx].NodeId = keys[i];
                _nodeIdToIdx[keys[i]] = nodeIdx;
                indexes.AddUnsafe(nodeIdx);
            }

            _nodeIdToLocations.GetFor(keys, keys, -1);

            var spans = Buffers.GetSpans(keys.Length);
            Container.GetAll(Llt, keys, spans, -1, Llt.PageLocator);
            for (int i = 0; i < keys.Length; i++)
            {
                var buf = spans[i].ToSpan();
                var reader = Node.Decode(Llt, buf);
                reader.LoadInto(ref _nodes[indexes[matches + i]]);
            }
        }

        public int GetNodeIndexById(long nodeId)
        {
            ref var nodeIdx = ref CollectionsMarshal.GetValueRefOrAddDefault(_nodeIdToIdx, nodeId, out var exists);
            if (exists)
                return nodeIdx;

            // The shared HnswIndexCache is optimistic — a stale entry would have to be tolerated
            // by every consumer, which the entry point cannot do: its cached LevelCount sets
            // maxLevel for the entire descent. Reading it from disk costs one extra container
            // fetch per query but keeps the search routed correctly when the graph has grown
            // past the warm-up snapshot.
            if (nodeId != EntryPointId && _nodeCache != null && _nodeCache.TryGetNode(nodeId, _readerTxId, out var cached))
            {
                nodeIdx = AllocateNodeIndex(nodeId);
                CopyNodeFromCache(in cached, ref GetNodeByIndex(nodeIdx));
                return nodeIdx;
            }

            if (TryGetLocationForNode(nodeId, out var nodeLocation) is false)
                throw new InvalidOperationException($"Unable to find node id {nodeId}");

            nodeIdx = AllocateNodeIndex(nodeId);
            var reader = Node.Decode(Llt, nodeLocation);
            ref var n = ref GetNodeByIndex(nodeIdx);
            reader.LoadInto(ref n);
            return nodeIdx;
        }

        public ref Node GetNodeById(long nodeId)
        {
            int idx = GetNodeIndexById(nodeId);
            return ref GetNodeByIndex(idx);
        }

        public float Distance(UnmanagedSpan src, UnmanagedSpan dst)
        {
            return SimilarityCalc(src.ToSpan(), dst.ToSpan());
        }

        public float Distance(ReadOnlySpan<byte> vector, int fromIdx, int toIdx)
        {
            if (vector.IsEmpty)
            {
                ref var from = ref GetNodeByIndex(fromIdx);
                vector = from.GetVector(this); // Note: we have to make a copy here since we cannot pass this as ref into a ref value
            }

            ref var to = ref GetNodeByIndex(toIdx);
            if (to.QueryDistanceVersion == _queryVectorVersion)
                return to.QueryDistanceValue;

            Span<byte> v2 = to.GetVector(this);
            var distance = SimilarityCalc(vector, v2);

            return distance;
        }

        // Allows storing cached distance to the queried vector. Should be used only in the querying part!
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float QueryDistance(ReadOnlySpan<byte> vector, int toIdx, ref long vectorReadCounter)
        {
            // Fast path: the distance for this (query vector, node) pair was already computed during
            // the current traversal and is still valid (version equals _visitsCounter). The miss path
            // — loading the stored vector and running SimilarityCalc — lives in a NoInlining helper so
            // its register footprint does not leak into the search loop.
            ref var to = ref GetNodeByIndex(toIdx);
            if (to.QueryDistanceVersion == _queryVectorVersion)
                return to.QueryDistanceValue;

            return QueryDistanceMiss(vector, ref to, ref vectorReadCounter);
        }

        // Same fast/slow split as the index-taking overload. Callers in the search inner loop
        // already hold a ref to the Node so this avoids the second GetNodeByIndex lookup.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float QueryDistance(ReadOnlySpan<byte> vector, ref Node to, ref long vectorReadCounter)
        {
            if (to.QueryDistanceVersion == _queryVectorVersion)
                return to.QueryDistanceValue;

            return QueryDistanceMiss(vector, ref to, ref vectorReadCounter);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private float QueryDistanceMiss(ReadOnlySpan<byte> vector, ref Node to, ref long vectorReadCounter)
        {
            Span<byte> v2 = to.GetVector(this);
            vectorReadCounter++;
            var distance = SimilarityCalc(vector, v2);
            to.QueryDistanceValue = distance;
            to.QueryDistanceVersion = _queryVectorVersion;
            return distance;
        }

        /// <summary>
        /// Records the query vector a searcher is about to use. Bumps <see cref="_queryVectorVersion"/>
        /// iff the vector differs from the previously recorded one (compared by Memory identity, i.e.
        /// same underlying buffer and range). A bump invalidates every cached QueryDistance; a no-op
        /// keeps them valid.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void OnQueryVector(Memory<byte> queryVector)
        {
            if (queryVector.Equals(_lastQueryVector))
                return;
            _lastQueryVector = queryVector;
            ++_queryVectorVersion;
        }

        public void ReadPostingList(ContainerEntryId rawPostingListId, ref ContextBoundNativeList<long> listBuffer, ref FastPForDecoder pforDecoder, out int postingListSize)
        {
            ReadPostingList(Llt, rawPostingListId, ref listBuffer, ref pforDecoder, out postingListSize);
        }

        public static void ReadPostingList(LowLevelTransaction llt, ContainerEntryId rawPostingListId, ref ContextBoundNativeList<long> listBuffer, ref FastPForDecoder pforDecoder, out int postingListSize)
        {
            Container.Get(llt, rawPostingListId, out var smallPostingList);
            var count = VariableSizeEncoding.Read<int>(smallPostingList.Address, out var offset);

            var requiredSize = Math.Max(256, 256 * (int)Math.Ceiling((count + listBuffer.Count) / 256f));
            listBuffer.EnsureCapacityFor(requiredSize);
            Debug.Assert(listBuffer.Capacity > 0 && listBuffer.Capacity % 256 == 0, "The buffer must be multiple of 256 for PForDecoder.Read");

            pforDecoder.Init(smallPostingList.Address + offset, smallPostingList.Length - offset);
            listBuffer.Count += pforDecoder.Read(listBuffer.RawItems + listBuffer.Count, listBuffer.Capacity - listBuffer.Count);
            postingListSize = smallPostingList.Length;
        }

        [Flags]
        public enum NearestEdgesFlags
        {
            None = 0,
            StartingPointAsEdge = 1 << 1,
            FilterNodesWithEmptyPostingLists = 1 << 2
        }
        
        public IHnswSearcher NearestSearch(ContextBoundNativeList<int> startingPointsIndexes, Memory<byte> vector,
            int level, int numberOfCandidates,
            ContextBoundNativeList<int> candidates,
            NearestEdgesFlags flags)
        {
            return new NearestSearcher(this, startingPointsIndexes, vector, level , numberOfCandidates, candidates, flags);
        }
        
        public IHnswSearcher NearestSearch(int startingPointIndex, Memory<byte> vector,
            int level, int numberOfCandidates,
            ContextBoundNativeList<int> candidates,
            NearestEdgesFlags flags,
            bool hasFilterMatch)
        {
            return new NearestSearcher(this, startingPointIndex, vector, level , numberOfCandidates, candidates, flags, hasFilterMatch);
        }

        public IHnswSearcher EmptySearch() => new EmptySearcher();
        
        public IHnswSearcher ExactSearch(Memory<byte> vector, bool hasFilterMatch, int numberOfCandidates, ContextBoundNativeList<long>? nodesToScan) => new ExactSearcher(this, vector, hasFilterMatch, numberOfCandidates, nodesToScan);


        public void SearchFilteredNearest<TNodes>(ref ContextBoundNativeList<int> nearestIndexes, TNodes nodesFromFilter, int candidatesRequested, int maximumNumberOfNodesVisitedWithoutFindingBetterCandidate)
            where TNodes : IEnumerator<long>
        {
            var numberOfNodesVisitedFromLastNewCandidate = 0;
            PriorityQueue<int, int> nearestCandidates = new();
            
            // Retrieving nodes from the filter is costly (in terms of I/O). Therefore, we limit the number of random nodes we will visit.
            var maximumAmountOfNodesToVisit = 512;
            
            while (nodesFromFilter.MoveNext() 
                   && numberOfNodesVisitedFromLastNewCandidate < maximumNumberOfNodesVisitedWithoutFindingBetterCandidate 
                   && maximumAmountOfNodesToVisit-- > 0)
            {
                var currentNodeIndex = nodesFromFilter.Current;
                var nodeId = GetNodeIndexById(currentNodeIndex);
                ref var entry = ref GetNodeByIndex(nodeId);
                var currentNodeMaximumLevel = entry.GetLevelCount();

                if (nearestCandidates.Count < candidatesRequested)
                {
                    // The queue is still empty. We will add anything that comes from the filter.
                    nearestCandidates.Enqueue(nodeId, currentNodeMaximumLevel);
                    numberOfNodesVisitedFromLastNewCandidate = 0;
                    continue;
                }
                
                // Our queue is full, which means we will only update the queue if the currently visited node is better (based on existence across levels)
                // than our worst found candidate.
                nearestCandidates.TryPeek(out _, out var worstCandidateMaximumLevel);
                if (currentNodeMaximumLevel > worstCandidateMaximumLevel)
                {
                    // Replace the current worst candidate with the newly visited node since it exists on a higher level
                    nearestCandidates.EnqueueDequeue(nodeId, currentNodeMaximumLevel);
                    numberOfNodesVisitedFromLastNewCandidate = 0;
                }
                else
                {
                    numberOfNodesVisitedFromLastNewCandidate++;
                }
            }
            
            nearestIndexes.EnsureCapacityFor((int)nearestCandidates.Count);
            while (nearestCandidates.TryDequeue(out var currentNodeIndex, out var _))
                nearestIndexes.AddUnsafe(currentNodeIndex);
            nearestIndexes.Inner.Reverse(); // we want to prioritize the nodes from the higher levels
            nodesFromFilter.Dispose(); // dispose the iterator to restore the state.
        }
        
        public void SearchNearestAcrossLevels(ReadOnlySpan<byte> vector, int dstIdx, int maxLevel, ref ContextBoundNativeList<int> nearestIndexes)
        {
            var visitCounter = ++_visitsCounter;
            var currentNodeIndex = GetNodeIndexById(EntryPointId);
            var level = maxLevel;
            ref var entry = ref GetNodeByIndex(currentNodeIndex);
            entry.EdgesPerLevel.SetCapacity(Llt.Allocator, maxLevel + 1);
            var distance = Distance(vector, dstIdx, currentNodeIndex);
            var indexes = new NativeList<int>();
            var nodeIds = new NativeList<long>();

            while (level >= 0)
            {
                bool moved;
                do
                {
                    moved = false;
                    Debug.Assert(GetNodeByIndex(currentNodeIndex).GetLevelCount() > level, "n.GetLevelCount() > level");
                    ResolveEdgeIndexes(currentNodeIndex, level, ref nodeIds, ref indexes);
                    for (var i = 0; i < indexes.Count; i++)
                    {
                        var edgeIdx = indexes[i];
                        ref var edge = ref GetNodeByIndex(edgeIdx);
                        if (edge.Visited == visitCounter)
                            continue; // already checked it
                        edge.Visited = visitCounter;
                        var curDist = Distance(vector, dstIdx, edgeIdx);
                        if (curDist >= distance || double.IsNaN(curDist))
                            continue;

                        moved = true;
                        distance = curDist;
                        currentNodeIndex = edgeIdx;
                    }
                } while (moved);

                nearestIndexes.AddUnsafe(currentNodeIndex);
                level--;
            }

            indexes.Dispose(Llt.Allocator);
            nodeIds.Dispose(Llt.Allocator);
            nearestIndexes.Inner.Reverse();
        }


        private static class Buffers
        {
            [ThreadStatic]
            private static long[] IdsToLoad;
            [ThreadStatic]
            private static int[] IndexesOfIds;
            [ThreadStatic]
            private static long[] VectorIdsToLoad;
            [ThreadStatic]
            private static UnmanagedSpan[] SpansToLoad;
            [ThreadStatic]
            private static int[] NodeIndexes;

            public static Span<UnmanagedSpan> GetSpans(int count)
            {
                if (IdsToLoad is null || count > IdsToLoad.Length)
                {
                    Allocate(count);
                }

                return SpansToLoad;
            }

            public static void Get(int count, out Span<long> idsToLoad, out Span<long> vectorIdsToLoad, out Span<UnmanagedSpan> spansToLoad, out Span<int> nodeIndexes, out Span<int> indexesOfIds)
            {
                if (IdsToLoad is null || count > IdsToLoad.Length)
                {
                    Allocate(count);
                }

                idsToLoad = IdsToLoad;
                vectorIdsToLoad = VectorIdsToLoad;
                spansToLoad = SpansToLoad;
                nodeIndexes = NodeIndexes;
                indexesOfIds = IndexesOfIds;
            }

            private static void Allocate(int count)
            {
                count = Bits.NextAllocationSize(count);

                IndexesOfIds = new int[count];
                IdsToLoad = new long[count];
                VectorIdsToLoad = new long[count];
                SpansToLoad = new UnmanagedSpan[count];
                NodeIndexes = new int[count];
            }
        }

        /// <summary>
       /// Load all the _vectors_ associated with the provided node ids.
       /// Note that this actually requires us to first look up the nodes by id (loading them via batch)
       /// then load any yet unloaded vector (again using a batch) 
       /// </summary>
       public void PreloadNodesVectors(Span<long> nodeIds)
       {
           Buffers.Get(nodeIds.Length, 
               out var idsToLoad, 
               out var vectorIdsToLoad, 
               out var spans, 
               out var nodeIndexes,
               out var indexesOfIds);
           int idsToLoadIdx = 0;
           var vectorsToLoadIdx = 0;
           foreach (var nodeId in nodeIds)
           {
               if (_nodeIdToIdx.TryGetValue(nodeId, out var existingIdx))
               {
                   ref var n = ref _nodes[existingIdx];
                   if (n.VectorLoaded)
                       continue;
                   nodeIndexes[vectorsToLoadIdx] = existingIdx;
                   vectorIdsToLoad[vectorsToLoadIdx++] = n.GetVectorContainerId();
                   continue;
               }

               var nodeIdx = AllocateNodeIndex(nodeId);
               _nodes[nodeIdx].NodeId = nodeId;
               _nodeIdToIdx[nodeId] = nodeIdx;
               indexesOfIds[idsToLoadIdx] = nodeIdx;
               idsToLoad[idsToLoadIdx++] = nodeId;
           }

           if (idsToLoadIdx is not 0)
           {
               idsToLoad = idsToLoad[..idsToLoadIdx];
               indexesOfIds = indexesOfIds[..idsToLoadIdx];
           
               _nodeIdToLocations.GetFor(idsToLoad, idsToLoad, -1);
               idsToLoad.Sort(indexesOfIds);
               Container.GetAll(Llt, idsToLoad, spans, -1, Llt.PageLocator);
               for (int i = 0; i < indexesOfIds.Length; i++)
               {
                   var buf = spans[i].ToSpan();
                   var reader = Node.Decode(Llt, buf);
                   var nodeIndex = indexesOfIds[i];
                   ref var n = ref _nodes[nodeIndex];
                   reader.LoadInto(ref n);
                   nodeIndexes[vectorsToLoadIdx] = nodeIndex;
                   vectorIdsToLoad[vectorsToLoadIdx++] = n.GetVectorContainerId();
               }
           }
           if (vectorsToLoadIdx is 0)
               return;

           vectorIdsToLoad = vectorIdsToLoad[..vectorsToLoadIdx];
           nodeIndexes = nodeIndexes[..vectorsToLoadIdx];
           vectorIdsToLoad.Sort(nodeIndexes);
           Container.GetAll(Llt, vectorIdsToLoad, spans, -1, Llt.PageLocator);
           for (int i = 0; i < vectorIdsToLoad.Length; i++)
           {
               // note, small vectors (where multiple can fit in a single page), will be 
               // partition inside the SetVector if needed
               _nodes[nodeIndexes[i]].SetVector(Options, spans[i]);
           }
       }

       public bool TryGetNodeById(long nodeId, out int nodeIndex)
       {
           return _nodeIdToIdx.TryGetValue(nodeId, out nodeIndex);
       }

       /// <summary>
       /// Debug-only check that the candidate and nearest-edges priority queues owned by this
       /// SearchState are empty. A searcher that reuses a shared SearchState must leave these
       /// queues empty on Dispose; this method catches a searcher that fails to do so before the
       /// next one starts.
       /// </summary>
       [Conditional("DEBUG")]
       public void AssertSharedQueuesClean()
       {
           Debug.Assert(_candidatesQ.Count == 0, "_candidatesQ must be empty between sub-queries on a shared SearchState");
           Debug.Assert(_nearestEdgesQ.Count == 0, "_nearestEdgesQ must be empty between sub-queries on a shared SearchState");
       }

       public void Dispose()
       {
           _candidatesQ.Clear();
           _nearestEdgesQ.Clear();
           _newNodes.Dispose(Llt.Allocator);

           _nodes.Dispose(Llt.Allocator);

           if (_retiredNodeStores is not null)
           {
               foreach (var store in _retiredNodeStores)
               {
                   var retired = store;
                   retired.Dispose(Llt.Allocator);
               }
           }

           if (_queryNormalizationScope is { } scope)
           {
               scope.Dispose();
               _queryNormalizationScope = null;
           }
       }

    }

    public partial class Registration : IDisposable
    {
        public bool IsCommited { get; private set; }
        private readonly Dictionary<ByteString, (ByteString Key, int NodeIndex, NativeList<long> PostingList)> _vectorHashCache = new(ByteStringContentComparer.Instance);

        /// <summary>
        /// Node ids whose persisted record was written or rewritten during this commit. The
        /// post-commit cache update (HnswIndexCache.ApplyCommit) consumes this set to bring the
        /// in-memory cache forward without re-running BFS from the entry point. Populated by
        /// the persistence loop in Commit.
        /// </summary>
        public HashSet<long> DirtyNodeIds { get; } = new();
        private readonly Lookup<Int64LookupKey> _nodesByVectorId;
        private SearchState _searchState;
        public Random Random;
        private readonly CompactTree _vectorsByHash;
        private readonly int _vectorBatchSizeInPages;
        private readonly ContainerId _globalVectorsContainerId;
        private PostingList _largePostingListSet;

        // Lazy scratch used by Register to normalize raw-sized inputs in-place; one allocation per Registration.
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _normalizationScope;
        private ByteString _normalizationBuffer;

        public int AmountOfModifiedVectorsInTransaction => _vectorHashCache.Count;

        public Options Options => _searchState.Options;

        internal TestingStuff _forTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly() => _forTestingPurposes ??= new TestingStuff();

        // Deterministically reproduces the RavenDB-26809 interleaving for tests: parks a placement worker
        // while it holds a reference into a node's edges, then has the LLT thread move both that edge
        // buffer and the node array before the worker resumes. With the fix in place the worker reads a
        // private edge snapshot and the grown-from node buffer is retained, so the build completes and the
        // graph stays queryable; reverting either guard turns this into a use-after-free.
        internal sealed class TestingStuff
        {
            internal bool SimulateConcurrentRealloc;

            // Latched once the matching buffer was actually moved while the worker was parked, so a test
            // can fail loudly if the race window was never exercised.
            internal volatile bool VictimSelected;
            internal volatile bool InnerEdgeBufferMovedWhileWorkerParked;
            internal volatile bool NodesArrayMovedWhileWorkerParked;

            // False if a node reference captured before the move read poisoned/freed data after it — i.e.
            // the node array was not retained across the grow (the RavenDB-26809 bug).
            internal volatile bool StaleNodeReferenceStillValid = true;

            // Set by the runner so the parked worker can wake the dispatch loop; without it the loop may
            // not iterate again when the only outstanding work is the parked worker itself.
            internal Action WakeLltLoop;

            private int _victim = -1;
            private int _victimLevel;
            private nint _victimEdgeBufferBeforeMove;
            private nint _victimNodePtr;
            private long _victimNodeIdExpected;
            private readonly ManualResetEventSlim _workerParked = new(false);
            private readonly ManualResetEventSlim _lltMoved = new(false);

            // Worker thread: the first worker about to consume a node's edges at a level with real storage
            // parks here until the LLT thread has moved that storage and the node array.
            internal void OnWorkerCapturedEdgeListRef(SearchState searchState, int nodeIndex, int level)
            {
                if (SimulateConcurrentRealloc == false || Volatile.Read(ref _victim) >= 0)
                    return;

                ref var candidate = ref searchState.GetNodeByIndex(nodeIndex);
                if (candidate.EdgesIndexesPerLevel.Count <= level || candidate.EdgesIndexesPerLevel[level].Count == 0)
                    return;
                if (Interlocked.CompareExchange(ref _victim, nodeIndex, -1) != -1)
                    return;

                _victimLevel = level;
                _victimEdgeBufferBeforeMove = (nint)candidate.EdgesIndexesPerLevel[level].RawItems;
                _victimNodePtr = (nint)Unsafe.AsPointer(ref candidate);
                _victimNodeIdExpected = candidate.NodeId;
                VictimSelected = true;
                _workerParked.Set();
                WakeLltLoop?.Invoke();
                _lltMoved.Wait(TimeSpan.FromMinutes(2));

                // Read through the reference captured before the move, exactly as a racing worker would.
                // Retaining the grown-from buffer keeps this valid; freeing it (the bug) reads poison.
                if (((Node*)_victimNodePtr)->NodeId != _victimNodeIdExpected)
                    StaleNodeReferenceStillValid = false;
            }

            // LLT thread, at the top of each dispatch round: once the victim worker has parked, move its
            // edge buffer (restoring contents) and the node array, then release the worker.
            internal void OnLltRunRound(SearchState searchState)
            {
                if (SimulateConcurrentRealloc == false || _lltMoved.IsSet || _workerParked.IsSet == false)
                    return;

                ref var inner = ref searchState.GetNodeByIndex(_victim).EdgesIndexesPerLevel[_victimLevel];
                var saved = inner.ToSpan().ToArray();
                inner.ResetAndEnsureCapacity(searchState.Llt.Allocator, Math.Max(inner.Capacity * 2, saved.Length + 1));
                foreach (var v in saved)
                    inner.AddUnsafe(v);
                if ((nint)inner.RawItems != _victimEdgeBufferBeforeMove)
                    InnerEdgeBufferMovedWhileWorkerParked = true;

                if (searchState.GrowNodesAndPoisonVacatedForTesting())
                    NodesArrayMovedWhileWorkerParked = true;

                _lltMoved.Set();
            }
        }

        public Registration(LowLevelTransaction llt, Slice name, Random random = null)
        {
            Random = random ?? Random.Shared;
            _searchState = new SearchState(llt, name);
            _vectorBatchSizeInPages = _searchState.Options.VectorBatchInPages;
            _globalVectorsContainerId = ReadGlobalVectorsContainerId(llt);
            _nodesByVectorId = _searchState.Tree.LookupFor<Int64LookupKey>(NodesByVectorIdSlice);
            _vectorsByHash = llt.Transaction.CompactTreeFor(VectorsIdByHashSlice);
        }

        /// <summary>
        /// Removes a vector from the graph.
        /// </summary>
        /// <param name="entryId">The ID of the document.</param>
        /// <param name="vectorHash">The hash of the vector to remove.</param>
        public void Remove(long entryId, ReadOnlySpan<byte> vectorHash)
        {
            entryId = EntryIdToInternalEntryId(entryId);
            const long RemovalMask = 1;

            PortableExceptions.ThrowIfOnDebug<ArgumentOutOfRangeException>((entryId & Constants.Graphs.VectorId.EnsureIsSingleMask) != 0, "Entry ids must have the first two bits cleared, we are using those");

            _searchState.Llt.Allocator.AllocateDirect(Sodium.GenericHashSize, out var hashBuffer);
            vectorHash.CopyTo(hashBuffer.ToSpan());

            ref var postingList = ref CollectionsMarshal.GetValueRefOrAddDefault(_vectorHashCache, hashBuffer, out var exists);
            if (exists)
            {
                ref var l = ref postingList.PostingList;
                l.Add(_searchState.Llt.Allocator, entryId | RemovalMask);
                _searchState.Llt.Allocator.Release(ref hashBuffer);
                return;
            }

            if (_vectorsByHash.TryGetValue(vectorHash, out var vectorId) is false)
                PortableExceptions.Throw<InvalidOperationException>($"Unable to find the vector corresponding to the provided vector hash: base64({Convert.ToBase64String(vectorHash)}).");

            if (_nodesByVectorId.TryGetValue(vectorId, out var nodeId) is false)
                PortableExceptions.Throw<InvalidOperationException>($"Unable to find the node corresponding to the provided vector hash: base64({Convert.ToBase64String(vectorHash)}) and VectorId({vectorId}).");

            int nodeIndex = _searchState.GetNodeIndexById(nodeId);
            postingList = (hashBuffer, nodeIndex, NativeList<long>.Create(_searchState.Llt.Allocator, entryId | RemovalMask));
        }

        /// <summary>
        /// The two lowest bits must be cleared for mask purposes.
        /// </summary>
        /// <param name="entryId">Original entryId.</param>
        /// <returns>Internal Hnsw entryId</returns>
        internal static long EntryIdToInternalEntryId(long entryId)
        {
            Debug.Assert(entryId > 0 && (~(long.MaxValue >> 2) & entryId) == 0, "entryId > 0 && (~(long.MaxValue >> 2) & entryId) == 0");
            return entryId << 2;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long InternalEntryIdToEntryId(long entryId) => entryId >> 2;

        /// <summary>
        /// During indexing, we're shifting each ID 2 bits to the left to use the two lowest bits as a mask placeholder. This is for querying decoding.
        /// </summary>
        /// <param name="entries">Array of internal ids.</param>
        internal static void InternalEntryIdToEntryId(Span<long> entries)
        {
            var entriesPos = 0;
            ref var entriesRef = ref MemoryMarshal.GetReference(entries);

            if (AdvInstructionSet.IsAcceleratedVector512)
            {
                var N = Vector512<long>.Count;

                for (; entriesPos + N < entries.Length; entriesPos += N)
                {
                    ref var currentMemory = ref Unsafe.Add(ref entriesRef, entriesPos);
                    var current = Vector512.LoadUnsafe(ref currentMemory);
                    Vector512.ShiftRightLogical(current, 2).StoreUnsafe(ref currentMemory);
                }
            }

            if (AdvInstructionSet.IsAcceleratedVector256)
            {
                var N = Vector256<long>.Count;

                for (; entriesPos + N < entries.Length; entriesPos += N)
                {
                    ref var currentMemory = ref Unsafe.Add(ref entriesRef, entriesPos);
                    var current = Vector256.LoadUnsafe(ref currentMemory);
                    Vector256.ShiftRightLogical(current, 2).StoreUnsafe(ref currentMemory);
                }
            }

            for (; entriesPos < entries.Length; entriesPos++)
                Unsafe.Add(ref entriesRef, entriesPos) >>= 2;
        }

        /// <summary>
        /// Adds a vector to the graph.
        /// </summary>
        /// <param name="entryId">The ID of the document (source).</param>
        /// <param name="vector">The vector's data.</param>
        /// <returns>The CompactKey address of the hash calculated from the vector, which will be required for removal.</returns>
        public ByteString Register(long entryId, ReadOnlySpan<byte> vector)
        {
            entryId = EntryIdToInternalEntryId(entryId);
            PortableExceptions.ThrowIfOnDebug<ArgumentOutOfRangeException>((entryId & Constants.Graphs.VectorId.EnsureIsSingleMask) != 0, "Entry ids must have the first two bits cleared, we are using those");

            if (vector.Length != _searchState.Options.VectorSizeBytes)
                vector = NormalizeIntoScratch(vector);

            var hashBuffer = ComputeHashFor(vector);
            ref (ByteString Hash, int NodeIndex, NativeList<long> PostingList) postingList = ref CollectionsMarshal.GetValueRefOrAddDefault(_vectorHashCache, hashBuffer, out var exists);
            if (exists)
            {
                // already added this in the current batch
                ref var l = ref postingList.PostingList;
                l.Add(_searchState.Llt.Allocator, entryId);
                _searchState.Llt.Allocator.Release(ref hashBuffer);
                return postingList.Hash;
            }

            var vectorHash = hashBuffer.ToReadOnlySpan();
            long vectorId;
            if (_vectorsByHash.TryGetValue(vectorHash, out _, out vectorId) is false)
            {
                var vectorEntryId = RegisterVector(vector);
                vectorId = (long)vectorEntryId;
                _vectorsByHash.Add(vectorHash, vectorId);
            }

            if (_nodesByVectorId.TryGetValue(vectorId, out var nodeId))
            {
                int nodeIndex = _searchState.GetNodeIndexById(nodeId);
                postingList = (hashBuffer, nodeIndex, NativeList<long>.Create(_searchState.Llt.Allocator, entryId));
                return hashBuffer;
            }

            long newNodeId = ++_searchState.Options.CountOfVectors;
            int nodeIdx = _searchState.RegisterVectorNode(newNodeId, vectorId);
            _nodesByVectorId.Add(vectorId, newNodeId);

            postingList = (hashBuffer, nodeIdx, ToPostingListTuple(entryId));
            return hashBuffer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        NativeList<long> ToPostingListTuple(long entryId)
        {
            var list = new NativeList<long>();
            list.Add(_searchState.Llt.Allocator, entryId);
            return list;
        }
        
        private ContainerEntryId RegisterVector(ReadOnlySpan<byte> vector)
        {
            if (_searchState.Options.LastUsedContainerId.IsEmpty)
            {
                if (_vectorBatchSizeInPages is 1)
                {
                    // here we allocate a small value, directly from the container
                    var vectorId = Container.Allocate(_searchState.Llt, _globalVectorsContainerId,
                        vector.Length, out var singleVectorStorage);

                    vector.CopyTo(singleVectorStorage);
                    return vectorId;
                }

                var sizeInBytes = _vectorBatchSizeInPages * Constants.Storage.PageSize - PageHeader.SizeOf;
                var batchId = Container.Allocate(_searchState.Llt, _globalVectorsContainerId,
                    sizeInBytes, out var vectorStorage);

                Debug.Assert(vectorStorage.Length / _searchState.Options.VectorSizeBytes <= byte.MaxValue, "vectorStorage.Length / _searchState.Options.VectorSizeBytes <= byte.MaxValue");
                Debug.Assert(((long)batchId & 0xFFF) == 0, "We allocate > 1 page, so we get the full page container id");
                _searchState.Options.LastUsedContainerId = new ContainerId((long)batchId);
                _searchState.Options.VectorBatchIndex = 1;
                vector.CopyTo(vectorStorage);
                //container id | index    | marker
                return GetVectorId(new ContainerId((long)batchId), 0);
            }
            var span = Container.GetMutable(_searchState.Llt, new ContainerEntryId((long)_searchState.Options.LastUsedContainerId));
            var count = _searchState.Options.VectorBatchIndex++;
            Debug.Assert(((count) * vector.Length) < span.Length, "((count) * vector.Length) < span.Length");
            var offset = count * vector.Length;
            vector.CopyTo(span[offset..]);
            offset += vector.Length;
            var id = GetVectorId(_searchState.Options.LastUsedContainerId, count);
            if (offset + vector.Length > span.Length)
            {
                // no more room for the _next_ vector
                _searchState.Options.LastUsedContainerId = new ContainerId(0);
                _searchState.Options.VectorBatchIndex = 0;
            }

            return id;

            ContainerEntryId GetVectorId(ContainerId containerId, int index)
            {
                Debug.Assert(((long)containerId & Constants.Graphs.VectorId.EnsureIsSingleMask) == 0, $"Container id {containerId}");
                //container id | index    | marker
                return new ContainerEntryId((long)containerId | (uint)(index << 1) | Constants.Graphs.VectorStorage.VectorContainerInternalIndexer);
            }
        }

        private ReadOnlySpan<byte> NormalizeIntoScratch(ReadOnlySpan<byte> vector)
        {
            var expected = _searchState.Options.VectorSizeBytes;
            var floats = MemoryMarshal.Cast<byte, float>(vector);
            PortableExceptions.ThrowIf<ArgumentOutOfRangeException>(
                VectorNormalizationRequired(_searchState.Options) == false || TensorSizeBytes<float>(floats.Length) != expected,
                $"Vector size {vector.Length} does not match expected size: {expected}");

            if (_normalizationBuffer.HasValue == false)
                _normalizationScope = _searchState.Llt.Allocator.Allocate(expected, out _normalizationBuffer);

            var dst = _normalizationBuffer.ToSpan();
            WriteNormalizedTensor(floats, dst);
            return dst;
        }

        private ByteString ComputeHashFor(ReadOnlySpan<byte> vector)
        {
            _searchState.Llt.Allocator.AllocateDirect(Sodium.GenericHashSize, out var hashBuffer);
            Sodium.GenericHash(vector, hashBuffer.ToSpan());
            return hashBuffer;
        }

        public void Commit(CancellationToken token)
        {
            PortableExceptions.ThrowIfOnDebug<InvalidOperationException>(_searchState.Llt.Committed);

            var pforEncoder = new FastPForEncoder(_searchState.Llt.Allocator);
            var pforDecoder = new FastPForDecoder(_searchState.Llt.Allocator);
            var listBuffer = new ContextBoundNativeList<long>(_searchState.Llt.Allocator);
            var byteBuffer = new ContextBoundNativeList<byte>(_searchState.Llt.Allocator);
            byteBuffer.EnsureCapacityFor(128);

            var nodes = _searchState.Nodes;
            foreach (var (_, (_, nodeIndex, modifications)) in _vectorHashCache)
            {
                ref var node = ref nodes[nodeIndex];
                node.PostingListId = MergePostingList(node.PostingListId, modifications);
            }

            // Intentionally zeroing the nodes var, we may realloc the underlying array in the insert vector phase
            nodes = Span<Node>.Empty;
            _ = nodes;

            InsertVectorsToGraph(ref byteBuffer, token);

            nodes = _searchState.Nodes;
            for (int i = 0; i < nodes.Length; i++)
            {
                PersistNode(ref nodes[i], ref byteBuffer);
                DirtyNodeIds.Add(nodes[i].NodeId);
            }

            // flush the local modifications
            _searchState.FlushOptions();

            listBuffer.Dispose();
            byteBuffer.Dispose();
            pforEncoder.Dispose();
            pforDecoder.Dispose();

            IsCommited = true;

            long MergePostingList(long postingList, NativeList<long> modifications)
            {
                // We may have duplicates in the list.
                // Scenarios:
                // 1) additions: when the source document contains a list of the same vectors
                // 2) removals: when the deleted document contained a list of the same vectors
                // However, the HNSW does not use frequency. So we want to have information in buffer about:
                // - There was an addition
                // - There was a removal
                // In such case, we can just sort and remove duplicates to have unique list.
                modifications.Shrink(Sorting.SortAndRemoveDuplicates(modifications.ToSpan()));

                listBuffer.Clear();
                listBuffer.AddRange(modifications.ToSpan());

                int currentSize = 0;
                bool hasSmallPostingList = false;
                ContainerEntryId rawPostingListId = new ContainerEntryId(postingList & Constants.Graphs.VectorId.ContainerType);

                switch (postingList & Constants.Graphs.VectorId.EnsureIsSingleMask)
                {
                    case Constants.Graphs.VectorId.Tombstone: // nothing there
                        break;
                    case Constants.Graphs.VectorId.Single: // single value, just add it
                        listBuffer.Add((long)rawPostingListId);
                        break;
                    case Constants.Graphs.VectorId.SmallPostingList:
                        hasSmallPostingList = true;
                        _searchState.ReadPostingList(rawPostingListId, ref listBuffer, ref pforDecoder, out currentSize);
                        break;
                    case Constants.Graphs.VectorId.PostingList:
                        return UpdatePostingList(rawPostingListId, in modifications, pforEncoder, ref pforDecoder, ref listBuffer);
                }

                // Due to deduplication performed before reading the posting list from disk, we can now have the following scenarios:
                // 1) 2x Additions + 1x Removal
                // There was an update of the document. So, we have 1x addition and 1x removal from indexing, plus the loaded entry id from disk -> the ID will remain in the buffer
                // 2) 1x Addition + 1x Removal:
                // There was a delete operation during indexing, plus the loaded entry id from disk: id will be removed from buffer
                // 3) 1x Addition: New document 
                // INFO: All other scenarios are invalid.
                PostingList.SortModificationsAndRemoveDuplicates(ref listBuffer);

                if (listBuffer.Count is 0 or 1)
                {
                    if (hasSmallPostingList)
                    {
                        Container.Delete(_searchState.Llt, _searchState.Options.Container, rawPostingListId);
                    }

                    if (listBuffer.Count is 0)
                        return 0;

                    Debug.Assert((listBuffer[0] & Constants.Graphs.VectorId.PostingList) == 0, "(listBuffer[0] & 0b11) == 0");
                    return listBuffer[0] | Constants.Graphs.VectorId.Single;
                }

                int size = pforEncoder.Encode(listBuffer.RawItems, listBuffer.Count);
                if (size > Container.MaxSizeInsideContainerPage)
                {
                    DeleteOldSmallPostingListIfNeeded();
                    return CreateNewPostingList(pforEncoder);
                }

                byteBuffer.EnsureCapacityFor(size + 5);
                var offset = VariableSizeEncoding.Write(byteBuffer.RawItems, listBuffer.Count);
                (int itemsCount, int sizeUsed) = pforEncoder.Write(byteBuffer.RawItems + offset, byteBuffer.Capacity - offset);
                byteBuffer.Count = sizeUsed + offset;
                Debug.Assert(itemsCount == listBuffer.Count && sizeUsed == size, "itemsCount == listBuffer.Count && sizeUsed == size");
                Span<byte> mutable;
                if (currentSize == byteBuffer.Count)
                {
                    mutable = Container.GetMutable(_searchState.Llt, rawPostingListId);
                }
                else
                {
                    DeleteOldSmallPostingListIfNeeded();
                    rawPostingListId = Container.Allocate(_searchState.Llt, _searchState.Options.Container, byteBuffer.Count, out mutable);
                }

                Span<byte> span = byteBuffer.ToSpan();
                span.CopyTo(mutable);

                Debug.Assert(((long)rawPostingListId & Constants.Graphs.VectorId.PostingList) == 0, "(rawPostingListId & 0b11) == 0");
                return (long)rawPostingListId | Constants.Graphs.VectorId.SmallPostingList;


                void DeleteOldSmallPostingListIfNeeded()
                {
                    if (hasSmallPostingList)
                    {
                        Container.Delete(_searchState.Llt, _searchState.Options.Container, rawPostingListId);
                    }
                }
            }
        }

        public void Dispose()
        {
            if (_normalizationBuffer.HasValue)
                _normalizationScope.Dispose();
        }

        private long UpdatePostingList(ContainerEntryId postingListId, in NativeList<long> modifications, FastPForEncoder pForEncoder, ref FastPForDecoder pForDecoder, ref ContextBoundNativeList<long> tempListBuffer)
        {
            var setSpace = Container.GetMutable(_searchState.Llt, postingListId);
            ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace);

            var lists = stackalloc long*[2];
            var indexes = stackalloc int[2];

            using var _1 = _searchState.Llt.Allocator.Allocate(modifications.Count * sizeof(long), out var bs1);
            using var _2 = _searchState.Llt.Allocator.Allocate(modifications.Count * sizeof(long), out var bs2);
            lists[0] = (long*)bs1.Ptr;
            lists[1] = (long*)bs2.Ptr;

            for (int i = 0; i < modifications.Count; i++)
            {
                var cur = modifications[i];
                var listIdx = cur & 1;
                var curIndex = indexes[listIdx]++;
                lists[listIdx][curIndex] = cur;
            }

            var numberOfEntries = PostingList.Update(_searchState.Llt, ref postingListState, lists[0], indexes[0],
                lists[1], indexes[1], pForEncoder, ref tempListBuffer, ref pForDecoder);

            if (numberOfEntries is 0)
            {
                _largePostingListSet ??= _searchState.Llt.Transaction.OpenPostingList(Constants.PostingList.PostingListRegister);
                _largePostingListSet.Remove((long)postingListId);
                Container.Delete(_searchState.Llt, _searchState.Options.Container, postingListId);
                return 0;
            }

            return (long)postingListId | Constants.Graphs.VectorId.PostingList;
        }

        private long CreateNewPostingList(FastPForEncoder pforEncoder)
        {
            var setId = Container.Allocate(_searchState.Llt, _searchState.Options.Container, sizeof(PostingListState), out var setSpace);

            _largePostingListSet ??= _searchState.Llt.Transaction.OpenPostingList(Constants.PostingList.PostingListRegister);
            _largePostingListSet.Add((long)setId);
            
            ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace);
            PostingList.Create(_searchState.Llt, ref postingListState, pforEncoder);
            return (long)setId | Constants.Graphs.VectorId.PostingList;
        }


        void PersistNode(ref Node node, ref ContextBoundNativeList<byte> byteBuffer)
        {
            var encoded = node.Encode(ref byteBuffer);
            if (_searchState.TryGetLocationForNode(node.NodeId, out var locationId))
            {
                var existing = Container.GetMutable(_searchState.Llt, new ContainerEntryId(locationId));
                if (existing.Length == encoded.Length)
                {
                    encoded.CopyTo(existing);
                    return;
                }

                Container.Delete(_searchState.Llt, _searchState.Options.Container, new ContainerEntryId(locationId));
            }

            var allocatedId = Container.Allocate(_searchState.Llt, _searchState.Options.Container, encoded.Length, out var storage);
            locationId = (long)allocatedId;
            _searchState.RegisterNodeLocation(node.NodeId, locationId);
            encoded.CopyTo(storage);
        }
    }

    public static Registration RegistrationFor(LowLevelTransaction llt, string name, Random random = null)
    {
        Slice.From(llt.Allocator, name, out var slice);
        return RegistrationFor(llt, slice, random);
    }

    public static Registration RegistrationFor(LowLevelTransaction llt, Slice name, Random random = null)
    {
        return new Registration(llt, name, random);
    }
    
    public static VectorSearchRetriever ExactNearest(LowLevelTransaction llt, Slice name, int numberOfCandidates, Memory<byte> vector, float minimumSimilarity, bool hasFilterMatch, ContextBoundNativeList<long>? nodesToScan = null)
    {
        var searchState = new SearchState(llt, name, ref vector);
        var results = searchState.ExactSearch(vector, hasFilterMatch, numberOfCandidates, nodesToScan);
        return new VectorSearchRetriever(searchState,  results, vector, minimumSimilarity);
    }

    public static VectorSearchRetriever ApproximateFilteredNearest<TEnumerator>(LowLevelTransaction llt, Slice name, int numberOfCandidates, Memory<byte> vector, float minimumSimilarity, TEnumerator nodesToProbe)
     where TEnumerator : IEnumerator<long>
    {
        var searchState = new SearchState(llt, name, ref vector);
        if (searchState.Options.CountOfVectors == 0)
            return new VectorSearchRetriever(searchState, searchState.EmptySearch(), vector, minimumSimilarity);

        var startingPointsIndexes = new ContextBoundNativeList<int>(llt.Allocator);
        var candidates = new ContextBoundNativeList<int>(llt.Allocator);
        candidates.EnsureCapacityFor(searchState.Options.MaxLevel + 1);

        searchState.SearchFilteredNearest(ref startingPointsIndexes, nodesToProbe, numberOfCandidates, 16);
        candidates.Clear();
        var nearestEdgesSearch = searchState.NearestSearch(startingPointsIndexes, vector, 0, numberOfCandidates, candidates,
            SearchState.NearestEdgesFlags.StartingPointAsEdge | SearchState.NearestEdgesFlags.FilterNodesWithEmptyPostingLists);
        return new VectorSearchRetriever(searchState, nearestEdgesSearch, vector, minimumSimilarity);
    }
    
    public static VectorSearchRetriever ApproximateNearest(LowLevelTransaction llt, Slice name, int numberOfCandidates, Memory<byte> vector, float minimumSimilarity, bool hasFilterMatch = false)
    {
        var searchState = new SearchState(llt, name, ref vector);
        if (searchState.Options.CountOfVectors == 0)
            return new VectorSearchRetriever(searchState, searchState.EmptySearch(), vector, minimumSimilarity);

        var nearestNodesByLevel = new ContextBoundNativeList<int>(llt.Allocator);
        nearestNodesByLevel.EnsureCapacityFor(searchState.Options.MaxLevel + 1);

        searchState.SearchNearestAcrossLevels(vector.Span, -1, searchState.Options.MaxLevel, ref nearestNodesByLevel);
        var nearest = nearestNodesByLevel[0];
        nearestNodesByLevel.Clear();
        var nearestEdgesSearch = searchState.NearestSearch(nearest, vector, 0, numberOfCandidates, nearestNodesByLevel,
            SearchState.NearestEdgesFlags.StartingPointAsEdge | SearchState.NearestEdgesFlags.FilterNodesWithEmptyPostingLists, hasFilterMatch);
        return new VectorSearchRetriever(searchState, nearestEdgesSearch, vector, minimumSimilarity);
    }

    /// <summary>
    /// Approximate nearest neighbor search using a caller-owned <see cref="SearchState"/>. The
    /// caller keeps the SearchState alive across multiple queries and is responsible for
    /// disposing it; node data loaded by earlier queries remains in SearchState for later ones,
    /// so repeated traversals avoid the node-index resolution and container reads already paid
    /// for on the first pass.
    /// </summary>
    public static VectorSearchRetriever ApproximateNearest(SearchState searchState, int numberOfCandidates, Memory<byte> vector, float minimumSimilarity, bool hasFilterMatch = false)
    {
        TryNormalizeQueryVector(searchState.Options, searchState.Llt.Allocator, ref vector, out var queryVectorScope);
        // Bind the per-node QueryDistance cache to this query vector before SearchNearestAcrossLevels
        // reads it. Without this, a shared SearchState reuses cached distances from a prior sub-query
        // and the upper-level descent latches onto the wrong starting point.
        searchState.OnQueryVector(vector);

        if (searchState.Options.CountOfVectors == 0)
            return new VectorSearchRetriever(searchState, searchState.EmptySearch(), vector, minimumSimilarity, ownsSearchState: false, queryVectorScope);

        var nearestNodesByLevel = new ContextBoundNativeList<int>(searchState.Llt.Allocator);
        nearestNodesByLevel.EnsureCapacityFor(searchState.Options.MaxLevel + 1);

        searchState.SearchNearestAcrossLevels(vector.Span, -1, searchState.Options.MaxLevel, ref nearestNodesByLevel);
        var nearest = nearestNodesByLevel[0];
        nearestNodesByLevel.Clear();
        var nearestEdgesSearch = searchState.NearestSearch(nearest, vector, 0, numberOfCandidates, nearestNodesByLevel,
            SearchState.NearestEdgesFlags.StartingPointAsEdge | SearchState.NearestEdgesFlags.FilterNodesWithEmptyPostingLists, hasFilterMatch);
        return new VectorSearchRetriever(searchState, nearestEdgesSearch, vector, minimumSimilarity, ownsSearchState: false, queryVectorScope);
    }

    /// <summary>
    /// Exact nearest neighbor search using a caller-owned SearchState.
    /// The caller is responsible for disposing the SearchState.
    /// </summary>
    public static VectorSearchRetriever ExactNearest(SearchState searchState, int numberOfCandidates, Memory<byte> vector, float minimumSimilarity, bool hasFilterMatch, ContextBoundNativeList<long>? nodesToScan = null)
    {
        TryNormalizeQueryVector(searchState.Options, searchState.Llt.Allocator, ref vector, out var queryVectorScope);
        searchState.OnQueryVector(vector);
        var results = searchState.ExactSearch(vector, hasFilterMatch, numberOfCandidates, nodesToScan);
        return new VectorSearchRetriever(searchState, results, vector, minimumSimilarity, ownsSearchState: false, queryVectorScope);
    }

    /// <summary>
    /// Approximate filtered nearest neighbor search using a caller-owned SearchState.
    /// The caller is responsible for disposing the SearchState.
    /// </summary>
    public static VectorSearchRetriever ApproximateFilteredNearest<TEnumerator>(SearchState searchState, int numberOfCandidates, Memory<byte> vector, float minimumSimilarity, TEnumerator nodesToProbe)
        where TEnumerator : IEnumerator<long>
    {
        TryNormalizeQueryVector(searchState.Options, searchState.Llt.Allocator, ref vector, out var queryVectorScope);
        searchState.OnQueryVector(vector);

        if (searchState.Options.CountOfVectors == 0)
            return new VectorSearchRetriever(searchState, searchState.EmptySearch(), vector, minimumSimilarity, ownsSearchState: false, queryVectorScope);

        var startingPointsIndexes = new ContextBoundNativeList<int>(searchState.Llt.Allocator);
        var candidates = new ContextBoundNativeList<int>(searchState.Llt.Allocator);
        candidates.EnsureCapacityFor(searchState.Options.MaxLevel + 1);

        searchState.SearchFilteredNearest(ref startingPointsIndexes, nodesToProbe, numberOfCandidates, 16);
        candidates.Clear();
        var nearestEdgesSearch = searchState.NearestSearch(startingPointsIndexes, vector, 0, numberOfCandidates, candidates,
            SearchState.NearestEdgesFlags.StartingPointAsEdge | SearchState.NearestEdgesFlags.FilterNodesWithEmptyPostingLists);
        return new VectorSearchRetriever(searchState, nearestEdgesSearch, vector, minimumSimilarity, ownsSearchState: false, queryVectorScope);
    }

    public static VectorSearchRetriever EmptySearch(LowLevelTransaction llt, Slice name, int numberOfCandidates, Memory<byte> vector, float minimumSimilarity)
    {
        var searchState = new SearchState(llt, name);
        return new VectorSearchRetriever(searchState, searchState.EmptySearch(), vector, minimumSimilarity);
    }
}

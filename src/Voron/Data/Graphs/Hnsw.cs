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
        private int _visitsCounter;
        public readonly delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, float> SimilarityCalc;
        public readonly bool IsEmpty;

        public Span<Node> Nodes => _nodes.ToSpan();
        public Tree Tree => _tree;

        public int CreatedNodes => _newNodes.Count;

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

        public SearchState(LowLevelTransaction llt, Slice name)
        {
            Llt = llt;
            _tree = llt.Transaction.ReadTree(name);

            if (_tree is null || _tree.TryGetLookupFor(NodeIdToLocationSlice, out _nodeIdToLocations) == false)
            {
                IsEmpty = true;
                return;
            }

            var options = _tree.DirectRead(OptionsSlice);
            Options = Unsafe.Read<Options>(options);
            SimilarityCalc = Options.SimilarityMethod switch
            {
                SimilarityMethod.CosineSimilaritySingles => &CosineDistanceSingles,
                SimilarityMethod.CosineSimilarityI8 => &CosineDistanceI8,
                SimilarityMethod.HammingDistance => &HammingDistance,
                _ => throw new ArgumentOutOfRangeException(nameof(Options.SimilarityMethod), Options.SimilarityMethod, null)
            };
        }

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
            int nodeIndex = _nodes.Count;
            _nodes.Add(Llt.Allocator, new Node { NodeId = nodeId });
            return nodeIndex;
        }

        public bool TryGetLocationForNode(long nodeId, out long locationId) =>
            _nodeIdToLocations.TryGetValue(nodeId, out locationId);

        public void RegisterNodeLocation(long nodeId, long locationId) =>
            _nodeIdToLocations.Add(nodeId, locationId);

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
            if (to.QueryDistance is not null)
                return to.QueryDistance.Value;
            
            Span<byte> v2 = to.GetVector(this);
            var distance = SimilarityCalc(vector, v2);
            
            return distance;
        }

        // Allows storing cached distance to the queried vector. Should be used only in the querying part!
        public float QueryDistance(ReadOnlySpan<byte> vector, int toIdx, ref long vectorReadCounter)
        {
            ref var to = ref GetNodeByIndex(toIdx);
            if (to.QueryDistance is not null)
            {
                return to.QueryDistance.Value;
            }
            
            Span<byte> v2 = to.GetVector(this);
            vectorReadCounter++;
            var distance = SimilarityCalc(vector, v2);
            to.QueryDistance = distance;

            return distance;
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
                var currentNodeMaximumLevel = entry.EdgesPerLevel.Count;

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
                    ref var n = ref GetNodeByIndex(currentNodeIndex);
                    Debug.Assert(n.EdgesPerLevel.Count > level, "n.EdgesPerLevel.Count > level");
                    ref var edges = ref n.EdgesPerLevel[level];
                    nodeIds.ResetAndCopyFrom(Llt.Allocator, edges.ToSpan());
                    LoadNodeIndexes(ref nodeIds, ref indexes);
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
               _nodes[nodeIndexes[i]].SetVector(this, spans[i]);
           }
       }

       public bool TryGetNodeById(long nodeId, out int nodeIndex)
       {
           return _nodeIdToIdx.TryGetValue(nodeId, out nodeIndex);
       }

       public void Dispose()
       {
           _candidatesQ.Clear();
           _nearestEdgesQ.Clear();
           _newNodes.Dispose(Llt.Allocator);
           
           _nodes.Dispose(Llt.Allocator);
           
       }
    }

    public partial class Registration : IDisposable
    {
        public bool IsCommited { get; private set; }
        private readonly Dictionary<ByteString, (ByteString Key, int NodeIndex, NativeList<long> PostingList)> _vectorHashCache = new(ByteStringContentComparer.Instance);
        private readonly Lookup<Int64LookupKey> _nodesByVectorId;
        private SearchState _searchState;
        public Random Random;
        private readonly CompactTree _vectorsByHash;
        private readonly int _vectorBatchSizeInPages;
        private readonly ContainerId _globalVectorsContainerId;
        private PostingList _largePostingListSet;

        public int AmountOfModifiedVectorsInTransaction => _vectorHashCache.Count;

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
            PortableExceptions.ThrowIf<ArgumentOutOfRangeException>(
                vector.Length != _searchState.Options.VectorSizeBytes,
                $"Vector size {vector.Length} does not match expected size: {_searchState.Options.VectorSizeBytes}");

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
            //todo: we may wants to release the vector hash cache
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
        var searchState = new SearchState(llt, name);
        var results = searchState.ExactSearch(vector, hasFilterMatch, numberOfCandidates, nodesToScan);
        return new VectorSearchRetriever(searchState,  results, vector, minimumSimilarity);
    }

    public static VectorSearchRetriever ApproximateFilteredNearest<TEnumerator>(LowLevelTransaction llt, Slice name, int numberOfCandidates, Memory<byte> vector, float minimumSimilarity, TEnumerator nodesToProbe)
     where TEnumerator : IEnumerator<long> 
    {
        var searchState = new SearchState(llt, name);
        var startingPointsIndexes = new ContextBoundNativeList<int>(llt.Allocator);
        var candidates = new ContextBoundNativeList<int>(llt.Allocator);
        candidates.EnsureCapacityFor(searchState.Options.MaxLevel + 1);

        if (searchState.Options.CountOfVectors == 0)
            return new VectorSearchRetriever(searchState, searchState.EmptySearch(), vector, minimumSimilarity);
        
        searchState.SearchFilteredNearest(ref startingPointsIndexes, nodesToProbe, numberOfCandidates, 16);
        candidates.Clear();
        var nearestEdgesSearch = searchState.NearestSearch(startingPointsIndexes, vector, 0, numberOfCandidates, candidates,
            SearchState.NearestEdgesFlags.StartingPointAsEdge | SearchState.NearestEdgesFlags.FilterNodesWithEmptyPostingLists);
        return new VectorSearchRetriever(searchState, nearestEdgesSearch, vector, minimumSimilarity);
    }
    
    public static VectorSearchRetriever ApproximateNearest(LowLevelTransaction llt, Slice name, int numberOfCandidates, Memory<byte> vector, float minimumSimilarity, bool hasFilterMatch = false)
    {
        var searchState = new SearchState(llt, name);
        var nearestNodesByLevel = new ContextBoundNativeList<int>(llt.Allocator);
        nearestNodesByLevel.EnsureCapacityFor(searchState.Options.MaxLevel + 1);

        if (searchState.Options.CountOfVectors == 0)
            return new VectorSearchRetriever(searchState, searchState.EmptySearch(), vector, minimumSimilarity);
        
        searchState.SearchNearestAcrossLevels(vector.Span, -1, searchState.Options.MaxLevel, ref nearestNodesByLevel);
        var nearest = nearestNodesByLevel[0];
        nearestNodesByLevel.Clear();
        var nearestEdgesSearch = searchState.NearestSearch(nearest, vector, 0, numberOfCandidates, nearestNodesByLevel,
            SearchState.NearestEdgesFlags.StartingPointAsEdge | SearchState.NearestEdgesFlags.FilterNodesWithEmptyPostingLists, hasFilterMatch);
        return new VectorSearchRetriever(searchState, nearestEdgesSearch, vector, minimumSimilarity);
    }

    public static VectorSearchRetriever EmptySearch(LowLevelTransaction llt, Slice name, int numberOfCandidates, Memory<byte> vector, float minimumSimilarity)
    {
        var searchState = new SearchState(llt, name);
        return new VectorSearchRetriever(searchState, searchState.EmptySearch(), vector, minimumSimilarity);
    }
}

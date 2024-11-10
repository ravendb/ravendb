using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Numerics.Tensors;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron.Data.BTrees;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;
using Container = Voron.Data.Containers.Container;

namespace Voron.Data.Graphs;

public unsafe partial class Hnsw
{
    public const long EntryPointId = 1;


    private static readonly Slice VectorsContainerIdSlice;
    private static readonly Slice NodeIdToLocationSlice;
    private static readonly Slice NodesByVectorIdSlice;
    private static readonly Slice VectorsIdByHashSlice;
    private static readonly Slice HnswGlobalConfigSlice;
    private static readonly Slice OptionsSlice;

    static Hnsw()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
        {
            // Global to all HNSWs
            Slice.From(ctx, "VectorsContainerId", ByteStringType.Immutable, out VectorsContainerIdSlice);
            Slice.From(ctx, "HnswGlobalConfig", ByteStringType.Immutable, out HnswGlobalConfigSlice);
            Slice.From(ctx, "VectorsIdByHash", ByteStringType.Immutable, out VectorsIdByHashSlice);
            // Local to a single HNSW
            Slice.From(ctx, "NodeIdToLocation", ByteStringType.Immutable, out NodeIdToLocationSlice);
            Slice.From(ctx, "NodesByVectorId", ByteStringType.Immutable, out NodesByVectorIdSlice);
            Slice.From(ctx, "Options", ByteStringType.Immutable, out OptionsSlice);
        }
    }


    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 64)]
    public struct Options
    {
        [FieldOffset(0)]
        public int VectorSizeBytes;

        [FieldOffset(4)] // M - Number of edges
        public int NumberOfEdges;

        [FieldOffset(8)] // EfConstruction - Number of candidates 
        public int NumberOfCandidates;

        [FieldOffset(12)] // this is used only in debug, not important for persistence
        public int Version;  
        
        [FieldOffset(16)]
        public long CountOfVectors;

        [FieldOffset(24)]
        public long Container;

        [FieldOffset(32)]
        public long GlobalVectorsContainerId;

        public int MaxLevel => BitOperations.Log2((ulong)CountOfVectors);
        
        public int CurrentMaxLevel(int reduceBy) => BitOperations.Log2((ulong)(CountOfVectors - reduceBy));
    }

    public ref struct NodeReader(ByteStringContext allocator, Span<byte> buffer)
    {
        public long PostingListId;
        public long VectorId;
        public int CountOfLevels;

        private int _offset;
        private readonly Span<byte> _buffer = buffer;

        public void LoadInto(ref Node node)
        {
            node.VectorId = VectorId;
            node.PostingListId = PostingListId;
            node.EdgesPerLevel.EnsureCapacityFor(allocator, CountOfLevels);
            while (NextReadEdges(out var list))
            {
                node.EdgesPerLevel.AddUnsafe(list);
            }
        }

        private bool NextReadEdges(out NativeList<long> list)
        {
            if (_offset >= _buffer.Length)
            {
                list = default;
                return false;
            }

            var count = VariableSizeEncoding.Read<int>(_buffer, out int offset, _offset);
            _offset += offset;
            list = new NativeList<long>();
            list.EnsureCapacityFor(allocator, count);
            long prev = 0;
            for (int i = 0; i < count; i++)
            {
                var item = VariableSizeEncoding.Read<long>(_buffer, out offset, _offset);
                _offset += offset;
                prev += item;
                Debug.Assert(prev >= 0, "prev >= 0");
                list.AddUnsafe(prev);
            }
            return true;
        }
    }

    public struct Node
    {
        public long PostingListId;
        public long VectorId;
        public long NodeId;
        public NativeList<NativeList<long>> EdgesPerLevel;
        private UnmanagedSpan _vectorSpan;
        public int Visited;

        public static NodeReader Decode(LowLevelTransaction llt, long id)
        {
            var span = Container.Get(llt, id).ToSpan();
            return Decode(llt, span);
        }

        public static NodeReader Decode(LowLevelTransaction llt, Span<byte> span)
        {
            var postingListId = VariableSizeEncoding.Read<long>(span, out var pos);
            var offset = pos;
            var vectorId = VariableSizeEncoding.Read<long>(span, out pos, offset);
            offset += pos;
            var countOfLevels = VariableSizeEncoding.Read<int>(span, out pos, offset);
            offset += pos;

            return new NodeReader(llt.Allocator, span[offset..])
            {
                PostingListId = postingListId,
                VectorId = vectorId,
                CountOfLevels = countOfLevels
            };
        }

        public Span<byte> Encode(ref ContextBoundNativeList<byte> buffer)
        {
            int countOfLevels = EdgesPerLevel.Count;

            // posting list id, vector id, count of levels
            var maxSize = 3 * VariableSizeEncoding.MaximumSizeOf<long>();
            for (int i = 0; i < countOfLevels; i++)
            {
                maxSize += EdgesPerLevel[i].Count * VariableSizeEncoding.MaximumSizeOf<long>();
            }
            buffer.EnsureCapacityFor(maxSize);

            var bufferSpan = buffer.ToFullCapacitySpan();
            
            var pos = VariableSizeEncoding.Write(bufferSpan, PostingListId);
            pos += VariableSizeEncoding.Write(bufferSpan, VectorId, pos);
            pos += VariableSizeEncoding.Write(bufferSpan, countOfLevels, pos);
            
            for (int i = 0; i < countOfLevels; i++)
            {
                Span<long> span = EdgesPerLevel[i].ToSpan();
                int len = Sorting.SortAndRemoveDuplicates(span);
                span = span[..len];
                if (len != span.Length)
                {
                    Console.WriteLine();
                }
                long prev = 0;
                pos += VariableSizeEncoding.Write(bufferSpan, span.Length, pos);
                for (int j = 0; j < span.Length; j++)
                {
                    var delta = span[j] - prev;
                    prev = span[j];
                    pos += VariableSizeEncoding.Write(bufferSpan, delta, pos);
                }
            }

            return bufferSpan[..pos];
        }

        public Span<byte> GetVector(LowLevelTransaction llt)
        {
            if (_vectorSpan.Length > 0) 
                return _vectorSpan.ToSpan();
            
            var item = Container.Get(llt, VectorId);
            _vectorSpan = new UnmanagedSpan(item.Address, item.Length);

            return _vectorSpan.ToSpan();
        }
    }

    public static void Create(LowLevelTransaction llt, string name, int vectorSizeBytes, int numberOfEdges, int numberOfCandidates)
    {
        using var _ = Slice.From(llt.Allocator, name, out var slice);
        Create(llt, slice, vectorSizeBytes, numberOfEdges, numberOfCandidates);

    }
    public static void Create(LowLevelTransaction llt, Slice name, int vectorSizeBytes, int numberOfEdges, int numberOfCandidates)
    {
        var tree = llt.Transaction.CreateTree(name);
        if (tree.ReadHeader().NumberOfEntries is not 0)
            return; // already created

        // global creation for all HNSWs in the database
        var vectorsContainerId = CreateHnswGlobalState(llt);
        long storage = Container.Create(llt);
        tree.LookupFor<Int64LookupKey>(NodeIdToLocationSlice);
        tree.LookupFor<Int64LookupKey>(NodesByVectorIdSlice);
        var options = new Options
        {
            Version = 1,
            VectorSizeBytes = vectorSizeBytes,
            CountOfVectors = 0,
            Container = storage,
            NumberOfEdges = numberOfEdges,
            NumberOfCandidates = numberOfCandidates,
            GlobalVectorsContainerId = vectorsContainerId
        };
        using (tree.DirectAdd(OptionsSlice, sizeof(Options), out var output))
        {
            Unsafe.Write(output, options);
        }
    }

    private static long CreateHnswGlobalState(LowLevelTransaction llt)
    {
        llt.Transaction.CompactTreeFor(VectorsIdByHashSlice);
        var config = llt.Transaction.CreateTree(HnswGlobalConfigSlice);
        var read = config.DirectRead(VectorsContainerIdSlice);
        if (read is not null)
            return Unsafe.Read<long>(read);

        long vectorsContainerId = Container.Create(llt);
        config.Add(VectorsContainerIdSlice, vectorsContainerId);
        return vectorsContainerId;
    }

    public struct SearchState
    {
        private readonly PriorityQueue<int, float> _candidatesQ = new();
        private readonly PriorityQueue<int, float> _nearestEdgesQ = new();
        private readonly Dictionary<long, int> _nodeIdToIdx = new();
        private Node[] _nodes = [];//TODO: Native List instead of array to spare managed allocs
        private int _nodesCount;
        private readonly Tree _tree;
        private readonly Lookup<Int64LookupKey> _nodeIdToLocations;
        public readonly LowLevelTransaction Llt;
        private int _visitsCounter;

        public Span<Node> Nodes => _nodes.AsSpan(0, _nodesCount);
        public Tree Tree => _tree;

        public int CreatedNodesCount;

        public Options Options;

        public SearchState(LowLevelTransaction llt, string name): this(llt, SliceFromString(llt, name))
        {
        }

        private static Slice SliceFromString(LowLevelTransaction llt, string name)
        {
            Slice.From(llt.Allocator, name, out var slice);
            return slice;
        }

        public SearchState(LowLevelTransaction llt, Slice name)
        {
            Llt = llt;
            
            _tree = llt.Transaction.ReadTree(name);
            _nodeIdToLocations = _tree.LookupFor<Int64LookupKey>(NodeIdToLocationSlice);
            var options = _tree.DirectRead(OptionsSlice);
            Options = Unsafe.Read<Options>(options);
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
            CreatedNodesCount++;
            int nodeIndex = AllocateNodeIndex(newNodeId);
            _nodes[nodeIndex].VectorId = vectorId;

            _nodeIdToIdx[newNodeId] = nodeIndex;
            return nodeIndex;
        }

        private int AllocateNodeIndex(long nodeId)
        {
            if (_nodesCount == _nodes.Length)
            {
                Array.Resize(ref _nodes, (int)BitOperations.RoundUpToPowerOf2((nuint)_nodes.Length + 1));
            }

            int nodeIndex = _nodesCount++;
            _nodes[nodeIndex].NodeId = nodeId;
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
        /// This accepts a list of node ids (mutable, we do destructive to it) and translate
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
            
            using var _ = Llt.Allocator.AllocateDirect(sizeof(UnmanagedSpan) * keys.Length, out var buffer);
            var spans = (UnmanagedSpan*)buffer.Ptr;
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

            nodeIdx =  AllocateNodeIndex(nodeId);
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

        public float Distance(Span<byte> vector, ref Node from, ref Node to)
        {
            if (vector.IsEmpty)
            {
                vector = from.GetVector(Llt);
            }

            Span<byte> v2 = to.GetVector(Llt);
            var distance = 1 - TensorPrimitives.CosineSimilarity(
                MemoryMarshal.Cast<byte, float>(vector),
                MemoryMarshal.Cast<byte, float>(v2)
            );
            return distance;
        }
        
        public void ReadPostingList(long rawPostingListId, ref ContextBoundNativeList<long> listBuffer, FastPForDecoder pforDecoder, out int postingListSize)
        {
            var smallPostingList = Container.Get(Llt, rawPostingListId);
            var count = VariableSizeEncoding.Read<int>(smallPostingList.Address, out var offset);
            listBuffer.EnsureCapacityFor(Math.Max(256, count + listBuffer.Count));
            Debug.Assert(listBuffer.Capacity > 0 && listBuffer.Capacity % 256 ==0, "The buffer must be multiple of 256 for PForDecoder.Read");
            pforDecoder.Init(smallPostingList.Address + offset, smallPostingList.Length - offset);
            listBuffer.Count = pforDecoder.Read(listBuffer.RawItems, listBuffer.Capacity);
            postingListSize = smallPostingList.Length;
        }

        public void FilterEdgesHeuristic(ref Node src, ref NativeList<int> candidates)
        {
            // See: https://icode.best/i/45208840268843 - Chinese, but auto-translate works, and a good explanation with 
            // conjunction of: https://img-bc.icode.best/20210425010212938.png
            // See also the paper here: https://arxiv.org/pdf/1603.09320
            // This implements the Fig. 2 / Algorithm 4
            
            Debug.Assert(_candidatesQ.Count is 0);
            for (int i = 0; i < candidates.Count; i++)
            {
                var dstIndex = candidates[i];
                var distance = Distance(Span<byte>.Empty, ref src, ref GetNodeByIndex(dstIndex));
                _candidatesQ.Enqueue(dstIndex, distance);
            }

            candidates.Clear();
            
            while (candidates.Count <= Options.NumberOfEdges &&
                   _candidatesQ.TryDequeue(out var cur, out var distance))
            {
                bool match = true;
                for (int i = 0; i < candidates.Count; i++)
                {
                    int alternativeIndex = candidates[i];
                    var curDist = Distance(Span<byte>.Empty, ref GetNodeByIndex(cur), ref GetNodeByIndex(alternativeIndex));
                    // there is already an item in the result that is *closer* to the current
                    // node than the target node, so no need to add it
                    if (curDist < distance)
                    {
                        match = false;
                        break;
                    }
                }

                if (match)
                {
                    candidates.AddUnsafe(cur);
                }
            }

            _candidatesQ.Clear();
        }


        public void NearestEdges(int startingPointIndex, int level, int numberOfCandidates, Span<byte> vector, ref Node dst, ref NativeList<int> levelEdges, bool startingPointAsEdge)
        {
            Debug.Assert(_candidatesQ.Count is 0 && _nearestEdgesQ.Count is 0);
            ref var startingPointNode = ref GetNodeByIndex(startingPointIndex);
            float lowerBound = -Distance(vector, ref dst, ref startingPointNode);
            var visitedCounter = ++_visitsCounter;
            startingPointNode.Visited = visitedCounter;
            
            // candidates queue is sorted using the distance, so the lowest distance
            // will always pop first.
            // nearest edges is sorted using _reversed_ distance, so when we add a 
            // new item to the queue, we'll pop the one with the largest distance
            
            _candidatesQ.Enqueue(startingPointIndex, -lowerBound);
            if (startingPointAsEdge)
            {
                _nearestEdgesQ.Enqueue(startingPointIndex, lowerBound);
            }
            var indexes = new NativeList<int>();
            var nodeIds = new NativeList<long>();
            while (_candidatesQ.TryDequeue(out var cur, out var curDistance))
            {
                if (-curDistance < lowerBound && 
                    _nearestEdgesQ.Count == numberOfCandidates)
                    break;

                ref var candidate = ref GetNodeByIndex(cur);
                candidate.Visited = visitedCounter;
             
                ref var edges = ref candidate.EdgesPerLevel[level];

                nodeIds.ResetAndCopyFrom(Llt.Allocator, edges.ToSpan());
                LoadNodeIndexes(ref nodeIds, ref indexes);

                for (int i = 0; i < indexes.Count; i++)
                {
                    var nextIndex = indexes[i];
                    ref var next = ref GetNodeByIndex(nextIndex);
                    if(next.Visited == visitedCounter)
                        continue;
            
                    float nextDist = -Distance(vector, ref dst, ref next);
                    if (_nearestEdgesQ.Count < numberOfCandidates)
                    {
                        _candidatesQ.Enqueue(nextIndex, -nextDist);
                        _nearestEdgesQ.Enqueue(nextIndex, nextDist);
                    }
                    else if (lowerBound < nextDist)
                    {
                        _candidatesQ.Enqueue(nextIndex, -nextDist);
                        _nearestEdgesQ.EnqueueDequeue(nextIndex, nextDist);
                    }
                    else
                    {
                        continue;
                    }
                    
                    Debug.Assert(_candidatesQ.Count > 0);
                    _nearestEdgesQ.TryPeek(out _, out lowerBound);
                }
            }

            _candidatesQ.Clear();
            levelEdges.EnsureCapacityFor(Llt.Allocator, _nearestEdgesQ.Count);
            var pq = new PriorityQueue<long, float>();
            while (_nearestEdgesQ.TryDequeue(out var edgeId, out var d))
            {
                pq.Enqueue(GetNodeByIndex(edgeId).NodeId, d);
                levelEdges.AddUnsafe(edgeId);
            }
            levelEdges.Reverse();

            nodeIds.Dispose(Llt.Allocator);
            indexes.Dispose(Llt.Allocator);
        }

        public void SearchNearestAcrossLevels(Span<byte> vector, ref Node dst, int maxLevel, ref NativeList<int> nearestIndexes)
        {
            var visitCounter = ++_visitsCounter;
            var currentNodeIndex = GetNodeIndexById(EntryPointId);
            var level = maxLevel;
            ref var entry = ref GetNodeByIndex(currentNodeIndex);
            entry.EdgesPerLevel.SetCapacity(Llt.Allocator, maxLevel + 1);
            var distance = Distance(vector, ref dst, ref entry);
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
                        var curDist = Distance(vector, ref dst, ref edge);
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
            nearestIndexes.Reverse();
        }
    }
    
    public class Registration : IDisposable
    {
        //TODO: make into NativeList
        private readonly Dictionary<ByteString, (int NodeIndex, List<long> PostingList)> _vectorHashCache = new(ByteStringContentComparer.Instance);
        private readonly Lookup<Int64LookupKey> _nodesByVectorId;
        private SearchState _searchState;
        public Random Random = Random.Shared;
        private readonly CompactTree _vectorsByHash;

        public Registration(LowLevelTransaction llt, Slice name)
        {
            _searchState = new SearchState(llt, name);
            _nodesByVectorId = _searchState.Tree.LookupFor<Int64LookupKey>(NodesByVectorIdSlice);
            _vectorsByHash = llt.Transaction.CompactTreeFor(VectorsIdByHashSlice);
        }

        public void Register(long entryId, Span<byte> vector)
        {
            PortableExceptions.ThrowIfOnDebug<ArgumentOutOfRangeException>((entryId & 0b11) != 0, "Entry ids must have the first two bits cleared, we are using those");
            PortableExceptions.ThrowIf<ArgumentOutOfRangeException>(
                vector.Length != _searchState.Options.VectorSizeBytes,
                $"Vector size {vector.Length} does not match expected size: {_searchState.Options.VectorSizeBytes}");

            var hashBuffer = ComputeHashFor(vector);
            ref var postingList = ref CollectionsMarshal.GetValueRefOrAddDefault(_vectorHashCache, hashBuffer, out var exists);
            if(exists)
            {
                // already added this in the current batch
                postingList.PostingList.Add(entryId);
                // key already exists in the dictionary, so can clear this 
                _searchState.Llt.Allocator.Release(ref hashBuffer);
                return;
            }

            var vectorHash = hashBuffer.ToReadOnlySpan();
            if (_vectorsByHash.TryGetValue(vectorHash, out var vectorId) is false)
            {
                vectorId = Container.Allocate(_searchState.Llt, _searchState.Options.GlobalVectorsContainerId, vector.Length, out var vectorStorage);
                vector.CopyTo(vectorStorage);
                _vectorsByHash.Add(vectorHash, vectorId);
            }
            
            if (_nodesByVectorId.TryGetValue(vectorId, out var nodeId))
            {
                int nodeIndex = _searchState.GetNodeIndexById(nodeId);
                postingList = (nodeIndex,[entryId]);
                return;
            }

            long newNodeId = ++_searchState.Options.CountOfVectors;
            int nodeIdx = _searchState.RegisterVectorNode(newNodeId, vectorId);
            _nodesByVectorId.Add(vectorId, newNodeId);
            postingList = (nodeIdx,[entryId]);
        }

        private ByteString ComputeHashFor(Span<byte> vector)
        {
            _searchState.Llt.Allocator.AllocateDirect(Sodium.GenericHashSize, out var hashBuffer);
            Sodium.GenericHash(vector, hashBuffer.ToSpan());
            return hashBuffer;
        }
        
        private int GetLevelForNewNode(int maxLevel)
        {
            int level = 0;
            while ((Random.Next() & 1) == 0 && // 50% chance 
                   level < maxLevel)
            {
                level++;
            }
            return level;
        }

        public void Dispose()
        {
            PortableExceptions.ThrowIfOnDebug<InvalidOperationException>(_searchState.Llt.Committed);
            
            using var pforDecoder = new FastPForDecoder(_searchState.Llt.Allocator);
            using var pforEncoder = new FastPForEncoder(_searchState.Llt.Allocator);
            
            var listBuffer = new ContextBoundNativeList<long>(_searchState.Llt.Allocator);
            var byteBuffer = new ContextBoundNativeList<byte>(_searchState.Llt.Allocator);
            byteBuffer.EnsureCapacityFor(128);

            var nodes = _searchState.Nodes;
            foreach (var (_, (nodeIndex, modifications)) in _vectorHashCache)
            {
                ref var node = ref nodes[nodeIndex];
                node.PostingListId = MergePostingList(node.PostingListId, modifications);
            }
            
            // Intentionally zeroing the nodes var, we may realloc the underlying array in the insert vector phase
            nodes = Span<Node>.Empty;
            _ = nodes;

            InsertVectorsToGraph(ref byteBuffer);

            nodes = _searchState.Nodes;
            for (int i = 0; i < nodes.Length; i++)
            {
                PersistNode(ref nodes[i], ref byteBuffer);
            }

            // flush the local modifications
            _searchState.FlushOptions();
            
            listBuffer.Dispose();
            byteBuffer.Dispose();
            
            
            long MergePostingList(long postingList, List<long> modifications)
            {
                // here we have 5 options:
                // - value does not exist - so it would be 0
                // or - by the first two bits
                // - 0b00 - tombstone
                // - 0b01 - direct value
                // - 0b10 - small posting list
                // - 0b11 - large posting list

                
                listBuffer.Clear();
                listBuffer.AddRange(CollectionsMarshal.AsSpan(modifications));
                int currentSize = 0;
                bool hasSmallPostingList = false;
                long rawPostingListId = postingList & ~0b11;
                switch (postingList & 0b11)
                {
                    case 0b00: // nothing there
                        break;
                    case 0b01: // single value, just add it
                        listBuffer.Add(rawPostingListId);
                        break;
                    case 0b10:
                        hasSmallPostingList = true;
                        _searchState.ReadPostingList(rawPostingListId, ref listBuffer, pforDecoder, out currentSize);
                        break;
                    case 0b11:
                        //TODO: fix large posting lists
                        throw new NotImplementedException();
                }
                
                PostingList.SortEntriesAndRemoveDuplicatesAndRemovals(ref listBuffer);
                if (listBuffer.Count is 0 or 1)
                {
                    if (hasSmallPostingList)
                    {
                        Container.Delete(_searchState.Llt, _searchState.Options.Container, rawPostingListId);
                    }

                    if (listBuffer.Count is 0) 
                        return 0;
                    
                    Debug.Assert((listBuffer[0] & 0b11) == 0, "(listBuffer[0] & 0b11) == 0");
                    return listBuffer[0] | 0b01;
                }

                int size = pforEncoder.Encode(listBuffer.RawItems, listBuffer.Count);
                if (size > Container.MaxSizeInsideContainerPage)
                {
                    throw new NotImplementedException();
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
                    if (hasSmallPostingList)
                    {
                        Container.Delete(_searchState.Llt, _searchState.Options.Container, rawPostingListId);
                    }
                    rawPostingListId = Container.Allocate(_searchState.Llt, _searchState.Options.Container, byteBuffer.Count, out mutable);
                }
                Span<byte> span = byteBuffer.ToSpan();
                span.CopyTo(mutable);

                Debug.Assert((rawPostingListId & 0b11) == 0, "(rawPostingListId & 0b11) == 0");
                return rawPostingListId | 0b10;
            }
        }


        void PersistNode(ref Node node, ref ContextBoundNativeList<byte> byteBuffer)
        {
            var encoded = node.Encode(ref byteBuffer);
            if (_searchState.TryGetLocationForNode(node.NodeId, out var locationId))
            {
                var existing = Container.GetMutable(_searchState.Llt, locationId);
                if (existing.Length == encoded.Length)
                {
                    encoded.CopyTo(encoded);
                    return;
                }

                Container.Delete(_searchState.Llt, _searchState.Options.Container, locationId);
            }

            locationId = Container.Allocate(_searchState.Llt, _searchState.Options.Container, encoded.Length, out var storage);
            _searchState.RegisterNodeLocation(node.NodeId, locationId);
            encoded.CopyTo(storage);
        }


        void InsertVectorsToGraph(ref ContextBoundNativeList<byte> byteBuffer)
        {
            if (_searchState.TryGetLocationForNode(EntryPointId, out var entryPointNode) is false)
            {
                if (_searchState.CreatedNodesCount == 0)
                    return;

                ref Node startingNode = ref _searchState.Nodes[0];
                Span<byte> span = startingNode.Encode(ref byteBuffer);
                entryPointNode = Container.Allocate(_searchState.Llt, _searchState.Options.Container, span.Length, out Span<byte> allocated);
                span.CopyTo(allocated);
                _searchState.RegisterNodeLocation(EntryPointId, entryPointNode);
            }

            var nearestNodesByLevel = new NativeList<int>();
            var edges = new NativeList<int>();
            var tmp = new NativeList<int>();

            nearestNodesByLevel.EnsureCapacityFor(_searchState.Llt.Allocator, _searchState.Options.MaxLevel + 1);

            for (int currentNodeIndex = 0; currentNodeIndex < _searchState.CreatedNodesCount; currentNodeIndex++)
            {
                nearestNodesByLevel.Clear();

                var currentMaxLevel = _searchState.Options.CurrentMaxLevel(_searchState.CreatedNodesCount - currentNodeIndex);
                int nodeRandomLevel = GetLevelForNewNode(currentMaxLevel);
                ref var node = ref _searchState.GetNodeByIndex(currentNodeIndex);
                node.EdgesPerLevel.SetCapacity(_searchState.Llt.Allocator, nodeRandomLevel + 1);

                var vector = node.GetVector(_searchState.Llt);

                _searchState.SearchNearestAcrossLevels(vector, ref node, currentMaxLevel, ref nearestNodesByLevel);

                for (int level = nodeRandomLevel; level >= 0; level--)
                {
                    int startingPointIndex = nearestNodesByLevel[level];
                    edges.Clear();
                    _searchState.NearestEdges(startingPointIndex, level,
                        _searchState.Options.NumberOfCandidates,
                        vector, ref node,
                        ref edges,
                        startingPointAsEdge: currentNodeIndex != startingPointIndex);

                    if (edges.Count > _searchState.Options.NumberOfEdges)
                        _searchState.FilterEdgesHeuristic(ref node, ref edges);

                    ref var list = ref node.EdgesPerLevel[level];
                    list.EnsureCapacityFor(_searchState.Llt.Allocator, edges.Count);
                    list.Clear();
                    for (int i = 0; i < edges.Count; i++)
                    {
                        ref Node edge = ref _searchState.GetNodeByIndex(edges[i]);
                        list.AddUnsafe(edge.NodeId);
                        Debug.Assert(edge.NodeId != node.NodeId, "edge.NodeId != node.NodeId");

                        ref var edgeList = ref edge.EdgesPerLevel[level];
                        edgeList.Add(_searchState.Llt.Allocator, node.NodeId);

                        if (edgeList.Count <= _searchState.Options.NumberOfEdges)
                            continue;

                        // FilterEdgesHeuristic works on node indexes, while edges list is node ids
                        // so we need to convert them back & forth in this manner
                        tmp.ResetAndEnsureCapacity(_searchState.Llt.Allocator, edgeList.Count);
                        for (int k = 0; k < edgeList.Count; k++)
                        {
                            tmp.AddUnsafe(_searchState.GetNodeIndexById(edgeList[k]));
                        }
                        _searchState.FilterEdgesHeuristic(ref edge, ref tmp);
                        edgeList.Clear();
                        for (int k = 0; k < tmp.Count; k++)
                        {
                            edgeList.AddUnsafe(_searchState.GetNodeByIndex(tmp[k]).NodeId);
                        }
                    }
                }
            }
        }
    }

    public static Registration RegistrationFor(LowLevelTransaction llt, string name)
    {
        Slice.From(llt.Allocator, name, out var slice);
        return RegistrationFor(llt, slice);
    }
    public static Registration RegistrationFor(LowLevelTransaction llt, Slice name)
    {
        return new Registration(llt, name);
    }

    public static NearestSearch ExactNearest(LowLevelTransaction llt, string name, int numberOfCandidates, Span<byte> vector)
    {
        Slice.From(llt.Allocator, name, out var slice);
        return ExactNearest(llt, slice, numberOfCandidates, vector);
    }

    public static NearestSearch ExactNearest(LowLevelTransaction llt, Slice name, int numberOfCandidates, Span<byte> vector)
    {
        var searchState = new SearchState(llt, name);
        var pq = new PriorityQueue<long, float>();
        for (long nodeId = 1; nodeId <= searchState.Options.CountOfVectors; nodeId++)
        {
            searchState.ReadNode(nodeId, out var reader);
            if (reader.PostingListId is 0)
                continue; // no entries, can skip

            var curVect = Container.Get(llt, reader.VectorId).ToSpan();
            var distance = 1 - TensorPrimitives.CosineSimilarity(
                MemoryMarshal.Cast<byte, float>(vector),
                MemoryMarshal.Cast<byte, float>(curVect)
            );

            if (pq.Count < numberOfCandidates)
            {
                pq.Enqueue(nodeId, -distance);
            }
            else
            {
                pq.EnqueueDequeue(nodeId, -distance);
            }
        }

        var candidates = new ContextBoundNativeList<int>(llt.Allocator);
        while(pq.TryDequeue(out var nodeId, out _))
        {
            var nodeIdx = searchState.GetNodeIndexById(nodeId);
            candidates.Add(nodeIdx);
        }
        candidates.Inner.Reverse();
        return new NearestSearch(searchState, candidates, vector);
    }

    public static NearestSearch ApproximateNearest(LowLevelTransaction llt, string name, int numberOfCandidates, Span<byte> vector)
    {
        Slice.From(llt.Allocator, name, out var slice);
        return ApproximateNearest(llt, slice, numberOfCandidates, vector);
    }

    public static NearestSearch ApproximateNearest(LowLevelTransaction llt, Slice name, int numberOfCandidates, Span<byte> vector)
    {
        var searchState = new SearchState(llt, name);
        var nearestNodesByLevel = new ContextBoundNativeList<int>(llt.Allocator);
        nearestNodesByLevel.EnsureCapacityFor(searchState.Options.MaxLevel + 1);

        if (searchState.Options.CountOfVectors == 0)
            return new NearestSearch(searchState, nearestNodesByLevel, vector);
        
        searchState.SearchNearestAcrossLevels(vector, ref Unsafe.NullRef<Node>(), searchState.Options.MaxLevel, ref nearestNodesByLevel.Inner);
        var nearest = nearestNodesByLevel[0];
        nearestNodesByLevel.Clear();
        searchState.NearestEdges(nearest, level: 0, numberOfCandidates, vector, ref Unsafe.NullRef<Node>(), ref nearestNodesByLevel.Inner, startingPointAsEdge: true);
        return new NearestSearch(searchState, nearestNodesByLevel, vector);
    }

    public ref struct NearestSearch(SearchState searchState, ContextBoundNativeList<int> indexes, Span<byte> vector) 
    {
        private int _currentNode, _currentMatchesIndex;
        private ContextBoundNativeList<long> _postingListResults = new(searchState.Llt.Allocator);
        private FastPForDecoder _pforDecoder = new(searchState.Llt.Allocator);
        private readonly Span<byte> _vector = vector;

        public void Dispose()
        {
            indexes.Dispose();
            _postingListResults.Dispose();
            _pforDecoder.Dispose();
        }

        public int Fill(Span<long> matches)
        {
            return Fill(matches, Span<float>.Empty);
        }
        public int Fill(Span<long> matches, Span<float> distances)
        {
            int index = 0;
            float distance = float.NaN;
            while (index < matches.Length)
            {
                if (_currentNode >= indexes.Count)
                    break;

                if (_currentMatchesIndex < _postingListResults.Count)
                {
                    var copy = Math.Min(_postingListResults.Count - _currentMatchesIndex, matches.Length - index);
                    _postingListResults.CopyTo(matches[index..], _currentMatchesIndex, copy);
                    if (distances.IsEmpty is false)
                    {
                        distances.Slice(index, copy).Fill(distance);
                    }
                    index += copy;
                    _currentMatchesIndex += copy;
                    if(_currentMatchesIndex == _postingListResults.Count)
                    {
                        _currentMatchesIndex = 0;
                        _postingListResults.Clear();
                        _currentNode++;
                    }
                    continue;
                }

                ref var node = ref searchState.GetNodeByIndex(indexes[index]);
                if (distances.IsEmpty is false)
                {
                    distance = searchState.Distance(_vector, ref Unsafe.NullRef<Node>(), ref node);
                }
                var rawPostingListId = node.PostingListId & ~0b11;
                switch (node.PostingListId & 0b11)
                {
                    case 0b00: // empty
                        _currentNode++;
                        continue;
                    case 0b01: // single item posting list
                        if (distances.IsEmpty is false)
                        {
                            distances[index] = distance;
                        }
                        matches[index++] = rawPostingListId;
                        _currentNode++;
                        continue;
                    case 0b10: // small posting list
                        Debug.Assert(_postingListResults.Count is 0 && _currentMatchesIndex is 0);
                        searchState.ReadPostingList(rawPostingListId, ref _postingListResults, _pforDecoder, out _);
                        continue;
                    case 0b11: // large posting list
                        // TODO: large posting list
                        throw new NotSupportedException();
                }
                throw new NotSupportedException();
            }
            return index;
        }
    }
}

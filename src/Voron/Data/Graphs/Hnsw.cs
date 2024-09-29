using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;
using Container = Voron.Data.Containers.Container;

namespace Voron.Data.Graphs;

public unsafe class Hnsw
{
    public const long EntryPointId = 1;

    private const string VectorsPostingListByHash = nameof(VectorsPostingListByHash);
    private const string NodeIdToLocation = nameof(NodeIdToLocation);

    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 64)]
    public struct Options
    {
        [FieldOffset(0)]
        public int VectorSizeBytes;

        [FieldOffset(4)] // M - Number of neighbors
        public int NumberOfNeighbors;

        [FieldOffset(8)] // EfConstruction - Number of candidates 
        public int NumberOfCandidates;

        [FieldOffset(12)] // this is used only in debug, not important for persistence
        public int Version;  
        
        [FieldOffset(16)]
        public long CountOfVectors;

        [FieldOffset(24)]
        public long Container;

        public int MaxLevel => BitOperations.Log2((ulong)CountOfVectors);
    }

    public ref struct NodeReader(ByteStringContext allocator, Span<byte> buffer)
    {
        public long PostingListId;
        public long VectorId;
        public int CountOfLevels;

        private int _offset;
        private readonly Span<byte> _buffer = buffer;

        public bool NextReadNeighbors(out NativeList<long> list)
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
                list.AddUnsafe(prev);
            }
            return true;
        }
    }

    public struct Node
    {
        public long PostingListId;
        public long VectorId;
        public NativeList<NativeList<long>> NeighborsPerLevel;
        public int Visited;

        public static NodeReader Decode(LowLevelTransaction llt, long id)
        {
            var span = Container.Get(llt, id).ToSpan();
            var postingListId = VariableSizeEncoding.Read<long>(span, out var pos);
            var offset = pos;
            var vectorId = VariableSizeEncoding.Read<long>(span, out pos,offset);
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
        
        [SkipLocalsInit]
        public Span<byte> Encode(ByteStringContext bsc, ref ContextBoundNativeList<byte> buffer)
        {
            int countOfLevels = NeighborsPerLevel.Count;

            // posting list id, vector id, count of levels
            var maxSize = 3 * VariableSizeEncoding.MaximumSizeOf<long>();
            for (int i = 0; i < countOfLevels; i++)
            {
                maxSize += NeighborsPerLevel[i].Count * VariableSizeEncoding.MaximumSizeOf<long>();
            }
            buffer.EnsureCapacityFor(maxSize);

            var bufferSpan = buffer.ToFullCapacitySpan();
            
            var pos = VariableSizeEncoding.Write(bufferSpan, PostingListId);
            pos += VariableSizeEncoding.Write(bufferSpan, VectorId, pos);
            pos += VariableSizeEncoding.Write(bufferSpan, countOfLevels, pos);
            
            for (int i = 0; i < countOfLevels; i++)
            {
                Span<long> span = NeighborsPerLevel[i].ToSpan();
                var num = Sorting.SortAndRemoveDuplicates(span);
                long prev = 0;
                pos += VariableSizeEncoding.Write(bufferSpan, num, pos);
                for (int j = 0; j < num; j++)
                {
                    var delta = span[j] - prev;
                    prev = span[j];
                    pos += VariableSizeEncoding.Write(bufferSpan, delta, pos);
                }
            }

            return bufferSpan[..pos];
        }
    }
    
    public static long Create(LowLevelTransaction llt, int vectorSizeBytes, int numberOfNeighbors, int numberOfCandidates)
    {
        long storage = Container.Create(llt);
        
        llt.Transaction.CompactTreeFor(VectorsPostingListByHash);
        llt.Transaction.LookupFor<Int64LookupKey>(NodeIdToLocation);
        
        var id = Container.Allocate(llt, storage, sizeof(Options), out var span);
        MemoryMarshal.AsRef<Options>(span) = new Options
        {
            Version = 1,
            VectorSizeBytes = vectorSizeBytes,
            CountOfVectors = 0,
            Container = storage,
            NumberOfNeighbors = numberOfNeighbors,
            NumberOfCandidates = numberOfCandidates
        };
        return id;
    }

    public struct SearchState
    {
        private readonly PriorityQueue<long, float> _candidatesQ = new();
        private readonly PriorityQueue<long, float> _nearestNeighborsQ = new();
        private readonly Dictionary<long, Node> _nodesById = new();
        private readonly Lookup<Int64LookupKey> _nodeIdToLocations;
        public readonly LowLevelTransaction Llt;
        private readonly CompactTree _vectorsByHash;
        public readonly long GraphId;
        private int _visitsCounter;

        public Dictionary<long, Node>.KeyCollection Keys => _nodesById.Keys;
        
        public int CreatedNodesCount;

        private readonly Dictionary<(long, long), float> _distanceCache = new();
        public Options Options;

        public SearchState(LowLevelTransaction llt, long graphId)
        {
            _nodeIdToLocations = llt.Transaction.LookupFor<Int64LookupKey>(NodeIdToLocation);
            Llt = llt;
            GraphId = graphId;

            var item = Container.Get(llt, graphId);
            Options = MemoryMarshal.Read<Options>(item.ToSpan());
            _vectorsByHash = Llt.Transaction.CompactTreeFor(VectorsPostingListByHash);
        }

        
        public void RegisterVectorNode(long newNodeId, long vectorId, ReadOnlySpan<byte> vectorHash)
        {
            CreatedNodesCount++;
            _nodesById[newNodeId] = new Node { VectorId = vectorId };
            _vectorsByHash.Add(vectorHash, newNodeId);
        }

        public bool TryGetLocationForNode(long nodeId, out long locationId) =>
            _nodeIdToLocations.TryGetValue(nodeId, out locationId);

        public ref Node GetNode(long nodeId) => ref CollectionsMarshal.GetValueRefOrNullRef(_nodesById, nodeId);
        
        public void RegisterNodeLocation(long nodeId, long locationId) =>
            _nodeIdToLocations.Add(nodeId, locationId);


        public ref Node GetNodeById(long nodeId)
        {
            ref var node = ref CollectionsMarshal.GetValueRefOrAddDefault(_nodesById, nodeId, out var exists);
            if (exists)
                return ref node;
            
            if (TryGetLocationForNode(nodeId, out var nodeLocation) is false)
                throw new InvalidOperationException($"Unable to find node id {nodeId}");
                
            var reader = Node.Decode(Llt, nodeLocation);
            node.VectorId = reader.VectorId;
            node.PostingListId = reader.PostingListId;
            node.NeighborsPerLevel.EnsureCapacityFor(Llt.Allocator, reader.CountOfLevels);
            while (reader.NextReadNeighbors(out var list))
            {
                node.NeighborsPerLevel.AddUnsafe(list);
            }

            return ref node;
        }

        private float Distance(Span<byte> vector, long to)
        {
            // we assume that the distance(from,to) == distance(to,from) and normalize the cache key
            var key = (-1, to);
            ref var distance = ref CollectionsMarshal.GetValueRefOrAddDefault(_distanceCache, key, out var exists);

            if (exists) 
                return distance;
            
            Span<byte> v2 = Container.Get(Llt, to).ToSpan();
            distance = 1 - TensorPrimitives.CosineSimilarity(
                MemoryMarshal.Cast<byte, float>(vector),
                MemoryMarshal.Cast<byte, float>(v2)
            );
            return distance;
        }

        private float Distance(long from, long to)
        {
            // we assume that the distance(from,to) == distance(to,from) and normalize the cache key
            var key = (Math.Min(from, to), Math.Max(from, to));
            
            ref var distance = ref CollectionsMarshal.GetValueRefOrAddDefault(_distanceCache, key, out var exists);

            if (exists) 
                return distance;
            
            Span<byte> v1 = Container.Get(Llt, from).ToSpan();
            Span<byte> v2 = Container.Get(Llt, to).ToSpan();
            distance = 1 - TensorPrimitives.CosineSimilarity(
                MemoryMarshal.Cast<byte, float>(v1),
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

        public void FilterNeighborsHeuristic(ref Node src, ref NativeList<long> candidates)
        {
            // See: https://icode.best/i/45208840268843 - Chinese, but auto-translate works, and a good explanation with 
            // conjunction of: https://img-bc.icode.best/20210425010212938.png
            // See also the paper here: https://arxiv.org/pdf/1603.09320
            // This implements the Fig. 2 / Algorithm 4
            
            Debug.Assert(_candidatesQ.Count is 0);
            for (int i = 0; i < candidates.Count; i++)
            {
                long nodeId = candidates[i];
                ref var dst = ref GetNodeById(nodeId); 
                var distance = Distance(src.VectorId, dst.VectorId);
                _candidatesQ.Enqueue(candidates[i], distance);
            }

            candidates.Clear();
            
            while (candidates.Count < Options.NumberOfNeighbors &&
                   _candidatesQ.TryDequeue(out var cur, out var distance))
            {
                bool match = true;
                ref var node = ref GetNodeById(cur);
                for (int i = 0; i < candidates.Count; i++)
                {
                    long nodeId = candidates[i];
                    ref var alternative = ref GetNodeById(nodeId);
                    var curDist = Distance(node.VectorId, alternative.VectorId);
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
        }


        public void NearestNeighbors(long startingPoint, int level, int numberOfCandidates, Span<byte> vector, ref NativeList<long> levelNeighbors, bool startingPointAsNeibhbor)
        {
            Debug.Assert(_candidatesQ.Count is 0 && _nearestNeighborsQ.Count is 0);

            ref var startingPointNode = ref GetNodeById(startingPoint);
            float negatedDist = -Distance(vector, startingPointNode.VectorId);
            var visitedCounter = ++_visitsCounter;
            startingPointNode.Visited = visitedCounter;
            _candidatesQ.Enqueue(startingPoint, negatedDist);
            if (startingPointAsNeibhbor)
            {
                _nearestNeighborsQ.Enqueue(startingPoint, negatedDist);
            }


            while (_candidatesQ.TryDequeue(out var cur, out _))
            {
                ref var candidate = ref GetNodeById(cur);
                ref var neighbors = ref candidate.NeighborsPerLevel[level];
                for (int i = 0; i < neighbors.Count; i++)
                {
                    long nextId = neighbors[i];
                    ref var next = ref GetNodeById(nextId);
                    if(next.Visited == visitedCounter)
                        continue;
            
                    next.Visited = visitedCounter;
                    
                    float nextDist = -Distance(vector, next.VectorId);

                    if (_nearestNeighborsQ.Count < numberOfCandidates)
                    {
                        _candidatesQ.Enqueue(nextId, nextDist);
                        _nearestNeighborsQ.Enqueue(nextId, nextDist);
                        continue;
                    }

                    if (_nearestNeighborsQ.EnqueueDequeue(nextId, nextDist) != nextId)
                    {
                        _candidatesQ.Enqueue(nextId, nextDist);
                    }
                }
            }

            levelNeighbors.EnsureCapacityFor(Llt.Allocator, _nearestNeighborsQ.Count);
            while (_nearestNeighborsQ.TryDequeue(out var neighborId, out _))
            {
                levelNeighbors.AddUnsafe(neighborId);
            }
        }

        
        public void SearchNearestAcrossLevels(Span<byte> vector, int maxLevel, ref NativeList<long> nearest)
        {
            var visitCounter = ++_visitsCounter;
            var currentNodeId = EntryPointId;
            ref var currentNode = ref GetNodeById(EntryPointId);
            var distance = Distance(vector, currentNode.VectorId);
            var level = maxLevel;
            currentNode.NeighborsPerLevel.SetCapacity(Llt.Allocator, maxLevel + 1);
            while (level >= 0)
            {
                bool moved = false;
                var neighbors = currentNode.NeighborsPerLevel[level];
                int count = neighbors.Count;
                for (var i = 0; i < count; i++)
                {
                    long nodeId = neighbors[i];
                    ref var cur = ref GetNodeById(nodeId);
                    if(cur.Visited == visitCounter)
                        continue; // already checked it
                    cur.Visited = visitCounter;
                    var curDist = Distance(vector, cur.VectorId);
                    if (curDist >= distance) 
                        continue;
                    
                    moved = true;
                    distance = curDist;
                    currentNode = ref cur;
                    currentNodeId = nodeId;
                }

                if (moved)
                    continue;
                
                nearest.AddUnsafe(currentNodeId);
                level--;
            }
            
            nearest.Reverse();
        }

        public bool VectorByHash(ReadOnlySpan<byte> vectorHash, out long postingListId)
        {
            return _vectorsByHash.TryGetValue(vectorHash, out postingListId);
        }
    }
    
    public class Registration : IDisposable
    {
        private readonly Dictionary<ByteString, (long NodeId, List<long> PostingList)> _vectorHashCache = new(ByteStringContentComparer.Instance);
        private readonly long _rangeStart;
        
        private SearchState _searchState;

        public Registration(LowLevelTransaction llt, long graphId)
        {
            _searchState = new SearchState(llt, graphId);
            _rangeStart = _searchState.Options.CountOfVectors + 1;
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
            if (_searchState.VectorByHash(vectorHash, out long nodeId))
            {
                postingList = (nodeId,[entryId]);
                return;
            }

            var vectorId = Container.Allocate(_searchState.Llt, _searchState.Options.Container, vector.Length, out var vectorStorage);
            vector.CopyTo(vectorStorage);

            long newNodeId = ++_searchState.Options.CountOfVectors;
            _searchState.RegisterVectorNode(newNodeId, vectorId, vectorHash);
            postingList = (newNodeId,[entryId]);
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
            while ((Random.Shared.Next() & 1) == 0 && // 50% chance 
                   level < maxLevel)
            {
                level++;
            }
            return level;
        }

        public void Dispose()
        {
            using var pforDecoder = new FastPForDecoder(_searchState.Llt.Allocator);
            using var pforEncoder = new FastPForEncoder(_searchState.Llt.Allocator);
            
            var listBuffer = new ContextBoundNativeList<long>(_searchState.Llt.Allocator);
            var byteBuffer = new ContextBoundNativeList<byte>(_searchState.Llt.Allocator);
            byteBuffer.EnsureCapacityFor(128);
            
            foreach (var (_, (nodeId, modifications)) in _vectorHashCache)
            {
                ref var node = ref _searchState.GetNodeById(nodeId);
                node.PostingListId = MergePostingList(node.PostingListId, modifications);
            }

            InsertVectorsToGraph();
            
            foreach (var nodeId in _searchState.Keys)
            {
                ref var node = ref _searchState.GetNode(nodeId);
                for (int i = 0; i < node.NeighborsPerLevel.Count; i++)
                {
                    ref var list = ref node.NeighborsPerLevel[i];
                    if(list.Count < _searchState.Options.NumberOfNeighbors)
                        continue;
                    _searchState.FilterNeighborsHeuristic(ref node, ref list);
                }
                PersistNode(ref node, nodeId);
            }

            // flush the local modifications
            MemoryMarshal.AsRef<Options>(Container.GetMutable(_searchState.Llt, _searchState.GraphId)) = _searchState.Options;
            
            listBuffer.Dispose();
            byteBuffer.Dispose();

            void PersistNode(ref Node node, long nodeId)
            {
                var encoded = node.Encode(_searchState.Llt.Allocator, ref byteBuffer);
                if (_searchState.TryGetLocationForNode(nodeId, out var locationId))
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
                _searchState.RegisterNodeLocation(nodeId, locationId);
                encoded.CopyTo(storage);
            }
            
            
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

            void InsertVectorsToGraph()
            {
                if (_searchState.TryGetLocationForNode(EntryPointId, out var entryPointNode) is false)
                {
                    ref Node startingNode = ref _searchState.GetNode(EntryPointId);
                    Span<byte> span = startingNode.Encode(_searchState.Llt.Allocator, ref byteBuffer);
                    entryPointNode = Container.Allocate(_searchState.Llt, _searchState.Options.Container, span.Length, out Span<byte> allocated);
                    span.CopyTo(allocated);
                    _searchState.RegisterNodeLocation(EntryPointId, entryPointNode);
                }

                int maxLevel = _searchState.Options.MaxLevel;

                var nearestNodesByLevel = new NativeList<long>();
                nearestNodesByLevel.EnsureCapacityFor(_searchState.Llt.Allocator, maxLevel + 1);

                int createdNodesCount = _searchState.CreatedNodesCount;
                for (int j = 0; j < createdNodesCount; j++)
                {
                    nearestNodesByLevel.Clear();

                    long nodeId = _rangeStart + j;
                    ref var node = ref _searchState.GetNode(nodeId);
                    
                    var vector = Container.Get(_searchState.Llt, node.VectorId).ToSpan();

                    _searchState.SearchNearestAcrossLevels(vector, maxLevel, ref nearestNodesByLevel);

                    int nodeRandomLevel = GetLevelForNewNode(maxLevel);
                    node.NeighborsPerLevel.SetCapacity(_searchState.Llt.Allocator, nodeRandomLevel + 1);
                
                    for (int level = nodeRandomLevel; level >= 0; level--)
                    {
                        long startingPointPerLevel = nearestNodesByLevel[level];
                        _searchState.NearestNeighbors(startingPointPerLevel, level, 
                            _searchState.Options.NumberOfCandidates, 
                            vector, 
                            ref node.NeighborsPerLevel[level],
                            startingPointAsNeibhbor: nodeId != startingPointPerLevel);

                        ref var list = ref node.NeighborsPerLevel[level];
                        var startingCount = list.Count;
                        for (var i = 0; i < startingCount; i++)
                        {
                            long neighborId = list[i];
                            ref var neighbor = ref _searchState.GetNodeById(neighborId);
                            Debug.Assert(neighborId != nodeId, "neighbor.NodeId != node.NodeId");
                            ref var neighborList = ref neighbor.NeighborsPerLevel[level];
                            neighborList.Add(_searchState.Llt.Allocator, nodeId);
                        }
                    }
                }
            }
        }
    }

    public static Registration RegistrationFor(LowLevelTransaction llt, long id)
    {
        return new Registration(llt, id);
    }

    public static Options ReadOptions(LowLevelTransaction llt, long id)
    {
        var item = Container.Get(llt, id);
        return MemoryMarshal.Read<Options>(item.ToSpan());
    }

    public static NearestSearch Nearest(LowLevelTransaction llt, long graphId, int numberOfCandidates, Span<byte> vector)
    {
        var searchState = new SearchState(llt, graphId);
        var nearestNodesByLevel = new ContextBoundNativeList<long>(llt.Allocator);
        nearestNodesByLevel.EnsureCapacityFor(searchState.Options.MaxLevel + 1);

        if (searchState.Options.CountOfVectors == 0)
            return new NearestSearch(searchState, nearestNodesByLevel);
        
        searchState.SearchNearestAcrossLevels(vector, searchState.Options.MaxLevel, ref nearestNodesByLevel.Inner);
        var nearest = nearestNodesByLevel[0];
        nearestNodesByLevel.Clear();

        searchState.NearestNeighbors(nearest, level: 0, numberOfCandidates, vector, ref nearestNodesByLevel.Inner, startingPointAsNeibhbor: true);
        nearestNodesByLevel.Inner.Reverse();
        return new NearestSearch(searchState, nearestNodesByLevel);
    }

    public struct NearestSearch(SearchState searchState,ContextBoundNativeList<long> nodes) : IDisposable
    {
        private int _currentNode, _currentMatchesIndex;
        private ContextBoundNativeList<long> _postingListResults = new(searchState.Llt.Allocator);
        private FastPForDecoder _pforDecoder = new(searchState.Llt.Allocator);

        public void Dispose()
        {
            nodes.Dispose();
            _postingListResults.Dispose();
            _pforDecoder.Dispose();
        }

        [SkipLocalsInit]
        public int Fill(Span<long> matches)
        {
            int index = 0;
            Span<byte> buffer = stackalloc byte[Sodium.GenericHashSize];
            while (index < matches.Length)
            {
                if (_currentNode >= nodes.Count)
                    break;

                if (_currentMatchesIndex < _postingListResults.Count)
                {
                    var copy = Math.Min(_postingListResults.Count - _currentMatchesIndex, matches.Length - index);
                    _postingListResults.CopyTo(matches[index..], _currentMatchesIndex, copy);
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

                var nodeId = nodes[_currentNode];
                ref var node = ref searchState.GetNodeById(nodeId);
                var rawPostingListId = node.PostingListId & ~0b11;
                switch (node.PostingListId & 0b11)
                {
                    case 0b00: // empty
                        _currentNode++;
                        continue;
                    case 0b01: // single item posting list
                        matches[index++] = rawPostingListId;
                        _currentNode++;
                        continue;
                    case 0b10: // small posting list
                        Debug.Assert(_postingListResults.Count is 0 && _currentMatchesIndex is 0);
                        searchState.ReadPostingList(rawPostingListId, ref _postingListResults, _pforDecoder, out _);
                        continue;
                    case 0b11: // large posting list
                        break;
                }
                throw new NotSupportedException();
            }
            return index;
        }
    }

}

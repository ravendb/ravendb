using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Server.Collections;
using Voron.Global;
using Voron.Util;

namespace Voron.Data.Graphs;

public unsafe partial class Hnsw
{
    public partial class SearchState
    {
        private class NearestSearcher : IHnswSearcher
        {
            private readonly SearchState _searchState;
            private readonly Memory<byte> _vector;
            private readonly int _level;
            private readonly int _numberOfCandidates;

            // The working set size: the requested candidates, and with a filter, grown before every walk.
            private int _internalNumberOfCandidates;

            private ContextBoundNativeList<int> _candidates;
            private readonly NearestEdgesFlags _flags;
            private NativeList<int> _indexes;
            private NativeList<long> _nodeIds;
            private long _vectorReadCounter = 0;
            private readonly int _startingPointIndex;
            // With a filter, the results are the nearest of the filter's nodes among every vector the search read.
            private readonly GrowableBitArray? _filter;
            private readonly long _readLimit;
            // What a filtered search knows about each node index.
            private NativeList<NodeState> _nodeStates;

            [Flags]
            private enum NodeState : byte
            {
                // In the working set of an earlier walk, so a later walk keeps it out of its working set.
                Walked = 1,
                // Checking the filter can decode the posting list of the node, so the search checks it once.
                FilterChecked = 2,
                InFilter = 4
            }

            public long CandidatesProcessed => _vectorReadCounter;

            public NearestSearcher(SearchState searchState, int startingPointIndex, Memory<byte> vector,
                int level, int numberOfCandidates,
                ContextBoundNativeList<int> candidates,
                NearestEdgesFlags flags,
                GrowableBitArray? filter)
            {
                _searchState = searchState;
                _startingPointIndex = startingPointIndex;
                _vector = vector;
                _level = level;
                _numberOfCandidates = numberOfCandidates;
                _internalNumberOfCandidates = numberOfCandidates;
                _filter = filter;
                if (filter.HasValue)
                {
                    searchState._nearestInFilterQ ??= new();
                    var filterCount = filter.Value.Count;
                    _readLimit = filterCount < 1024 ? 2 * filterCount : Math.Min(filterCount / 2, searchState.Options.CountOfVectors / 5);
                    _internalNumberOfCandidates = GrowWithinBudget(numberOfCandidates);
                    _nodeStates.SetCapacity(searchState.Llt.Allocator, Math.Max(1024, searchState.Nodes.Length * 2));
                }

                _candidates = candidates;
                _flags = flags;
                _indexes = new NativeList<int>();
                _nodeIds = new NativeList<long>();
            }

            // A walk could not fill a working set larger than the reads left. The working set grows by at least one, because a walk that reads no
            // new vector with the same working set would repeat the last one.
            private int GrowWithinBudget(int size)
            {
                long minNextSize = Math.Min(int.MaxValue, size + 1L);
                long maxNextSize = Math.Max(minNextSize, Math.Min(int.MaxValue, _readLimit - _vectorReadCounter));

                return (int)Math.Clamp(size + size / 2L, minNextSize, maxNextSize);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private ref NodeState StateOf(int nodeIndex)
            {
                if (nodeIndex >= _nodeStates.Count)
                    _nodeStates.SetCapacity(_searchState.Llt.Allocator, Math.Max(nodeIndex + 1, _nodeStates.Count * 2));
                return ref _nodeStates[nodeIndex];
            }

            public ContextBoundNativeList<int> Search()
            {
                var nearestEdgesQ = _searchState._nearestEdgesQ;
                if (_filter.HasValue == false)
                {
                    Walk();
                    ProcessResults(nearestEdgesQ);
                    return _candidates;
                }

                // A node is considered as a result when the search first reads its vector, so the search starts with no distance that an earlier search cached.
                ++_searchState._queryVectorVersion;
                var held = 0;
                while (true)
                {
                    var ranOutOfNodesToExpand = Walk() == false;
                    foreach (var (nodeIndex, _) in nearestEdgesQ.UnorderedItems)
                    {
                        ref var state = ref StateOf(nodeIndex);
                        if ((state & NodeState.Walked) == 0 && IsInFilter(ref state, _searchState.GetNodeByIndex(nodeIndex).PostingListId))
                            held++;
                    }

                    if (ranOutOfNodesToExpand || held >= _numberOfCandidates || _vectorReadCounter >= _readLimit)
                        break;

                    foreach (var (nodeIndex, _) in nearestEdgesQ.UnorderedItems)
                        StateOf(nodeIndex) |= NodeState.Walked;
                    nearestEdgesQ.Clear();
                    _internalNumberOfCandidates = GrowWithinBudget(_internalNumberOfCandidates);
                }

                nearestEdgesQ.Clear();
                ProcessResults(_searchState._nearestInFilterQ);
                return _candidates;
            }

            // A walk from the starting point that ends once the nearest unexpanded node is farther than a full working set's farthest node, and returns
            // false when it ran out of nodes to expand first.
            private bool Walk()
            {
                var candidatesQ = _searchState._candidatesQ;
                var nearestEdgesQ = _searchState._nearestEdgesQ;

                Debug.Assert(candidatesQ.Count == 0, "_candidatesQ.Count == 0");
                Debug.Assert(nearestEdgesQ.Count == 0, "_nearestEdgesQ.Count == 0");

                var visitedCounter = ++_searchState._visitsCounter;
                var filtered = _filter.HasValue;
                _searchState.OnQueryVector(_vector);
                var read = _vectorReadCounter;
                var lowerBound = -_searchState.QueryDistance(_vector.Span, _startingPointIndex, ref _vectorReadCounter);
                ref var startingPoint = ref _searchState.GetNodeByIndex(_startingPointIndex);
                startingPoint.Visited = visitedCounter;
                if (filtered && _vectorReadCounter != read)
                    ConsiderAsResult(ref StateOf(_startingPointIndex), _startingPointIndex, startingPoint.PostingListId, lowerBound);
                // The candidates queue is sorted by distance, so the lowest distance will always pop first.
                // The nearest-edges queue is sorted by reversed distance, so a full queue pops the one with the largest distance.
                candidatesQ.Enqueue(_startingPointIndex, -lowerBound);
                if (_flags.HasFlag(NearestEdgesFlags.StartingPointAsEdge) &&
                    ((startingPoint.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask) != Constants.Graphs.VectorId.Tombstone
                     || _flags.HasFlag(NearestEdgesFlags.FilterNodesWithEmptyPostingLists) is false))
                {
                    nearestEdgesQ.Enqueue(_startingPointIndex, lowerBound);
                }

                while (candidatesQ.TryDequeue(out var cur, out var curDistance))
                {
                    if (-curDistance < lowerBound && nearestEdgesQ.Count == _internalNumberOfCandidates)
                    {
                        // The queues belong to the SearchState, and the next walk on it starts from them empty.
                        candidatesQ.Clear();
                        return true;
                    }

                    _searchState.GetNodeByIndex(cur).Visited = visitedCounter;
                    _searchState.ResolveEdgeIndexes(cur, _level, ref _nodeIds, ref _indexes);

                    // Pin loop locals: NativeList field reads through `this` force the JIT to
                    // reload the storage pointer on every candidate.
                    int indexesCount = _indexes.Count;
                    int* indexesPtr = _indexes.RawItems;
                    for (int i = 0; i < indexesCount; i++)
                    {
                        var nextIndex = indexesPtr[i];
                        ref var next = ref _searchState.GetNodeByIndex(nextIndex);
                        if (next.Visited == visitedCounter)
                            continue; // already checked
                        next.Visited = visitedCounter;

                        // Prefetch the next unvisited neighbor's vector data to overlap
                        // cache-miss latency with the current distance computation.
                        PrefetchNextNeighborVector(indexesPtr, indexesCount, i + 1, visitedCounter);

                        ref var state = ref Unsafe.NullRef<NodeState>();
                        if (filtered)
                            state = ref StateOf(nextIndex);
                        var isDeleted = (next.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask) == Constants.Graphs.VectorId.Tombstone
                                        || (filtered && (state & NodeState.Walked) != 0);

                        read = _vectorReadCounter;
                        float nextDist = -_searchState.QueryDistance(_vector.Span, ref next, ref _vectorReadCounter);
                        if (filtered && _vectorReadCounter != read)
                            ConsiderAsResult(ref state, nextIndex, next.PostingListId, nextDist);

                        if (nearestEdgesQ.Count < _internalNumberOfCandidates)
                        {
                            candidatesQ.Enqueue(nextIndex, -nextDist);

                            if (isDeleted == false || _flags.HasFlag(NearestEdgesFlags.FilterNodesWithEmptyPostingLists) is false)
                            {
                                nearestEdgesQ.Enqueue(nextIndex, nextDist);
                            }
                        }
                        else if (lowerBound < nextDist)
                        {
                            candidatesQ.Enqueue(nextIndex, -nextDist);

                            if (isDeleted == false || _flags.HasFlag(NearestEdgesFlags.FilterNodesWithEmptyPostingLists) is false)
                            {
                                nearestEdgesQ.EnqueueDequeue(nextIndex, nextDist);
                            }
                        }
                        else
                        {
                            continue;
                        }

                        Debug.Assert(candidatesQ.Count > 0);
                        nearestEdgesQ.TryPeek(out _, out lowerBound);
                    }
                }

                return false;
            }

            // Keeps the requested number of the filter's nearest nodes among every vector read, inside or outside the working set. Only a node nearer than
            // the farthest one kept needs the membership check.
            private void ConsiderAsResult(ref NodeState state, int nodeIndex, long postingListId, float negatedDistance)
            {
                var nearestInFilterQ = _searchState._nearestInFilterQ;
                if (nearestInFilterQ.Count == _numberOfCandidates && nearestInFilterQ.TryPeek(out _, out var farthest) && negatedDistance <= farthest)
                    return;

                if (IsInFilter(ref state, postingListId) == false)
                    return;

                if (nearestInFilterQ.Count < _numberOfCandidates)
                    nearestInFilterQ.Enqueue(nodeIndex, negatedDistance);
                else
                    nearestInFilterQ.EnqueueDequeue(nodeIndex, negatedDistance);
            }

            private bool IsInFilter(ref NodeState state, long postingListId)
            {
                if ((state & NodeState.FilterChecked) == 0)
                {
                    state |= NodeState.FilterChecked;
                    if (_searchState.HasEntryInFilter(postingListId, _filter.Value))
                        state |= NodeState.InFilter;
                }

                return (state & NodeState.InFilter) != 0;
            }

            private void ProcessResults(PriorityQueue<int, float> resultsQ)
            {
                _candidates.Clear();
                _candidates.EnsureCapacityFor(resultsQ.Count);

                while (resultsQ.TryDequeue(out var edgeId, out _))
                    _candidates.AddUnsafe(edgeId);

                _candidates.Inner.Reverse();
            }

            /// <summary>
            /// Finds the next unvisited neighbor starting from <paramref name="startFrom"/> and
            /// issues software prefetch instructions for its vector data. This overlaps the
            /// cache-miss latency (~500ns for 6KB from L3) with the current distance computation (~633ns).
            /// On platforms without software prefetch the JIT eliminates this entirely.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void PrefetchNextNeighborVector(int* indexesPtr, int indexesCount, int startFrom, int visitedCounter)
            {
                if (PortableIntrinsics.CanPrefetch == false)
                    return;

                for (int j = startFrom; j < indexesCount; j++)
                {
                    var idx = indexesPtr[j];
                    ref var node = ref _searchState.GetNodeByIndex(idx);
                    if (node.Visited == visitedCounter)
                        continue;

                    if (node.TryGetVectorAddress(out byte* address, out int length) == false)
                        return; // vector not loaded yet, skip prefetch

                    PortableIntrinsics.PrefetchRange(address, length);
                    return;
                }
            }

            public void Dispose()
            {
                _candidates.Dispose();
                _nodeIds.Dispose(_searchState.Llt.Allocator);
                _indexes.Dispose(_searchState.Llt.Allocator);
                // A search that throws leaves its queues filled, and the next search on the SearchState expects them empty.
                _searchState._candidatesQ.Clear();
                _searchState._nearestEdgesQ.Clear();
                _searchState._nearestInFilterQ?.Clear();
                _nodeStates.Dispose(_searchState.Llt.Allocator);
            }
        }
    }
}

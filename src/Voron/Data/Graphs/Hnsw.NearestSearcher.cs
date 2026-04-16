using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
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

            // Actual number of candidates currently being processed. It may differ from the caller's NumberOfCandidates
            // because we may need to over-fetch to satisfy the filter clause.
            private int _internalNumberOfCandidates;

            private ContextBoundNativeList<int> _candidates;
            private readonly NearestEdgesFlags _flags;
            private readonly bool _hasFilterMatch;
            private NativeList<int> _indexes;
            private NativeList<long> _nodeIds;
            private readonly HashSet<int> _alreadyReturnedEdges;
            private long _vectorReadCounter = 0;
            private readonly int _startingPointIndex;
            private ContextBoundNativeList<int> _startingPointIndexes;
            
            public long CandidatesProcessed { get => _vectorReadCounter; }
            public int NumberOfCandidates { get; init; }

            private NearestSearcher(SearchState searchState, Memory<byte> vector,
                int level, int numberOfCandidates,
                ContextBoundNativeList<int> candidates,
                NearestEdgesFlags flags,
                bool hasFilterMatch)
            {
                _searchState = searchState;
                _vector = vector;
                _level = level;
                NumberOfCandidates = numberOfCandidates;

                _internalNumberOfCandidates = hasFilterMatch == false
                    ? numberOfCandidates
                    : numberOfCandidates + GetPrefetchExtendSize(numberOfCandidates);
                _candidates = candidates;
                _flags = flags;
                _hasFilterMatch = hasFilterMatch;
                _indexes = new NativeList<int>();
                _nodeIds = new NativeList<long>();
                if (_hasFilterMatch)
                {
                    _alreadyReturnedEdges = new();
                }
            }
            
            
            public NearestSearcher(SearchState searchState, ContextBoundNativeList<int> startingPointsIndexes, Memory<byte> vector, int level, int numberOfCandidates, ContextBoundNativeList<int> candidates, NearestEdgesFlags flags) : this(searchState, vector, level, numberOfCandidates, candidates, flags, true)
            {
                _startingPointIndexes = startingPointsIndexes;
                _startingPointIndex = -1;
            }

            public NearestSearcher(SearchState searchState, int startingPointIndex, Memory<byte> vector,
                int level, int numberOfCandidates,
                ContextBoundNativeList<int> candidates,
                NearestEdgesFlags flags,
                bool hasFilterMatch) : this(searchState, vector, level, numberOfCandidates, candidates, flags, hasFilterMatch)
            {
                _startingPointIndex = startingPointIndex;
            }

            private void InitState(out float lowerBound, out int visitedCounter)
            {
                if (_startingPointIndex != -1)
                {
                    Deepest(out lowerBound, out visitedCounter);
                }
                else
                {
                    Multi(out lowerBound, out visitedCounter);
                }
            }

            private void Multi(out float lowerBound, out int visitedCounter)
            {
                var candidatesQ = _searchState._candidatesQ;
                var nearestEdgesQ = _searchState._nearestEdgesQ;

                Debug.Assert(candidatesQ.Count == 0, "_candidatesQ.Count == 0");
                Debug.Assert(nearestEdgesQ.Count == 0, "_nearestEdgesQ.Count == 0");

                lowerBound = float.MaxValue;
                visitedCounter = ++_searchState._visitsCounter;
                _searchState.OnQueryVector(_vector);

                foreach (var nodeIdx in _startingPointIndexes)
                {
                    ref var startingPoint = ref _searchState.GetNodeByIndex(nodeIdx);
                    startingPoint.Visited = visitedCounter;
                    var currentDistance = -_searchState.QueryDistance(_vector.Span, nodeIdx, ref _vectorReadCounter);
                    lowerBound = Math.Min(lowerBound, currentDistance);
                    candidatesQ.Enqueue(nodeIdx, -currentDistance);
                    if (_flags.HasFlag(NearestEdgesFlags.StartingPointAsEdge) &&
                        ((startingPoint.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask) != Constants.Graphs.VectorId.Tombstone
                         || _flags.HasFlag(NearestEdgesFlags.FilterNodesWithEmptyPostingLists) is false))
                    {
                        nearestEdgesQ.Enqueue(nodeIdx, currentDistance);
                    }
                }
            }

            private void Deepest(out float lowerBound, out int visitedCounter)
            {
                var candidatesQ = _searchState._candidatesQ;
                var nearestEdgesQ = _searchState._nearestEdgesQ;
                var allocator = _searchState.Llt.Allocator;

                Debug.Assert(candidatesQ.Count == 0, "_candidatesQ.Count == 0");
                Debug.Assert(nearestEdgesQ.Count == 0, "_nearestEdgesQ.Count == 0");

                visitedCounter = ++_searchState._visitsCounter;
                _searchState.OnQueryVector(_vector);
                lowerBound = -_searchState.QueryDistance(_vector.Span, _startingPointIndex, ref _vectorReadCounter);
                {
                    ref var startingPoint = ref _searchState.GetNodeByIndex(_startingPointIndex);
                    startingPoint.Visited = visitedCounter;
                    // The candidates queue is sorted by distance, so the lowest distance
                    // will always pop first.
                    // The nearest-edges queue is sorted by reversed distance, so when we add a
                    // new item to the queue, we'll pop the one with the largest distance.

                    candidatesQ.Enqueue(_startingPointIndex, -lowerBound);
                    if (_flags.HasFlag(NearestEdgesFlags.StartingPointAsEdge) &&
                        ((startingPoint.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask) != Constants.Graphs.VectorId.Tombstone
                         || _flags.HasFlag(NearestEdgesFlags.FilterNodesWithEmptyPostingLists) is false))
                    {
                        nearestEdgesQ.Enqueue(_startingPointIndex, lowerBound);
                    }
                }
            }

            public IEnumerable<bool> Search()
            {
                Start:
                var candidatesQ = _searchState._candidatesQ;
                var nearestEdgesQ = _searchState._nearestEdgesQ;
                var allocator = _searchState.Llt.Allocator;

                Debug.Assert(candidatesQ.Count == 0, "_candidatesQ.Count == 0");
                Debug.Assert(nearestEdgesQ.Count == 0, "_nearestEdgesQ.Count == 0");
                InitState(out float lowerBound, out int visitedCounter);

                while (candidatesQ.TryDequeue(out var cur, out var curDistance))
                {
                    if (-curDistance < lowerBound &&
                        nearestEdgesQ.Count == _internalNumberOfCandidates)
                    {
                        ProcessResults();
                        // Drain candidatesQ before yielding. Non-filtered queries consume only
                        // the first batch and never resume the iterator, so without this clear
                        // the shared SearchState carries far-from-the-old-vector candidates into
                        // the next query, polluting its priority queue and forcing the search to
                        // expand 25-30% more nodes than a fresh state would.
                        candidatesQ.Clear();
                        yield return true;
                        // If we need to fetch more, we'll start the query again with a higher NumberOfCandidates.
                        // The SearchState keeps its state, so traversal through already visited nodes is I/O-free
                        // because we keep distances in memory from the previous run.
                        // This method can be greedy enough to traverse the entire graph, so it's the caller's
                        // responsibility to enforce a stop condition.
                        goto Start;
                    }

                    ref var candidate = ref _searchState.GetNodeByIndex(cur);
                    candidate.Visited = visitedCounter;

                    ref var edges = ref candidate.EdgesPerLevel[_level];

                    _nodeIds.ResetAndCopyFrom(allocator, edges.ToSpan());
                    _searchState.LoadNodeIndexes(ref _nodeIds, ref _indexes);

                    for (int i = 0; i < _indexes.Count; i++)
                    {
                        var nextIndex = _indexes[i];
                        ref var next = ref _searchState.GetNodeByIndex(nextIndex);
                        if (next.Visited == visitedCounter)
                            continue; // already checked
                        next.Visited = visitedCounter;

                        // Prefetch the next unvisited neighbor's vector data to overlap
                        // cache-miss latency with the current distance computation.
                        // SimilarityCalc is ~65% of query time, ~80% of which is data loading.
                        PrefetchNextNeighborVector(i + 1, visitedCounter);

                        var isDeleted = (next.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask) == Constants.Graphs.VectorId.Tombstone
                                        || (_hasFilterMatch && _alreadyReturnedEdges.Contains(nextIndex));

                        float nextDist = -_searchState.QueryDistance(_vector.Span, nextIndex, ref _vectorReadCounter);
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

                // Nothing more to visit. Move the current results into the candidates list and finish.
                ProcessResults();
                candidatesQ.Clear();
                yield return _candidates.Count > 0;
            }

            public bool TryGetCurrentCandidates(out ContextBoundNativeList<int> candidates)
            {
                candidates = _candidates;
                return candidates.Count > 0;
            }

            public void IncreaseNumberOfCandidates(int currentlyAcceptedNodes)
            {
                Reset();
                _internalNumberOfCandidates += GetPrefetchExtendSize(_internalNumberOfCandidates);
            }

            // Reset the NearestSearcher state; however, it does not clear the data already stored inside SearchState.
            // This is important because it allows us to reduce I/O pressure during over-fetching and when restarting the query.
            // Node.Visited uses version-based invalidation keyed on SearchState._visitsCounter
            // (incremented in InitState), and Node.QueryDistanceVersion uses a separate version
            // keyed on the query vector, so neither needs a per-node reset here.
            private void Reset()
            {
                _searchState._candidatesQ.Clear();
                _searchState._nearestEdgesQ.Clear();
                _candidates.Clear();
                _nodeIds.Clear();
                _indexes.Clear();
            }

            private void ProcessResults()
            {
                _candidates.Clear();
                _candidates.EnsureCapacityFor(_searchState._nearestEdgesQ.Count);

                while (_searchState._nearestEdgesQ.TryDequeue(out var edgeId, out var d))
                {
                    if (_hasFilterMatch && _alreadyReturnedEdges!.Add(edgeId) == false)
                        continue;

                    _candidates.AddUnsafe(edgeId);
                }

                _candidates.Inner.Reverse();
            }
            
            public bool ShouldContinueSearch(long filterDocsCount)
            {
                if (_hasFilterMatch == false)
                    return false;

                var max = filterDocsCount switch
                {
                    < 1024 => 2 * filterDocsCount, // It's hard to determine right number here. For smaller graphs it may be an issue.
                    // However, for small filter set (for 1K we will force exact search anyway, therefore, this probably will be used only for testing purposes.
                    _ => Math.Min(filterDocsCount / 2, _searchState.Options.CountOfVectors / 5)
                };
                
                return _vectorReadCounter < max;
            }

            /// <summary>
            /// Finds the next unvisited neighbor starting from <paramref name="startFrom"/> and
            /// issues software prefetch instructions for its vector data. This overlaps the
            /// cache-miss latency (~500ns for 6KB from L3) with the current distance computation (~633ns).
            /// On platforms without software prefetch the JIT eliminates this entirely.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void PrefetchNextNeighborVector(int startFrom, int visitedCounter)
            {
                if (PortableIntrinsics.CanPrefetch == false)
                    return;

                for (int j = startFrom; j < _indexes.Count; j++)
                {
                    var idx = _indexes[j];
                    ref var node = ref _searchState.GetNodeByIndex(idx);
                    if (node.Visited == visitedCounter)
                        continue;

                    if (node.TryGetVectorAddress(out byte* address, out int length) == false)
                        return; // vector not loaded yet, skip prefetch

                    PortableIntrinsics.PrefetchRange(address, length);
                    return;
                }
            }

            private static int GetPrefetchExtendSize(int numberOfCandidates) => numberOfCandidates switch
            {
                <= 131_072 =>  numberOfCandidates / 2,
                _ => (int)Math.Sqrt(131_072L * numberOfCandidates)
            };

            public void Dispose()
            {
                if (_startingPointIndex == -1)
                    _startingPointIndexes.Dispose();
                _candidates.Dispose();
                _nodeIds.Dispose(_searchState.Llt.Allocator);
                _indexes.Dispose(_searchState.Llt.Allocator);
                _searchState._candidatesQ.Clear();
            }
        }
    }
}

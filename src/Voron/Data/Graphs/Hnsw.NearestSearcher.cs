using System;
using System.Collections.Generic;
using System.Diagnostics;
using Voron.Global;
using Voron.Util;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    public partial class SearchState
    {
        private class NearestSearcher : IHnswSearcher
        {
            private readonly SearchState _searchState;
            private readonly int _startingPointIndex;
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
            private readonly HashSet<int> _candidatesProcessed;

            public long CandidatesProcessed { get => _candidatesProcessed?.Count ?? 0; }
            
            public int NumberOfCandidates { get; init; }

            public NearestSearcher(SearchState searchState, int startingPointIndex, Memory<byte> vector,
                int level, int numberOfCandidates,
                ContextBoundNativeList<int> candidates,
                NearestEdgesFlags flags,
                bool hasFilterMatch)
            {
                _searchState = searchState;
                _startingPointIndex = startingPointIndex;
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
                    _candidatesProcessed = new();
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

                float lowerBound = -_searchState.QueryDistance(_vector.Span, _startingPointIndex);
                var visitedCounter = ++_searchState._visitsCounter;
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

                while (candidatesQ.TryDequeue(out var cur, out var curDistance))
                {
                    _candidatesProcessed?.Add(cur);
                    if (-curDistance < lowerBound &&
                        nearestEdgesQ.Count == _internalNumberOfCandidates)
                    {
                        ProcessResults();
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

                        var isDeleted = (next.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask) == Constants.Graphs.VectorId.Tombstone
                                        || _hasFilterMatch && _alreadyReturnedEdges.Contains(nextIndex);

                        float nextDist = -_searchState.QueryDistance(_vector.Span, nextIndex);
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
            private void Reset()
            {
                _searchState._candidatesQ.Clear();
                _searchState._nearestEdgesQ.Clear();

                for (int nodeIdx = 0; nodeIdx < _searchState._nodes.Count; nodeIdx++)
                {
                    _searchState._nodes[nodeIdx].Visited = 0;
                }

                _searchState._visitsCounter = 0;
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
                    // When filtering is enabled, avoid returning the same edge more than once.
                    Debug.Assert(_hasFilterMatch == false || _alreadyReturnedEdges!.Contains(edgeId) == false);

                    _alreadyReturnedEdges?.Add(edgeId);
                    _candidates.AddUnsafe(edgeId);
                }

                _candidates.Inner.Reverse();
            }
            
            public bool ShouldContinueSearch(long filterDocsCount)
            {
                if (_hasFilterMatch == false)
                    return false;

                const int minCount = 1_048_576;
                const int maxCount = 8_388_608;
                const float maxFactor = 4;
                const float minFactor = 1.5f;
                
                switch (filterDocsCount)
                {
                    case <= minCount:
                        return filterDocsCount * maxFactor < CandidatesProcessed;
                    case >= maxCount:
                        return filterDocsCount * minFactor < CandidatesProcessed;
                }


                var normalized = (filterDocsCount - minCount) / (float)(maxCount - minCount);
                var t = (MathF.Cos(normalized * MathF.PI) + 1.0f) / 2f;
                var factor = minFactor + (maxFactor - minFactor) * t;
                return filterDocsCount * factor < CandidatesProcessed;
            }

            private static int GetPrefetchExtendSize(int numberOfCandidates) => numberOfCandidates switch
            {
                <= 131_072 => numberOfCandidates,
                _ => (int)Math.Sqrt(131_072L * numberOfCandidates)
            };

            public void Dispose()
            {
                _candidates.Dispose();
                _nodeIds.Dispose(_searchState.Llt.Allocator);
                _indexes.Dispose(_searchState.Llt.Allocator);
                _searchState._candidatesQ.Clear();
            }
        }
    }
}

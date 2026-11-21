using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Sparrow;
using Voron.Global;
using Voron.Util;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    public partial class SearchState
    {
        private class ExactSearcher : IHnswSearcher
        {
            private readonly SearchState _searchState;
            private readonly Memory<byte> _vector;
            private ContextBoundNativeList<int> _candidates;
            private ContextBoundNativeList<float> _distances;
            private readonly ContextBoundNativeList<long>? _nodesToScan;
            private PriorityQueue<long, float> _pq;
            private readonly int _numberOfCandidates;
            private long _vectorReadCounter = 0;

            public ExactSearcher(SearchState searchState, Memory<byte> vector, int numberOfCandidates, ContextBoundNativeList<long>? nodesToScan)
            {
                _searchState = searchState;
                _vector = vector;
                _numberOfCandidates = numberOfCandidates;
                _nodesToScan = nodesToScan;
                _candidates = new ContextBoundNativeList<int>(searchState.Llt.Allocator);
                _distances = new ContextBoundNativeList<float>(searchState.Llt.Allocator);
            }

            public void Dispose()
            {
                _searchState._candidatesQ.Clear();
                _searchState._nearestEdgesQ.Clear();
                _candidates.Dispose();
                _distances.Dispose();
            }

            public (ContextBoundNativeList<int> Results, ContextBoundNativeList<float> Distances) Search()
            {
                // Reset the visited set for this traversal and record the query vector so the
                // per-node QueryDistance cache is either reused (same vector) or invalidated
                // (different vector) — OnQueryVector decides based on Memory identity.
                ++_searchState._visitsCounter;
                _searchState.OnQueryVector(_vector);

                _pq = new PriorityQueue<long, float>();
                if (_nodesToScan.HasValue)
                {
                    foreach (long nodeId in _nodesToScan.Value.Iterate())
                        ScanNode(nodeId);
                }
                else
                {
                    long count = _searchState.Options.CountOfVectors;
                    for (long nodeId = 1; nodeId <= count; nodeId++)
                        ScanNode(nodeId);
                }

                _candidates.EnsureCapacityFor(_pq.Count);
                _distances.EnsureCapacityFor(_pq.Count);
                while (_pq.TryDequeue(out var nodeId, out var negatedDistance))
                {
                    _candidates.AddUnsafe(_searchState.GetNodeIndexById(nodeId));
                    _distances.AddUnsafe(-negatedDistance);
                }

                _candidates.Inner.Reverse();
                _distances.Inner.Reverse();
                return (_candidates, _distances);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void ScanNode(long nodeId)
            {
                CandidatesProcessed++;
                var nodeIndex = _searchState.GetNodeIndexById(nodeId);
                if ((_searchState.GetNodeByIndex(nodeIndex).PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask) == Constants.Graphs.VectorId.Tombstone)
                    return;

                var distance = _searchState.QueryDistance(_vector.Span, nodeIndex, ref _vectorReadCounter);
                // The queue keeps the nearest candidates with the farthest on top.
                if (_pq.Count < _numberOfCandidates)
                    _pq.Enqueue(nodeId, -distance);
                else
                    _pq.EnqueueDequeue(nodeId, -distance);
            }

            public long CandidatesProcessed { get; private set; }
        }
    }
}

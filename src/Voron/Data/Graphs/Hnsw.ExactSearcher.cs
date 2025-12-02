using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Sparrow;
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
            private readonly bool _hasFilterMatch;
            private ContextBoundNativeList<int> _candidates;
            private readonly ContextBoundNativeList<long>? _nodesToScan;

            public ExactSearcher(SearchState searchState, Memory<byte> vector, bool hasFilterMatch, int numberOfCandidates, ContextBoundNativeList<long>? nodesToScan)
            {
                _searchState = searchState;
                _vector = vector;
                _hasFilterMatch = hasFilterMatch;
                //TODO: more than int32 vectors
                PortableExceptions.ThrowIf<NotSupportedException>(_searchState.Options.CountOfVectors >= int.MaxValue && hasFilterMatch, "Cannot have more than int32 vectors and filter match");
                
                NumberOfCandidates = numberOfCandidates;
                _nodesToScan = nodesToScan;

                var candidatesCapacity = Math.Min(numberOfCandidates, (int)searchState.Options.CountOfVectors);
                _candidates = new ContextBoundNativeList<int>(searchState.Llt.Allocator, candidatesCapacity);
            }

            public void Dispose()
            {
                _searchState._candidatesQ.Clear();
                _searchState._nearestEdgesQ.Clear();
                _candidates.Dispose();
                _nodesToScan?.Dispose();
            }

            public IEnumerable<bool> Search()
            {
                var pq = new PriorityQueue<long, float>();
                Span<byte> vector = _vector.Span;
                IEnumerable<long> toScan = _nodesToScan.HasValue ? _nodesToScan.Value.Iterate() : AllNodes();
                foreach (long nodeId in toScan)
                {
                    CandidatesProcessed++;
                    unsafe
                    {
                        _searchState.ReadNode(nodeId, out var reader);
                        if (reader.PostingListId is 0)
                            continue; // no entries, can skip

                        var curVect = reader.ReadVector(in _searchState);
                        var distance = _searchState.SimilarityCalc(vector, curVect);
                        if (pq.Count < NumberOfCandidates || _hasFilterMatch)
                        {
                            pq.Enqueue(nodeId, -distance);
                        }
                        else
                        {
                            pq.EnqueueDequeue(nodeId, -distance);
                        }
                    }
                }

                while (pq.Count > 0)
                {
                    _candidates.Count = 0;
                    while (_candidates.Count < NumberOfCandidates && pq.TryDequeue(out var nodeId, out _))
                    {
                        _candidates.AddByRefUnsafe() = _searchState.GetNodeIndexById(nodeId);
                    }

                    _candidates.Inner.Reverse();
                    yield return true;
                }

                _candidates.Count = 0;

                IEnumerable<long> AllNodes()
                {
                    for (long nodeId = 1; nodeId <= _searchState.Options.CountOfVectors; nodeId++)
                        yield return nodeId;
                }
            }

            public bool TryGetCurrentCandidates(out ContextBoundNativeList<int> candidates)
            {
                if (_candidates.Count == 0)
                {
                    Unsafe.SkipInit(out candidates);
                    return false;
                }

                candidates = _candidates;
                return candidates.Count > 0;
            }

            public void IncreaseNumberOfCandidates(int delta)
            {
            }

            public long CandidatesProcessed { get; set; }
            public int NumberOfCandidates { get; init; }
        }
    }
}

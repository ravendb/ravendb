using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Server.Collections;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Global;
using Voron.Util;
using Voron.Util.PFor;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    public struct VectorSearchRetriever : IDisposable
    {
        private readonly SearchState _searchState;
        private readonly bool _ownsSearchState;
        private int _currentNode, _currentMatchesIndex;
        // The distance of the node whose entries Fill is returning; it carries over to the next Fill call.
        private float _currentDistance;
        private ContextBoundNativeList<long> _postingListResults;
        private FastPForDecoder _pforDecoder;
        private PostingList.Iterator _postingListIterator;
        private PostingList _postingList;
        private readonly float _maximumDistance;
        private readonly IHnswSearcher _vectorsSearcher;
        private readonly ContextBoundNativeList<int> _candidates;
        private readonly Memory<byte> _vector;
        // When the searchState-based entry points allocate a normalized copy of the query
        // vector they hand the scope here so Dispose releases it alongside the retriever's
        // other per-query state.
        private Sparrow.Server.ByteStringContext<Sparrow.Server.ByteStringMemoryCache>.InternalScope? _queryVectorScope;

        public SimilarityMethod? SimilarityMethod => _searchState?.Options.SimilarityMethod;
        
        public bool IsEmpty => _searchState.IsEmpty;
        
        public long CandidatesProcessed => _vectorsSearcher?.CandidatesProcessed ?? 0;
        
        public VectorSearchRetriever(SearchState searchState, IHnswSearcher vectorsSearcher, Memory<byte> vector, float minimumSimilarity,
            bool ownsSearchState = true, Sparrow.Server.ByteStringContext<Sparrow.Server.ByteStringMemoryCache>.InternalScope? queryVectorScope = null)
        {
            _searchState = searchState;
            _ownsSearchState = ownsSearchState;
            _vectorsSearcher = vectorsSearcher;
            _vector = vector;
            _queryVectorScope = queryVectorScope;
            _postingListResults = new(_searchState.Llt.Allocator);
            _pforDecoder = new(searchState.Llt.Allocator);
            _maximumDistance = MinimumSimilarityToDistance(searchState.Options.SimilarityMethod, searchState.Options.VectorSizeBytes, minimumSimilarity);
            _candidates = _vectorsSearcher.Search();
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float DistanceToScore(float distance) => _searchState.DistanceToScore(distance);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DistancesToScores(Span<float> distances) => _searchState.DistancesToScores(distances);

        // The searcher admits only nodes with an entry in the filter; the filter here drops the node's other entries.
        public int Fill(Span<long> matches, Span<float> distances, GrowableBitArray? filter)
        {
            var index = 0;
            while (index < matches.Length)
            {
                if (_postingList != null)
                {
                    if (_postingListIterator.Fill(matches[index..], out var total) is false && total is 0)
                    {
                        _postingListIterator = default;
                        _postingList = null;
                        continue;
                    }

                    index += KeepAdmitted(matches.Slice(index, total), distances[index..], filter);
                    continue;
                }

                if (_currentMatchesIndex < _postingListResults.Count)
                {
                    var copy = Math.Min(_postingListResults.Count - _currentMatchesIndex, matches.Length - index);
                    _postingListResults.CopyTo(matches[index..], _currentMatchesIndex, copy);
                    _currentMatchesIndex += copy;
                    index += KeepAdmitted(matches.Slice(index, copy), distances[index..], filter);
                    continue;
                }

                _currentMatchesIndex = 0;
                _postingListResults.Clear();

                if (TryMoveToNextNode(out var postingListId) == false)
                    break;

                var rawPostingListId = postingListId & Constants.Graphs.VectorId.ContainerType;
                switch (postingListId & Constants.Graphs.VectorId.EnsureIsSingleMask)
                {
                    case Constants.Graphs.VectorId.Tombstone: // empty
                        continue;
                    case Constants.Graphs.VectorId.Single: // single item posting list
                        matches[index] = rawPostingListId;
                        index += KeepAdmitted(matches.Slice(index, 1), distances[index..], filter);
                        continue;
                    case Constants.Graphs.VectorId.SmallPostingList: // small posting list
                        _searchState.ReadPostingList(new ContainerEntryId(rawPostingListId), ref _postingListResults, ref _pforDecoder, out _);
                        continue;
                    case Constants.Graphs.VectorId.PostingList: // large posting list
                        _postingList = OpenPostingList(rawPostingListId);
                        _postingListIterator = _postingList.Iterate();
                        continue;
                    default:
                        throw new ArgumentOutOfRangeException("Impossible scenario, we have only 4 options, but got: " + postingListId);
                }
            }

            return index;
        }

        private bool TryMoveToNextNode(out long postingListId)
        {
            while (_currentNode < _candidates.Count)
            {
                var nodeIndex = _candidates[_currentNode++];
                _currentDistance = QueryDistance(nodeIndex);
                if (_currentDistance > _maximumDistance)
                    continue;

                postingListId = _searchState.GetNodeByIndex(nodeIndex).PostingListId;
                return true;
            }

            postingListId = default;
            return false;
        }

        // Converts raw entries in place to entry ids, compacts them to the entries that the filter admits and gives those the current node's distance.
        private int KeepAdmitted(Span<long> entries, Span<float> distances, GrowableBitArray? filter)
        {
            Registration.InternalEntryIdToEntryId(entries);
            var kept = entries.Length;
            if (filter.HasValue)
            {
                var admitted = filter.Value;
                kept = 0;
                foreach (var entry in entries)
                {
                    if (admitted.Contains(entry))
                        entries[kept++] = entry;
                }
            }

            distances[..kept].Fill(_currentDistance);
            return kept;
        }

        private PostingList OpenPostingList(long rawPostingListId)
        {
            var setStateSpan = Container.GetReadOnly(_searchState.Llt, new(rawPostingListId));
            return new PostingList(_searchState.Llt, Slices.Empty, MemoryMarshal.AsRef<PostingListState>(setStateSpan));
        }

        private float QueryDistance(int nodeIndex)
        {
            long newVectorCount = 0;
            var distance = _searchState.QueryDistance(_vector.Span, nodeIndex, ref newVectorCount);
            Debug.Assert(newVectorCount == 0, "newVectorCount == 0");
            return distance;
        }

        public void Dispose()
        {
            _postingListResults.Dispose();
            _pforDecoder.Dispose();
            _vectorsSearcher?.Dispose();
            _queryVectorScope?.Dispose();
            if (_ownsSearchState)
                _searchState.Dispose();
        }
    }
}

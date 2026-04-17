using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Server;
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
        private int _returnedCandidates;
        private readonly SearchState _searchState;
        private readonly bool _ownsSearchState;
        private int _currentNode, _currentMatchesIndex;
        private ContextBoundNativeList<long> _postingListResults;
        private FastPForDecoder _pforDecoder;
        private PostingList.Iterator _postingListIterator;
        private PostingList _postingList;
        private readonly float _maximumDistance;
        private bool _foundCandidateInCurrentSmallPostingList;
        private readonly IHnswSearcher _vectorsSearcher;
        private readonly Memory<byte> _vector;
        private IEnumerator<bool> _resultsEnumerator;
        // When the searchState-based entry points allocate a normalized copy of the query
        // vector they hand the scope here so Dispose releases it alongside the retriever's
        // other per-query state.
        private Sparrow.Server.ByteStringContext<Sparrow.Server.ByteStringMemoryCache>.InternalScope? _queryVectorScope;

        public SimilarityMethod? SimilarityMethod => _searchState?.Options.SimilarityMethod;
        
        public bool IsEmpty => _searchState.IsEmpty;
        
        // Indicates whether the results are sorted by distance or not. If not, the caller should sort them after fetching the graph.
        // We cannot do it internally since the flow is made in a streaming manner.
        public bool IsSortedByDistance = true;
        
        public int NumberOfCandidates => _vectorsSearcher.NumberOfCandidates;

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
            _maximumDistance = searchState.MinimumSimilarityToDistance(minimumSimilarity);
            _resultsEnumerator = _vectorsSearcher.Search().GetEnumerator();
            _resultsEnumerator.MoveNext(); //read first batch
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float DistanceToScore(float distance) => _searchState.DistanceToScore(distance);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DistancesToScores(Span<float> distances) => _searchState.DistancesToScores(distances);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches, Span<float> distances, GrowableBitArray? filter)
        {
            if (filter != null)
                return Fill(matches, distances, filter.Value);
            
            return Fill(matches, distances);
        }
        
        private int Fill(Span<long> matches, Span<float> distances)
        {
            long newVectorCount = 0;
            if (_vectorsSearcher.TryGetCurrentCandidates(out var indexes) == false)
                return 0;

            int index = 0;
            float distance = float.NaN;
            while (index < matches.Length)
            {
                if (_currentNode >= indexes.Count)
                    break;

                if (_postingList != null)
                {
                    if (_postingListIterator.Fill(matches[index..], out var total) is false && total is 0)
                    {
                        _postingListIterator = default;
                        _postingList = null;
                        _currentNode++;
                        continue;
                    }

                    distances.Slice(index, total).Fill(distance);
                    index += total;
                    continue;
                }

                if (_currentMatchesIndex < _postingListResults.Count)
                {
                    var copy = Math.Min(_postingListResults.Count - _currentMatchesIndex, matches.Length - index);
                    _postingListResults.CopyTo(matches[index..], _currentMatchesIndex, copy);
                    distances.Slice(index, copy).Fill(distance);
                    index += copy;
                    _currentMatchesIndex += copy;
                    if (_currentMatchesIndex == _postingListResults.Count)
                    {
                        _currentMatchesIndex = 0;
                        _postingListResults.Clear();
                        _currentNode++;
                    }

                    continue;
                }

                var nodeIdx = indexes[_currentNode];
                ref var node = ref _searchState.GetNodeByIndex(nodeIdx);
                var rawPostingListId = node.PostingListId & Constants.Graphs.VectorId.ContainerType;
                
                distance = _searchState.QueryDistance(_vector.Span, nodeIdx, ref newVectorCount);
                Debug.Assert(newVectorCount == 0, "newVectorCount == 0");
                
                if (distance > _maximumDistance)
                {
                    _currentNode++;
                    continue;
                }

                switch (node.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask)
                {
                    case Constants.Graphs.VectorId.Tombstone: // empty
                        _currentNode++;
                        continue;
                    case Constants.Graphs.VectorId.Single: // single item posting list
                        distances[index] = distance;
                        matches[index++] = rawPostingListId;
                        _currentNode++;
                        continue;
                    case Constants.Graphs.VectorId.SmallPostingList: // small posting list
                        Debug.Assert(_postingListResults.Count is 0 && _currentMatchesIndex is 0);
                        _searchState.ReadPostingList(new ContainerEntryId(rawPostingListId), ref _postingListResults, ref _pforDecoder, out _);
                        continue;
                    case Constants.Graphs.VectorId.PostingList: // large posting list
                        var setStateSpan = Container.GetReadOnly(_searchState.Llt, new(rawPostingListId));
                        ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
                        _postingList = new PostingList(_searchState.Llt, Slices.Empty, setState);
                        _postingListIterator = _postingList.Iterate();
                        continue;
                    default:
                        throw new ArgumentOutOfRangeException("Impossible scenario, we have only 4 options, but got: " + node.PostingListId);
                }
            }

            Registration.InternalEntryIdToEntryId(matches.Slice(0, index));
            return index;
        }
        
        private int Fill(Span<long> matches, Span<float> distances, GrowableBitArray filter)
        {
            if (_vectorsSearcher.TryGetCurrentCandidates(out var indexes) == false)
                return 0;

            long newVectorCount = 0;
            int index = 0;
            float distance = float.NaN;
            while (index < matches.Length && _returnedCandidates < _vectorsSearcher.NumberOfCandidates)
            {
                if (_currentNode >= indexes.Count)
                {
                    // Double the difference between accepted and searched number of candidates.
                    _vectorsSearcher.IncreaseNumberOfCandidates(_vectorsSearcher.NumberOfCandidates - _returnedCandidates);
                    
                    if (_vectorsSearcher.ShouldContinueSearch(filter.Count) == false)
                    {
                        break;
                    }
                    
                    if (_resultsEnumerator.MoveNext() == false)
                    {
                        _resultsEnumerator = _vectorsSearcher.Search().GetEnumerator();    
                        _resultsEnumerator.MoveNext();
                    }
                    
                    // If we fetch more than once, we've no guarantee that the whole set of results are sorted by distances.
                    // We could explore not previously seen nodes that are closer to the query vector than the ones we've already seen.
                    IsSortedByDistance = false;

                    if (_vectorsSearcher.TryGetCurrentCandidates(out indexes) == false)
                    {
                        // We increased the number of candidates, but we didn't get any more results,
                        // so we end the search right here.
                        break;
                    }
                    
                    // Reset the current node index
                    _currentNode = 0;
                }

                if (_postingList != null)
                {
                    if (_postingListIterator.Fill(matches[index..], out var total) is false && total is 0)
                    {
                        _postingListIterator = default;
                        _postingList = null;

                        _returnedCandidates += _foundCandidateInCurrentSmallPostingList.ToInt32();
                        _foundCandidateInCurrentSmallPostingList = false;

                        _currentNode++;
                        continue;
                    }

                    //decode in bulk
                    Registration.InternalEntryIdToEntryId(matches.Slice(index, total));

                    var currentDocIdx = index;
                    var endDocIdx = index + total;
                    for (; currentDocIdx < endDocIdx; currentDocIdx++)
                    {
                        if (filter.Contains(matches[currentDocIdx]) == false)
                            continue;

                        _foundCandidateInCurrentSmallPostingList = true;
                        matches[index] = matches[currentDocIdx];
                        distances[index] = distance;
                        index++;
                    }

                    continue;
                }

                if (_currentMatchesIndex < _postingListResults.Count)
                {
                    var currentFillLimit = _currentMatchesIndex + Math.Min(_postingListResults.Count - _currentMatchesIndex, matches.Length - index);
                    for (; _currentMatchesIndex < currentFillLimit; _currentMatchesIndex++)
                    {
                        if (filter.Contains(_postingListResults[_currentMatchesIndex]) == false)
                            continue;

                        matches[index] = _postingListResults[_currentMatchesIndex];
                        distances[index] = distance;
                        _foundCandidateInCurrentSmallPostingList = true;
                        index++;
                    }

                    if (_currentMatchesIndex == _postingListResults.Count)
                    {
                        _returnedCandidates += _foundCandidateInCurrentSmallPostingList.ToInt32();
                        _foundCandidateInCurrentSmallPostingList = false;
                        _currentMatchesIndex = 0;
                        _postingListResults.Clear();
                        _currentNode++;
                    }

                    continue;
                }

                var nodeIdx = indexes[_currentNode];
                ref var node = ref _searchState.GetNodeByIndex(nodeIdx);
                var rawPostingListId = node.PostingListId & Constants.Graphs.VectorId.ContainerType;
                
                distance = _searchState.QueryDistance(_vector.Span, nodeIdx, ref newVectorCount);
                Debug.Assert(newVectorCount == 0, "newVectorCount == 0");
                
                if (distance > _maximumDistance)
                {
                    _currentNode++;
                    continue;
                }

                switch (node.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask)
                {
                    case Constants.Graphs.VectorId.Tombstone: // empty
                        _currentNode++;
                        continue;
                    case Constants.Graphs.VectorId.Single: // single item posting list
                        _currentNode++;
                        var rawEntry = Registration.InternalEntryIdToEntryId(rawPostingListId);
                        if (filter.Contains(rawEntry) == false)
                            continue;

                        distances[index] = distance;
                        matches[index++] = rawEntry;
                        _returnedCandidates++;
                        continue;
                    case Constants.Graphs.VectorId.SmallPostingList: // small posting list
                        Debug.Assert(_postingListResults.Count is 0 && _currentMatchesIndex is 0 && _foundCandidateInCurrentSmallPostingList is false);
                        _searchState.ReadPostingList(new(rawPostingListId), ref _postingListResults, ref _pforDecoder, out _);
                        Registration.InternalEntryIdToEntryId(_postingListResults.ToSpan());
                        continue;
                    case Constants.Graphs.VectorId.PostingList: // large posting list
                        Debug.Assert(_foundCandidateInCurrentSmallPostingList is false);
                        var setStateSpan = Container.GetReadOnly(_searchState.Llt, new(rawPostingListId));
                        ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
                        _postingList = new PostingList(_searchState.Llt, Slices.Empty, setState);
                        _postingListIterator = _postingList.Iterate();
                        continue;
                    default:
                        throw new ArgumentOutOfRangeException("Impossible scenario, we have only 4 options, but got: " + node.PostingListId);
                }
            }

            return index;
        }
        
        public void Dispose()
        {
            _postingListResults.Dispose();
            _pforDecoder.Dispose();
            _resultsEnumerator?.Dispose();
            _vectorsSearcher?.Dispose();
            _queryVectorScope?.Dispose();
            if (_ownsSearchState)
                _searchState.Dispose();
        }
    }
}

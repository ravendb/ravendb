using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Voron.Data.RoaringBitmaps;
using Sparrow.Server.Utils;
using Voron.Data.Graphs;
using Voron.Util;

namespace Corax.Querying.Matches;

public struct MultiVectorSearchMatch : IPostFilterMatch
{
    private const int ScanningThreshold = 1024;

    /// <summary>Set by <c>QueryPlanBuilder.ApplyPostFilters</c> when this vector match was lifted to a top-level
    /// post-filter. Left false when it is an ordinary leaf inside an OR branch.</summary>
    public bool IsPostFilter { get; set; }

    private readonly IndexSearcher _indexSearcher;
    private readonly FieldMetadata _metadata;
    private readonly float _minimumMatch;
    private readonly int _numberOfCandidates;
    private readonly bool _isExact;
    private readonly VectorValue[] _vectorsToSearch;


    // Number of documents to be directly scanned instead of ANN / Exact on HNSW.
    private readonly int _scanningThreshold;
    private readonly Random _random;
    private bool _scanningQuery;


    // Internal buffers used to store results from VectorSearch.
    private GrowableBuffer<long, Constant<long>> _matches;
    private GrowableBuffer<float, Constant<float>> _distances;

    // Reference to the first sub-query's retriever, kept so Score can call its distance-to-score
    // conversion methods after all retrievers have been disposed.
    private Hnsw.VectorSearchRetriever _firstRetriever;
    private ContextBoundNativeList<long> _nodesIdsToScan;
    private bool _vectorRetrieverInitialized;

    private bool _resultsPersisted;
    private int _positionOnPersistedValues = 0;
    private bool _isEmpty;


    /// <summary>
    /// When VectorSearch is the only condition in the WHERE statement,
    /// do not sort to fulfill the Fill guarantees.
    /// Otherwise, sorting is necessary as it may produce incorrect results in the upper AST statements.
    /// </summary>
    private readonly bool _singleVectorSearchDoNotSort;

    private RoaringBitmap _filterResults;
    private bool _hasFilterResults;
    private bool _ownsFilterResults;
    private IQueryMatch _filterQuery;

    public MultiVectorSearchMatch(IndexSearcher searcher, in FieldMetadata metadata, in VectorValue[] vectorsToSearch, in float minimumMatch, in int numberOfCandidates,
        in bool isExact, in bool singleVectorSearchDoNotSortByIds, IQueryMatch filterQuery, int scanningThreshold = ScanningThreshold, Random random = null)
    {
        _indexSearcher = searcher;
        _metadata = metadata;
        _minimumMatch = minimumMatch;
        _vectorsToSearch = vectorsToSearch;
        _numberOfCandidates = numberOfCandidates;
        _isExact = isExact;
        _filterQuery = filterQuery;
        _scanningThreshold = scanningThreshold;
        _random = random;
        IsBoosting = true;
        _singleVectorSearchDoNotSort = singleVectorSearchDoNotSortByIds;
        _isEmpty = false;
    }

    private void InitializeVectorSearch()
    {
        Debug.Assert(_vectorRetrieverInitialized == false, "Vector Retriever should be initialized only once.");
        _vectorRetrieverInitialized = true;

        if (_filterQuery != null)
        {
            // When filterQuery is IBitmapQueryMatch (e.g. CompiledQueryMatch), LoadFilterMatches
            // borrows the bitmap directly without re-materialization. No separate fast-path
            // needed here — the optimization lives in VectorSearchUtils.LoadFilterMatches.
            _filterResults = IndexSearcher.VectorSearchUtils.LoadFilterMatches(_indexSearcher, ref _filterQuery, out _ownsFilterResults);
            _hasFilterResults = true;

            // Shortcut for empty filter
            if (_filterResults.ComputeCount() == 0)
            {
                _isEmpty = true;
                return;
            }
        }

        _scanningQuery = IndexSearcher.VectorSearchUtils.ShouldScan(_indexSearcher, _filterResults.ComputeCount(), _isExact, _filterQuery, _scanningThreshold, _numberOfCandidates);
        if (_scanningQuery)
        {
            var hasNodes = IndexSearcher.VectorSearchUtils.TryConvertDocumentsIdsToNodesIds(_indexSearcher, _metadata, ref _filterResults, out _nodesIdsToScan);
            if (hasNodes == false)
            {
                _isEmpty = true;
                _nodesIdsToScan.Dispose();
                if (_hasFilterResults && _ownsFilterResults)
                    _filterResults.Dispose();
                foreach (var vector in _vectorsToSearch)
                    vector.Dispose();
                return;
            }
        }

        // Obtain the IndexSearcher-scoped SearchState for this field; sub-queries below reuse it
        // so loaded node data (edges, vectors) is shared across them.
        var sharedSearchState = _indexSearcher.GetOrCreateVectorSearchState(_metadata.FieldName);

        _isEmpty = sharedSearchState.IsEmpty || (_hasFilterResults && _filterResults.ComputeCount() == 0);
    }

    public int Fill(Span<long> matches)
    {
        if (_vectorRetrieverInitialized == false)
            InitializeVectorSearch();

        if (_resultsPersisted == false)
            FillAndPersistResults();

        if (_isEmpty)
            return 0;

        var resultsLeft = _matches.Count - _positionOnPersistedValues;

        if (resultsLeft == 0)
            return 0;

        var amountToCopy = Math.Min(resultsLeft, matches.Length);
        _matches.Results.Slice(_positionOnPersistedValues,  amountToCopy).CopyTo(matches.Slice(0, amountToCopy));
        _positionOnPersistedValues += amountToCopy;
        return amountToCopy;
    }

    private void FillAndPersistResults()
    {
        Debug.Assert(_resultsPersisted == false, "Results should be persisted only once.");
        _resultsPersisted = true;
        if (_isEmpty)
            return;

        // Construct and fully consume each retriever before moving to the next; the shared
        // SearchState has a single pair of priority queues (_candidatesQ, _nearestEdgesQ) that
        // cannot be interleaved across retrievers.
        var sharedSearchState = _indexSearcher.GetOrCreateVectorSearchState(_metadata.FieldName);

        _matches.Init(_indexSearcher.Allocator, 128);
        _distances.Init(_indexSearcher.Allocator, 128);
        for (var i = 0; i < _vectorsToSearch.Length; ++i)
        {
            // Invariant: when a new retriever starts, the shared priority queues on SearchState
            // must be empty. Violating this causes the new traversal to read leftover entries
            // from the previous sub-query.
            sharedSearchState.AssertSharedQueuesClean();

            var vector = _vectorsToSearch[i].GetEmbeddingMemory();
            var vectorSearcher = (_isExact) switch
            {
                _ when _scanningQuery => Hnsw.ExactNearest(sharedSearchState, _numberOfCandidates, vector, _minimumMatch, false, _nodesIdsToScan),
                true => Hnsw.ExactNearest(sharedSearchState, _numberOfCandidates, vector, _minimumMatch, _filterQuery != null, null),
                false when _filterQuery != null => Hnsw.ApproximateFilteredNearest(sharedSearchState, _numberOfCandidates, vector, _minimumMatch, new IndexSearcher.VectorSearchUtils.RandomNodesFromFilterEnumerator(_indexSearcher, _metadata, _filterResults, _random)),
                false => Hnsw.ApproximateNearest(sharedSearchState, _numberOfCandidates, vector, _minimumMatch, _filterQuery != null),
            };

            if (i == 0)
                _firstRetriever = vectorSearcher;

            int currentRead = 0;
            do
            {
                var matchBuffer = _matches.GetSpace();
                var distanceBuffer = _distances.GetSpace();
                Debug.Assert(matchBuffer.Length == distanceBuffer.Length, "matchBuffer.Length == distanceBuffer.Length");

                currentRead = _hasFilterResults
                    ? vectorSearcher.Fill(matchBuffer, distanceBuffer, ref _filterResults)
                    : vectorSearcher.Fill(matchBuffer, distanceBuffer);


                _matches.AddUsage(currentRead);
                _distances.AddUsage(currentRead);
                Count += currentRead;
            } while (currentRead > 0);

            vectorSearcher.Dispose();
            _vectorsToSearch[i].Dispose();
        }

        if (_scanningQuery)
            _nodesIdsToScan.Dispose();

        // Min on distances is Max on score.
        var uniqueCount = Sorting.SortAndMinOnDuplicates(_matches.Results, _distances.Results);
        _matches.Truncate(uniqueCount);
        _distances.Truncate(uniqueCount);

        // Streaming query, we need to return already sorted
        if (_singleVectorSearchDoNotSort) 
            _distances.Results.Sort(_matches.Results);
        
        if (_hasFilterResults && _ownsFilterResults)
            _filterResults.Dispose();
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        if (_vectorRetrieverInitialized == false)
            InitializeVectorSearch();

        if (_resultsPersisted == false)
            FillAndPersistResults();

        if (_isEmpty)
            return 0;

        var results = _matches.Results;
        return MergeHelper.And(buffer[..matches], buffer[..matches], results);
    }

    // Vector match results are not entry-id ordered, so there is no sorted fast path; behaves exactly like Score.
    public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor) => Score(matches, scores, boostFactor);

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (_isEmpty || _resultsPersisted == false)
        {
            // The caller may invoke Score even when this match was not evaluated (e.g. the other
            // side of an AND was empty). In these cases, the call is ignored.
            return;
        }

        if (_singleVectorSearchDoNotSort == false)
        {
            if (_filterQuery != null)
            {
                ref var filterQuery = ref _filterQuery;
                filterQuery.Score(matches, scores, boostFactor);
            }

            for (int i = 0; i < matches.Length; ++i)
            {
                var match = matches[i];
                var pos = _matches.Results.BinarySearch(match);
                if (pos < 0)
                    continue;

                var distance = _distances.Results[pos];
                scores[i] += boostFactor * _firstRetriever.DistanceToScore(distance);
            }
        }
        else
        {
            _distances.Results[..scores.Length].CopyTo(scores);
            _firstRetriever.DistancesToScores(scores);
            for (int i = 0; i < scores.Length; ++i)
                scores[i] *= boostFactor;
        }

        _matches.Dispose();
        _distances.Dispose();
    }

    public long Count { get; private set; }

    public bool IsBoosting { get; }

    public QueryInspectionNode Inspect()
    {
        var mvsInspect =  new QueryInspectionNode(nameof(MultiVectorSearchMatch),
            parameters: new Dictionary<string, string>()
            {
                { Constants.QueryInspectionNode.FieldName, _metadata.FieldName.ToString() },                
                { nameof(Hnsw.SimilarityMethod), _firstRetriever.SimilarityMethod?.ToString() ?? "Query not initialized." },
                { "IsExact", _isExact.ToString() },
                { "IsScanning", _scanningQuery.ToString() },
                { "MinimumMatch", _minimumMatch.ToString(CultureInfo.InvariantCulture) },
                { "NumberOfCandidates", _numberOfCandidates.ToString() },
            })
        {
            // Reflects the lifting decision recorded on this match, not the type: a vector leaf inside an OR is
            // a pipeline leaf, not a post-filter (see IPostFilterMatch).
            IsPostFilter = this.IsPostFilter
        };

        if (_filterQuery is not null)
        {
            return new QueryInspectionNode($"{nameof(MultiVectorSearchMatch)} [And]",
                children: new List<QueryInspectionNode> { mvsInspect, _filterQuery.Inspect() },
                parameters: new Dictionary<string, string>()
                {
                    {"VectorSearchAndOperation", "true"}
                });
        }
        
        return mvsInspect;
    }
}

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Voron.Data.RoaringBitmaps;
using Sparrow.Server.Utils;
using Voron.Data.Graphs;
using Voron.Util;

namespace Corax.Querying.Matches;

[DebuggerDisplay("{DebugView,nq}")]
public struct VectorSearchMatch : IPostFilterMatch
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
    private VectorValue _vectorToSearch;

    
    // Number of documents to be directly scanned instead of ANN / Exact on HNSW.
    private readonly int _scanningThreshold;
    private readonly Random _random;
    private bool _scanningQuery;

    
    // Internal buffers used to store results from VectorSearch.
    private GrowableBuffer<long, Constant<long>> _matches;
    private GrowableBuffer<float, Constant<float>> _distances;
    
    // Voron VectorSearch Retriever
    private Hnsw.VectorSearchRetriever _vectorSearchRetriever;
    private ContextBoundNativeList<long> _nodesIdsToScan;
    private bool _vectorRetrieverInitialized;
    
    private bool _resultsPersisted;
    private bool _returnedAllResults = false;
    private int _positionOnPersistedValues = 0;
    private int _scorePosition = 0;
    private bool _isEmpty;
    
    
    /// <summary>
    /// When true, results are returned in score order (sorted by distance, nearest-first) — the streaming Fill
    /// fast path. Only safe when this vector search is the sole WHERE condition; otherwise the matches must be
    /// entry-id sorted, as the upper AST statements (AND/OR merges, set-difference) rely on that ordering.
    /// </summary>
    private readonly bool _sortByScore;

    private RoaringBitmap _filterResults;
    private bool _hasFilterResults;
    private bool _ownsFilterResults;
    private IQueryMatch _filterQuery;
    private bool _filterQueryLoaded;

    // Diagnostics surfaced via Inspect() so the query-plan graph can attribute the vector
    // post-filter's cost: the filter candidate set size, and how long the (lazy) one-time
    // InitializeVectorSearch took (filter materialization + seed sampling + HNSW retriever setup).
    private long _filterEntriesCount;
    private double _initDurationMs;

    // Wall-clock spent inside the HNSW retriever's Fill calls — the graph traversal / exact scan that
    // CandidatesProcessed counts — accumulated across streamed batches. This is the search work proper,
    // distinct from the one-time InitMs setup (filter materialization + seed sampling + retriever construction).
    private double _searchDurationMs;

    private bool CanStreamResults => IsBoosting == false && _sortByScore;

    public VectorSearchMatch(IndexSearcher searcher,
        in FieldMetadata metadata,
        VectorValue vectorToSearch,
        in float minimumMatch,
        in int numberOfCandidates,
        in bool isExact,
        in bool sortByScore,
        IQueryMatch filterQuery,
        int scanningThreshold = ScanningThreshold,
        Random random = null)
    {
        _sortByScore = sortByScore;
        _filterQuery = filterQuery;
        _metadata = metadata;
        _indexSearcher = searcher;
        IsBoosting = metadata.HasBoost;
        _vectorRetrieverInitialized = false;
        _minimumMatch = minimumMatch;
        _numberOfCandidates = numberOfCandidates;
        _isExact = isExact;
        _vectorToSearch = vectorToSearch;
        _filterQueryLoaded = filterQuery is null;
        _scanningThreshold = scanningThreshold;
        _random = random;
        _isEmpty = false;
    }

    /// <summary>
    /// Initialization of vector search is lazy to avoid expensive computation/IO during the QueryBuilding phase.
    /// </summary>
    private void InitializeVectorSearch()
    {
        Debug.Assert(_vectorRetrieverInitialized == false, "Vector Retriever should be initialized only once.");
        _vectorRetrieverInitialized = true;

        var initStart = Stopwatch.GetTimestamp();
        try
        {
            if (_filterQueryLoaded == false)
            {
                _filterQueryLoaded = true;
                // When filterQuery is IBitmapQueryMatch (e.g. CompiledQueryMatch), LoadFilterMatches
                // borrows the bitmap directly without re-materialization. No separate fast-path
                // needed here — the optimization lives in VectorSearchUtils.LoadFilterMatches.
                _filterResults = IndexSearcher.VectorSearchUtils.LoadFilterMatches(_indexSearcher, ref _filterQuery, out _ownsFilterResults);
                _hasFilterResults = true;
                _filterEntriesCount = _filterResults.ComputeCount();

                // Shortcut for empty filter
                if (_filterEntriesCount == 0)
                {
                    _isEmpty = true;
                    return;
                }
            }

            _scanningQuery = IndexSearcher.VectorSearchUtils.ShouldScan(_indexSearcher, _filterEntriesCount, _isExact, _filterQuery, _scanningThreshold, _numberOfCandidates);
            var vector = _vectorToSearch.GetEmbeddingMemory();
            var fieldName = _metadata.FieldName;

            ContextBoundNativeList<long> nodesIdsToScan = default;
            if (_scanningQuery)
            {
                var hasNodes = IndexSearcher.VectorSearchUtils.TryConvertDocumentsIdsToNodesIds(_indexSearcher, _metadata, ref _filterResults, out nodesIdsToScan);
                if (hasNodes == false)
                {
                    _isEmpty = true;
                    _vectorToSearch.Dispose();
                    if (_hasFilterResults && _ownsFilterResults)
                        _filterResults.Dispose();
                    return;
                }

                _nodesIdsToScan = nodesIdsToScan;
            }
            var searchState = _indexSearcher.GetOrCreateVectorSearchState(fieldName);

            _vectorSearchRetriever = _isExact switch
            {
                _ when _scanningQuery => Hnsw.ExactNearest(searchState, _numberOfCandidates, vector, _minimumMatch, hasFilterMatch: false, nodesIdsToScan),
                true => Hnsw.ExactNearest(searchState, _numberOfCandidates, vector, _minimumMatch, _filterQuery != null),
                false when _filterQuery != null => Hnsw.ApproximateFilteredNearest(searchState, _numberOfCandidates, vector, _minimumMatch, new IndexSearcher.VectorSearchUtils.RandomNodesFromFilterEnumerator(_indexSearcher, _metadata, _filterResults, _random)),
                    _ => Hnsw.ApproximateNearest(searchState, _numberOfCandidates, vector, _minimumMatch, _filterQuery != null),
            };

            _isEmpty = _scanningQuery
                ? _filterEntriesCount == 0 || _vectorSearchRetriever.IsEmpty
                : _vectorSearchRetriever.IsEmpty;
        }
        finally
        {
            _initDurationMs = Stopwatch.GetElapsedTime(initStart).TotalMilliseconds;
        }
    }
    
    public int Fill(Span<long> matches)
    {
        if (_vectorRetrieverInitialized == false)
            InitializeVectorSearch();

        if (_isEmpty)
            return 0;

        if (CanStreamResults) // case when we do not care about scores.
            return FillDiscardSimilarity(matches);

        if (_resultsPersisted == false)
            FillAndPersistResults();


        var resultsLeft = _matches.Count - _positionOnPersistedValues;
        if (resultsLeft == 0)
        {
            return 0;
        }

        var amountToCopy = Math.Min(resultsLeft, matches.Length);
        _matches.Results.Slice(_positionOnPersistedValues,  amountToCopy).CopyTo(matches.Slice(0, amountToCopy));
        _positionOnPersistedValues += amountToCopy;
        return amountToCopy;
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        if (_vectorRetrieverInitialized == false)
            InitializeVectorSearch();

        if (_isEmpty)
            return 0;

        if (_resultsPersisted == false)
            FillAndPersistResults();

        var results = _matches.Results;
        return MergeHelper.And(buffer[..matches], buffer[..matches], results);
    }

    private int FillDiscardSimilarity(Span<long> matches)
    {
        if (_returnedAllResults || _isEmpty)
            return 0;
        
        if (_distances.Capacity < sizeof(float) * matches.Length)
            CreateDistanceBuffer(matches.Length);

        var distancesBuffer = _distances.GetSpace();

        var searchStart = Stopwatch.GetTimestamp();
        var read = _hasFilterResults ? _vectorSearchRetriever.Fill(matches, distancesBuffer, ref _filterResults) : _vectorSearchRetriever.Fill(matches, distancesBuffer);
        _searchDurationMs += Stopwatch.GetElapsedTime(searchStart).TotalMilliseconds;

        if (read == 0)
        {
            _returnedAllResults = true;
            _distances.Dispose();
            _distances = default;
            Dispose();
            return 0;
        }
        
        read = Sorting.SortAndMinOnDuplicates(matches[..read], distancesBuffer[..read]);
        distancesBuffer[..read].Sort(matches[..read]);
        Count += read;
        return read;
    }

    private void CreateDistanceBuffer(int length)
    {
        ref var distances = ref _distances;
        distances.Init(_indexSearcher.Allocator, length);
    }
    
    private void FillAndPersistResults()
    {
        Debug.Assert(_resultsPersisted == false, "Results should be persisted only once.");
        _resultsPersisted = true;
        
        ref var matches = ref _matches;
        ref var distances = ref _distances;
        
        matches.Init(_indexSearcher.Allocator, 128);
        distances.Init(_indexSearcher.Allocator, 128);
        var currentRead = 0;
        do
        {
            var mBuf = matches.GetSpace();
            var dBuf = distances.GetSpace();
            Debug.Assert(mBuf.Length == dBuf.Length, "mBuf.Length == dBuf.Length");

            var searchStart = Stopwatch.GetTimestamp();
            currentRead = _hasFilterResults
                ? _vectorSearchRetriever.Fill(mBuf, dBuf, ref _filterResults)
                : _vectorSearchRetriever.Fill(mBuf, dBuf);
            _searchDurationMs += Stopwatch.GetElapsedTime(searchStart).TotalMilliseconds;

            matches.AddUsage(currentRead);
            distances.AddUsage(currentRead);

        } while (currentRead != 0);
        
        if (_sortByScore == false)
        {
            var matchesCount = Sorting.SortAndMinOnDuplicates(matches.Results, distances.Results);
            distances.Truncate(matchesCount);
            matches.Truncate(matchesCount);
        }
        
        // Score order requested and the retriever didn't already return distance-sorted: sort by distance
        // (nearest-first). distances is the sort key; matches is permuted to follow.
        if (_sortByScore && _vectorSearchRetriever.IsSortedByDistance == false)
        {
            distances.Results.Sort(matches.Results);
        }

        Count = _matches.Count;

        Dispose();
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

        if (_sortByScore == false)
        {
            ref var matchesRef = ref MemoryMarshal.GetReference(matches);
            ref var scoresRef = ref MemoryMarshal.GetReference(scores);
            ref var distanceRef = ref MemoryMarshal.GetReference(_distances.Results);
            if (_filterQuery != null)
                _filterQuery.Score(matches, scores, boostFactor);


            for (var i = 0; i < matches.Length; ++i)
            {
                var match = Unsafe.Add(ref matchesRef, i);
                var pos = _matches.Results.BinarySearch(match);
                if (pos < 0)
                    continue;

                Unsafe.Add(ref scoresRef, i) += _vectorSearchRetriever.DistanceToScore(Unsafe.Add(ref distanceRef, pos));
            }

            _matches.Dispose();
            _distances.Dispose();
        }
        else
        {
            // Single vector post-filter streaming in score order: the SortingMatch wrapper that would
            // normally surface scores was skipped, so the read loop calls Score once per Fill batch. The
            // persisted distances are already in score order and aligned 1:1 with Fill's emission, so we
            // copy the batch starting at the running position (rather than always from 0) and convert it.
            _distances.Results.Slice(_scorePosition, scores.Length).CopyTo(scores);
            _vectorSearchRetriever.DistancesToScores(scores);
            _scorePosition += scores.Length;

            // A single full-set call (from the SortingMatch comparer) drains immediately; the batched
            // read-loop path releases on the final batch. The buffers are allocator-backed, so an early
            // paging stop simply defers the free to the query-end allocator reset.
            if (_scorePosition >= _distances.Count)
            {
                _matches.Dispose();
                _distances.Dispose();
            }
        }
    }
    
    private string ResolveSearchMode()
    {
        if (_scanningQuery) return "ExactOverFilter"; // Brute-force over filter docs mapped to nodes
        if (_isExact) return "ExactAll"; // Brute-force over the whole/filtered set
        if (_filterQuery != null) return "ApproximateFromFiltered"; // HNSW ANN seeded from a sample of the filter set
        return "ApproximateAll";// Plain HNSW ANN with no filter
    }

    public QueryInspectionNode Inspect()
    {
        var searchMode = ResolveSearchMode();

        var vsInspect =  new QueryInspectionNode(nameof(VectorSearchMatch),
            parameters: new Dictionary<string, string>()
            {
                { Constants.QueryInspectionNode.FieldName, _metadata.FieldName.ToString() },
                { nameof(Hnsw.SimilarityMethod), _vectorSearchRetriever.SimilarityMethod?.ToString() ?? "Query not initialized." },
                { "SearchMode", searchMode },
                { "IsExact", _isExact.ToString() },
                { "IsScanning", _scanningQuery.ToString() },
                { "MinimumMatch", _minimumMatch.ToString(CultureInfo.InvariantCulture) },
                { "NumberOfCandidates", _numberOfCandidates.ToString() },
                { "FilterEntries", _filterEntriesCount.ToString("N0") },
                // Vectors scanned: nodes the searcher walked (HNSW: graph nodes visited; Exact: nodes enumerated,
                // including skipped tombstones). VectorComparisons: actual distance computations (SimilarityCalc),
                // which for Exact is <= scanned and for HNSW equals it.
                { "NumberOfCandidatesScanned", (_vectorSearchRetriever.CandidatesProcessed).ToString("N0")},
                { "VectorComparisons", (_vectorSearchRetriever.VectorComparisons).ToString("N0")},
                { "InitMs", _initDurationMs.ToString("F3", CultureInfo.InvariantCulture) },
                { "SearchMs", _searchDurationMs.ToString("F3", CultureInfo.InvariantCulture) },
                { Constants.QueryInspectionNode.MatchedResults, Count.ToString("N0") }
            })
        {
            // Reflects the lifting decision recorded on this match, not the type: a vector leaf inside an OR is
            // a pipeline leaf, not a post-filter (see IPostFilterMatch).
            IsPostFilter = this.IsPostFilter
        };

        if (_filterQuery is not null)
        {
            return new QueryInspectionNode($"{nameof(VectorSearchMatch)} [And]",
                children: new List<QueryInspectionNode> { _filterQuery.Inspect(), vsInspect },
                parameters: new Dictionary<string, string>()
                {
                    {"VectorSearchAndOperation", "true"},
                    { Constants.QueryInspectionNode.MatchedResults, Count.ToString("N0") }
                });
        }
        
        return vsInspect;
    }

    public string DebugView => Inspect().ToString();
    
    public long Count { get; private set; }

    public bool IsBoosting { get; init; }

    private void Dispose()
    {
        if (_scanningQuery)
            _nodesIdsToScan.Dispose();
        if (_hasFilterResults && _ownsFilterResults)
            _filterResults.Dispose();
        _vectorSearchRetriever.Dispose();
        _vectorToSearch.Dispose();
    }
}

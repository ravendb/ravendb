using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow.Server.Collections;
using Sparrow.Server.Utils;
using Voron.Data.Graphs;
using Voron.Util;

namespace Corax.Querying.Matches;

[DebuggerDisplay("{DebugView,nq}")]
public struct VectorSearchMatch : IQueryMatch
{
    private const int ScanningThreshold = 1024;
    
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
    private bool _isEmpty;
    
    
    /// <summary>
    /// When VectorSearch is the only condition in the WHERE statement,
    /// do not sort to fulfill the Fill guarantees.
    /// Otherwise, sorting is necessary as it may produce incorrect results in the upper AST statements.
    /// </summary>
    private readonly bool _singleVectorSearchDoNotSort;

    private GrowableBitArray? _filterResults;
    private IQueryMatch _filterQuery;
    private bool _filterQueryLoaded;
    private long _filterMatchesCount;

    private bool CanStreamResults => IsBoosting == false && _singleVectorSearchDoNotSort;

    public VectorSearchMatch(IndexSearcher searcher, 
        in FieldMetadata metadata, 
        VectorValue vectorToSearch,
        in float minimumMatch, 
        in int numberOfCandidates, 
        in bool isExact, 
        in bool singleVectorSearchDoNotSort, 
        IQueryMatch filterQuery,
        int scanningThreshold = ScanningThreshold,
        Random random = null)
    {
        _singleVectorSearchDoNotSort = singleVectorSearchDoNotSort;
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

        if (_filterQueryLoaded == false)
        {
            _filterQueryLoaded = true;
            _filterResults = IndexSearcher.VectorSearchUtils.LoadFilterMatches(_indexSearcher, ref _filterQuery);
            _filterMatchesCount = _filterResults?.Count ?? 0;

            // Shortcut for empty filter
            if (_filterMatchesCount == 0)
            {
                _isEmpty = true;
                return;
            }
        }
        
        _scanningQuery = IndexSearcher.VectorSearchUtils.ShouldScan(_indexSearcher, _filterMatchesCount, _isExact, _filterQuery, _scanningThreshold, _numberOfCandidates);
        var llt = _indexSearcher._transaction.LowLevelTransaction;
        var vector = _vectorToSearch.GetEmbeddingMemory();
        var fieldName = _metadata.FieldName;
        
        ContextBoundNativeList<long> nodesIdsToScan = default;
        if (_scanningQuery)
        {
            var hasNodes = IndexSearcher.VectorSearchUtils.TryConvertDocumentsIdsToNodesIds(_indexSearcher, _metadata, _filterResults!.Value, out nodesIdsToScan);
            if (hasNodes == false)
            {
                _isEmpty = true;
                _vectorToSearch.Dispose();
                _filterResults?.Dispose();
                return;
            }
            
            _nodesIdsToScan = nodesIdsToScan;
        }
        
        
        var searchState = _indexSearcher.GetOrCreateVectorSearchState(fieldName);
        _vectorSearchRetriever = _isExact switch
        {
            _ when _scanningQuery => Hnsw.ExactNearest(searchState, _numberOfCandidates, vector, _minimumMatch, hasFilterMatch: false, nodesIdsToScan),
            true => Hnsw.ExactNearest(searchState, _numberOfCandidates, vector, _minimumMatch, _filterQuery != null),
            false when _filterQuery != null => Hnsw.ApproximateFilteredNearest(searchState, _numberOfCandidates, vector, _minimumMatch, new IndexSearcher.VectorSearchUtils.RandomNodesFromFilterEnumerator(_indexSearcher, _metadata, _filterResults!.Value, _random)),
                _ => Hnsw.ApproximateNearest(searchState, _numberOfCandidates, vector, _minimumMatch, _filterQuery != null),
        };
        

        _isEmpty = _scanningQuery 
            ? _filterMatchesCount == 0 || _vectorSearchRetriever.IsEmpty
            : _vectorSearchRetriever.IsEmpty;
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

        return MergeHelper.And(buffer, buffer.Slice(0, matches), _matches.Results);
    }

    private int FillDiscardSimilarity(Span<long> matches)
    {
        if (_returnedAllResults || _isEmpty)
            return 0;
        
        if (_distances.Capacity < sizeof(float) * matches.Length)
            CreateDistanceBuffer(matches.Length);

        var distancesBuffer = _distances.GetSpace();
        
        var read = _vectorSearchRetriever.Fill(matches, distancesBuffer, _filterResults);
        
        if (read == 0)
        {
            _returnedAllResults = true;
            _distances.Dispose();
            _distances = default;
            Dispose();
            return 0;
        }
        
        Sorting.SortAndMinOnDuplicates(matches[..read], distancesBuffer[..read]);
        distancesBuffer[..read].Sort(matches[..read]);
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

            currentRead = _vectorSearchRetriever.Fill(mBuf, dBuf, _filterResults);
            
            matches.AddUsage(currentRead);
            distances.AddUsage(currentRead);

        } while (currentRead != 0);
        
        if (_singleVectorSearchDoNotSort == false)
        {
            //Truncate the buffer to the actual size
            var matchesCount = Sorting.SortAndMinOnDuplicates(matches.Results, distances.Results);
            distances.Truncate(matchesCount);
            matches.Truncate(matchesCount);
        }
        else if (_vectorSearchRetriever.IsSortedByDistance == false)
        {
            distances.Results.Sort(matches.Results);
        }
        
        Dispose();
    }
    
    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (_isEmpty || _resultsPersisted == false)
        {
            // BinaryMatch may skip the method call if the other node of the AND clause 
            // is empty, the evaluation of this primitive is pointless. In these cases, the call is ignored.
            return;
        }
        
        if (_singleVectorSearchDoNotSort == false)
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
        }
        else
        {
            _distances.Results[..scores.Length].CopyTo(scores);
            _vectorSearchRetriever.DistancesToScores(scores);
        }
        
        _matches.Dispose();
        _distances.Dispose();
    }

    public QueryInspectionNode Inspect()
    {
        var vsInspect =  new QueryInspectionNode(nameof(VectorSearchMatch),
            parameters: new Dictionary<string, string>()
            {
                { Constants.QueryInspectionNode.FieldName, _metadata.FieldName.ToString() },
                { nameof(Hnsw.SimilarityMethod), _vectorSearchRetriever.SimilarityMethod?.ToString() ?? "Query not initialized." },
                { "IsExact", _isExact.ToString() },
                { "IsScanning", _scanningQuery.ToString() },
                { "Minimum match", _minimumMatch.ToString(CultureInfo.InvariantCulture) },
                { "Number of candidates", _numberOfCandidates.ToString() },
                { "Number of candidates scanned", (_vectorSearchRetriever.CandidatesProcessed).ToString()}
            });

        if (_filterQuery is not null)
        {
            return new QueryInspectionNode($"{nameof(BinaryMatch)} [And]",
                children: new List<QueryInspectionNode> { _filterQuery.Inspect(), vsInspect },
                parameters: new Dictionary<string, string>()
                {
                    {"VectorSearchAndOperation", "true"}
                });
        }
        
        return vsInspect;
    }

    public string DebugView => Inspect().ToString();
    
    // We have to perform deduplication in two cases:
    // a) when vector search is the only condition in the WHERE statement, we do not fulfill the IQueryMatch.Fill guarantee
    // about deduplication and sorted IDs on purpose to return results ordered by score without additional sorting, since
    // HNSW returns results ordered by distance.
    // b) when the query field explicitly has no boost, then we stream the results in bulks (instead of memoizing them), so
    // we need to track previously returned IDs to avoid duplicates.
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => _singleVectorSearchDoNotSort || IsBoosting == false
        ? DuplicatesOccurrence.Possible 
        : DuplicatesOccurrence.NotPossible;
    
    public long Count { get; private set; }

    public SkipSortingResult AttemptToSkipSorting()
    {
        return _singleVectorSearchDoNotSort 
            ? SkipSortingResult.ResultsNativelySorted 
            : SkipSortingResult.SortingIsRequired;
    }

    public QueryCountConfidence Confidence => QueryCountConfidence.Low;
    
    public bool IsBoosting { get; init; }

    private void Dispose()
    {
        if (_scanningQuery)
            _nodesIdsToScan.Dispose();
        _filterResults?.Dispose();
        _vectorSearchRetriever.Dispose();
        _vectorToSearch.Dispose();
    }
}

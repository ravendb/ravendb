using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow.Server.Utils;
using Sparrow.Server.Utils.VxSort;
using Voron.Data.Graphs;

namespace Corax.Querying.Matches;

[DebuggerDisplay("{DebugView,nq}")]
public struct VectorSearchMatch : IQueryMatch
{
    private Hnsw.NearestSearch _nearestSearch;
    private readonly IndexSearcher _indexSearcher;
    private float _maximumDistance;
    private readonly FieldMetadata _metadata;
    public bool IsBoosting { get; init; }
    
    
    private bool _resultsNotPersisted = true;
    private bool _returnedAllResults = false;
    private int _positionOnPersistedValues = 0;
    private GrowableBuffer<long, Constant<long>> _matches;
    private GrowableBuffer<float, Constant<float>> _distances;
    private readonly bool _isEmpty;
    public VectorSearchMatch(IndexSearcher searcher, in FieldMetadata metadata, VectorValue vectorToSearch, in float minimumMatch, in int numberOfCandidates, in bool isExact)
    {
        _metadata = metadata;
        _indexSearcher = searcher;
        _nearestSearch = isExact == false
            ? Hnsw.ApproximateNearest(searcher.Transaction.LowLevelTransaction, metadata.FieldName, numberOfCandidates, vectorToSearch.GetEmbedding())
            : Hnsw.ExactNearest(searcher.Transaction.LowLevelTransaction, metadata.FieldName, numberOfCandidates, vectorToSearch.GetEmbedding());

        vectorToSearch.Dispose(); // release memory from querying since search has own copy
        if (_nearestSearch.IsEmpty)
        {
            _isEmpty = true;
            return;
        }
        
        _maximumDistance = _nearestSearch.MinimumSimilarityToMaximumDistance(minimumMatch);
        IsBoosting = metadata.HasBoost;
    }

    public long Count { get; private set; }

    public SkipSortingResult AttemptToSkipSorting()
    {
        return SkipSortingResult.ResultsNativelySorted;
    }

    public QueryCountConfidence Confidence => QueryCountConfidence.Low;

    
    public int Fill(Span<long> matches)
    {
        if (_isEmpty)
            return 0;
        
        if (IsBoosting == false)
            return FillDiscardSimilarity(matches);

        if (_resultsNotPersisted)
            FillAndPersistResults();

        var resultsLeft = _matches.Count - _positionOnPersistedValues;
        if (resultsLeft == 0)
            return 0;

        var amountToCopy = Math.Min(resultsLeft, matches.Length);
        _matches.Results.Slice(_positionOnPersistedValues,  amountToCopy).CopyTo(matches.Slice(0, amountToCopy));
        _positionOnPersistedValues += amountToCopy;
        return amountToCopy;
    }

    private int FillDiscardSimilarity(Span<long> matches)
    {
        if (_returnedAllResults)
            return 0;
        
        if (_distances.Capacity < sizeof(float) * matches.Length)
            CreateDistanceBuffer(matches.Length);

        var idX = 0;
        var workingBuffer = matches;
        
        var read = 0;
        while (workingBuffer.IsEmpty == false && (read = _nearestSearch.Fill(workingBuffer, _distances.GetSpace())) != 0)
        {
            var currentBatch = DiscardDocumentUnderSimilarity(workingBuffer.Slice(0, read), _distances.GetSpace().Slice(0, read));
            workingBuffer = workingBuffer.Slice(currentBatch);
            idX += currentBatch;
        }

        if (read == 0)
        {
            _distances.Dispose();
            _nearestSearch.Dispose(); //no more matches
            _returnedAllResults = true;
        }

        // Fill have to return sorted array
        Sort.Run(matches.Slice(0, idX));
        return idX;
    }

    private int DiscardDocumentUnderSimilarity(Span<long> matches, Span<float> distances)
    {
        ref var distanceRef = ref MemoryMarshal.GetReference(distances);
        ref var matchesRef = ref MemoryMarshal.GetReference(matches);
        int idX = 0;
        for (var i = 0; i < matches.Length; ++i)
        {
            if (Unsafe.Add(ref distanceRef, i) > _maximumDistance) 
                continue;
            
            Unsafe.Add(ref matchesRef, idX) = Unsafe.Add(ref matchesRef, i);
            Unsafe.Add(ref distanceRef, idX++) = Unsafe.Add(ref distanceRef, i);
        }

        return idX;
    }
    
    private void CreateDistanceBuffer(int length)
    {
        ref var distances = ref _distances;
        distances.Init(_indexSearcher.Allocator, length);
    }

    private void FillAndPersistResults()
    {
        ref var matches = ref _matches;
        ref var distances = ref _distances;
        
        matches.Init(_indexSearcher.Allocator, 128);
        distances.Init(_indexSearcher.Allocator, 128);
        var currentRead = 0;
        do
        {
            var mBuf = matches.GetSpace();
            var dBuf = distances.GetSpace();
            currentRead = _nearestSearch.Fill(mBuf, dBuf);
            var matched = DiscardDocumentUnderSimilarity(mBuf.Slice(0, currentRead), dBuf.Slice(0, currentRead));
            matches.AddUsage(matched);
            distances.AddUsage(matched);
        } while (currentRead != 0);
            
        matches.Results.Sort(distances.Results);
        _nearestSearch.Dispose();
        _resultsNotPersisted = false;
    }
    
    public int AndWith(Span<long> buffer, int matches)
    {
        if (_isEmpty)
            return 0;
        
        if (_resultsNotPersisted)
            FillAndPersistResults();

        return MergeHelper.And(buffer, buffer.Slice(0, matches), _matches.Results);
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (_isEmpty)
            return;
        
        ref var matchesRef = ref MemoryMarshal.GetReference(matches);
        ref var scoresRef = ref MemoryMarshal.GetReference(scores);
        ref var distanceRef = ref MemoryMarshal.GetReference(_distances.Results);
        
        for (var i = 0; i < matches.Length; ++i)
        {
            var match = Unsafe.Add(ref matchesRef, i);
            var pos = _matches.Results.BinarySearch(match);
            if (pos < 0)
                continue;
           
            Unsafe.Add(ref scoresRef, i) = _nearestSearch.DistanceToScore(Unsafe.Add(ref distanceRef, pos));
        }
        
        _matches.Dispose();
        _distances.Dispose();
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode(nameof(VectorSearchMatch),
            parameters: new Dictionary<string, string>()
            {
                { Constants.QueryInspectionNode.FieldName, _metadata.FieldName.ToString() },
                { nameof(Hnsw.SimilarityMethod), _nearestSearch.SimilarityMethod.ToString() },
                { nameof(_maximumDistance), _maximumDistance.ToString() }
            });
    }

    public string DebugView => Inspect().ToString();
}

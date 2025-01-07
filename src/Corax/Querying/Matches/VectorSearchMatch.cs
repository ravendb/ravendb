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
    private readonly FieldMetadata _metadata;
    public bool IsBoosting { get; init; }
    
    
    private bool _resultsNotPersisted = true;
    private bool _returnedAllResults = false;
    private int _positionOnPersistedValues = 0;
    private GrowableBuffer<long, Constant<long>> _matches;
    private GrowableBuffer<float, Constant<float>> _distances;
    private readonly bool _isEmpty;
    
    /// <summary>
    /// When VectorSearch is the only condition in the WHERE statement,
    /// do not sort to fulfill the Fill guarantees.
    /// Otherwise, sorting is necessary as it may produce incorrect results in the upper AST statements.
    /// </summary>
    private readonly bool _singleVectorSearchDoNotSort;

    public VectorSearchMatch(IndexSearcher searcher, in FieldMetadata metadata, VectorValue vectorToSearch, in float minimumMatch, in int numberOfCandidates, in bool isExact, in bool singleVectorSearchDoNotSort)
    {
        _singleVectorSearchDoNotSort = singleVectorSearchDoNotSort;
        _metadata = metadata;
        _indexSearcher = searcher;
        _nearestSearch = isExact == false
            ? Hnsw.ApproximateNearest(searcher.Transaction.LowLevelTransaction, metadata.FieldName, numberOfCandidates, vectorToSearch.GetEmbedding(), minimumMatch)
            : Hnsw.ExactNearest(searcher.Transaction.LowLevelTransaction, metadata.FieldName, numberOfCandidates, vectorToSearch.GetEmbedding(), minimumMatch);

        vectorToSearch.Dispose();
        if (_nearestSearch.IsEmpty)
        {
            _isEmpty = true;
            return;
        }
        
        IsBoosting = metadata.HasBoost;
    }

    public long Count { get; private set; }

    public SkipSortingResult AttemptToSkipSorting()
    {
        return _singleVectorSearchDoNotSort 
            ? SkipSortingResult.ResultsNativelySorted 
            : SkipSortingResult.SortingIsRequired;
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

        var read = _nearestSearch.Fill(matches, _distances.GetSpace());
        if (read == 0)
        {
            _distances.Dispose();
            _nearestSearch.Dispose(); //no more matches
            _returnedAllResults = true;
            return 0;
        }
        
        // We've to sort inner batch and remove duplicates
        Sort.Run(matches[..read]);
        return RemoveDuplicates(matches[..read], _distances.GetSpace()[..read]);
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
            Debug.Assert(mBuf.Length == dBuf.Length, "mBuf.Length == dBuf.Length");
            
            currentRead = _nearestSearch.Fill(mBuf, dBuf);
            matches.AddUsage(currentRead);
            distances.AddUsage(currentRead);
        } while (currentRead != 0);

        if (_singleVectorSearchDoNotSort == false)
        {
            matches.Results.Sort(distances.Results);
        
            //Truncate the buffer to the actual size
            var matchesCount = RemoveDuplicates(matches.Results, distances.Results);
            distances.Truncate(matchesCount);
            matches.Truncate(matchesCount);
        }
        
        _nearestSearch.Dispose();
        _resultsNotPersisted = false;
    }

    /// <summary>
    /// Deduplicates the original matches buffer and retains the lowest distance value among duplicates.
    /// </summary>
    /// <param name="matches">sorted matches</param>
    /// <param name="distances">Distances buffer</param>
    /// <returns>Unique elements.</returns>
    internal static int RemoveDuplicates(Span<long> matches, Span<float> distances)
    {
        Debug.Assert(IsSorted(matches));
        if (matches.Length <= 1)
            return matches.Length;

        var outputPos = 0;
        ref var matchesRef = ref MemoryMarshal.GetReference(matches);
        ref var distancesRef = ref MemoryMarshal.GetReference(distances);
        
        for (int i = 0; i < matches.Length - 1; ++i)
        {
            var currentMatch = Unsafe.Add(ref matchesRef, i);
            var nextMatch = Unsafe.Add(ref matchesRef, i + 1);
            if (currentMatch == nextMatch)
            {
                ref var nextDistance = ref Unsafe.Add(ref distancesRef, i + 1);
                nextDistance = MathF.Min(
                    Unsafe.Add(ref distancesRef, i), 
                    nextDistance);
                continue;
            }
            
            Unsafe.Add(ref matchesRef, outputPos) = currentMatch;
            Unsafe.Add(ref distancesRef, outputPos) = Unsafe.Add(ref distancesRef, i);
            outputPos++;
        }
        
        Unsafe.Add(ref matchesRef, outputPos) = Unsafe.Add(ref matchesRef, matches.Length - 1);
        Unsafe.Add(ref distancesRef, outputPos) = Unsafe.Add(ref distancesRef, matches.Length - 1);
        outputPos++;
        
        return outputPos;
    }

    private static bool IsSorted(Span<long> matches)
    {
        if (matches.Length <= 1)
            return true;

        for (int i = 1; i < matches.Length; ++i)
        {
            if (matches[i - 1] > matches[i])
                return false;
        }

        return true;
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

        if (_singleVectorSearchDoNotSort == false)
        {
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
        }
        else
        {
            _distances.Results[..scores.Length].CopyTo(scores);
            _nearestSearch.DistancesToScores(scores);
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
            });
    }

    public string DebugView => Inspect().ToString();
}

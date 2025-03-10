using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow.Server.Utils;
using Voron.Data.Graphs;

namespace Corax.Querying.Matches;

public struct MultiVectorSearchMatch : IQueryMatch
{
    private readonly IndexSearcher _searcher;
    private readonly FieldMetadata _metadata;
    private readonly bool _singleVectorSearchDoNotSortByIds;
    private readonly Hnsw.NearestSearch[] _nearestSearches;
    private readonly bool _isEmpty;
    private GrowableBuffer<long, Constant<long>> _matches;
    private GrowableBuffer<float, Constant<float>> _distances;
    private bool _persisted = false;
    private int _positionOnPersistedValues = 0;

    public MultiVectorSearchMatch(IndexSearcher searcher, in FieldMetadata metadata, in VectorValue[] vectorsToSearch, in float minimumMatch, in int numberOfCandidates,
        in bool isExact, in bool singleVectorSearchDoNotSortByIds)
    {
        _searcher = searcher;
        _metadata = metadata;
        _singleVectorSearchDoNotSortByIds = singleVectorSearchDoNotSortByIds;
        _nearestSearches = new Hnsw.NearestSearch[vectorsToSearch.Length];
        _isEmpty = true;
        for (var i = 0; i < vectorsToSearch.Length; ++i)
        {
            var vectorToSearch = vectorsToSearch[i];
            _nearestSearches[i] = isExact == false
                ? Hnsw.ApproximateNearest(searcher.Transaction.LowLevelTransaction, metadata.FieldName, numberOfCandidates, vectorToSearch.GetEmbedding(),
                    minimumMatch)
                : Hnsw.ExactNearest(searcher.Transaction.LowLevelTransaction, metadata.FieldName, numberOfCandidates, vectorToSearch.GetEmbedding(),
                    minimumMatch);

            _isEmpty &= _nearestSearches[i].IsEmpty;
            vectorToSearch.Dispose();
        }

        IsBoosting = true;
    }

    public long Count { get; private set; }

    public SkipSortingResult AttemptToSkipSorting()
    {
        return _singleVectorSearchDoNotSortByIds
            ? SkipSortingResult.ResultsNativelySorted
            : SkipSortingResult.SortingIsRequired;
    }

    public QueryCountConfidence Confidence => QueryCountConfidence.Low;
    public bool IsBoosting { get; }

    public int Fill(Span<long> matches)
    {
        if (_isEmpty)
            return 0;

        if (_persisted == false)
            FillAndPersistResults();

        if (_positionOnPersistedValues == _matches.Count)
            return 0;

        var amountToCopy = Math.Min(matches.Length, _matches.Count - _positionOnPersistedValues);
        _matches.Results.Slice(_positionOnPersistedValues, amountToCopy).CopyTo(matches[..amountToCopy]);
        _positionOnPersistedValues += amountToCopy;
        return amountToCopy;
    }

    private void FillAndPersistResults()
    {
        _matches.Init(_searcher.Allocator, 16);
        _distances.Init(_searcher.Allocator, 16);
        for (var i = 0; i < _nearestSearches.Length; ++i)
        {
            ref var nearestSearch = ref _nearestSearches[i];
            int read = 0;
            do
            {
                var matchBuffer = _matches.GetSpace();
                var distanceBuffer = _distances.GetSpace();
                Debug.Assert(matchBuffer.Length == distanceBuffer.Length, "matchBuffer.Length == distanceBuffer.Length");

                read = nearestSearch.Fill(matchBuffer, distanceBuffer);
                _matches.AddUsage(read);
                _distances.AddUsage(read);
                Count += read;
            } while (read > 0);

            nearestSearch.Dispose();
        }

        var uniqueCount = Sorting.SortAndRemoveDuplicates(_matches.Results, _distances.Results);
        _matches.Truncate(uniqueCount);
        _distances.Truncate(uniqueCount);

        if (_singleVectorSearchDoNotSortByIds)
            _distances.Results.Sort(_matches.Results);

        _persisted = true;
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        if (_isEmpty)
            return 0;

        if (_persisted == false)
            FillAndPersistResults();

        return MergeHelper.And(buffer, buffer[..matches], _matches.Results);
    }

    public void Score(Span<long> matches, Span<float> scores, float _)
    {
        Debug.Assert(_persisted, "Score() should be called after Fill() or AndWith()");
        if (_singleVectorSearchDoNotSortByIds == false)
        {
            for (int i = 0; i < matches.Length; ++i)
            {
                var match = matches[i];
                var pos = _matches.Results.BinarySearch(match);
                if (pos < 0)
                    continue;

                var distance = _distances.Results[pos];
                scores[i] += _nearestSearches[0].DistanceToScore(distance);
            }
        }
        else
        {
            _distances.Results[..scores.Length].CopyTo(scores);
            _nearestSearches[0].DistancesToScores(scores);
        }

        _matches.Dispose();
        _distances.Dispose();
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode(nameof(MultiVectorSearchMatch),
            parameters: new Dictionary<string, string>()
            {
                { Constants.QueryInspectionNode.FieldName, _metadata.FieldName.ToString() },
                { nameof(Hnsw.SimilarityMethod), _nearestSearches[0].SimilarityMethod.ToString() },
            });
    }
}

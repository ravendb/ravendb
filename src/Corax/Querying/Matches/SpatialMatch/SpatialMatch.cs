using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Utils.Spatial;
using Sparrow.Server;
using Spatial4n.Context;
using Spatial4n.Shapes;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Util;
using SpatialRelation = Spatial4n.Shapes.SpatialRelation;

namespace Corax.Querying.Matches.SpatialMatch;

public sealed class SpatialMatch<TBoosting> : IQueryMatch
    where TBoosting : IBoostingMarker
{
    private readonly Querying.IndexSearcher _indexSearcher;
    private readonly SpatialContext _spatialContext;
    private readonly double _error;
    private readonly IShape _shape;
    private Page _lastPage;
    private Point _point;
    private readonly CompactTree _tree;
    private readonly FieldMetadata _field;
    private IEnumerator<(string Geohash, bool isTermMatch)> _termGenerator;
    private TermMatch _currentMatch;
    private readonly ByteStringContext _allocator;
    private readonly Utils.Spatial.SpatialRelation _spatialRelation;
    private readonly CancellationToken _token;
    private bool _isTermMatch;
    private IDisposable _startsWithDisposeHandler;
    private HashSet<long> _alreadyReturned;
    private long _fieldRootPage;
    private SpatialScore _spatialScore;
    private double _xShapeCenter;
    private double _yShapeCenter;
    

    public SpatialMatch(Querying.IndexSearcher indexSearcher, ByteStringContext allocator, SpatialContext spatialContext, in FieldMetadata field, IShape shape,
        CompactTree tree,
        double errorInPercentage, Utils.Spatial.SpatialRelation spatialRelation, CancellationToken token)
    {
        _spatialScore = default;
        if (typeof(TBoosting) == typeof(HasBoosting))
            _spatialScore.Init(allocator);
        
        _indexSearcher = indexSearcher;
        _spatialContext = spatialContext ?? throw new ArgumentNullException($"{nameof(spatialContext)} passed to {nameof(SpatialMatch)} is null.");
        _field = field;
        _error = SpatialUtils.GetErrorFromPercentage(spatialContext, shape, errorInPercentage);
        _shape = shape;
        _tree = tree;
        _allocator = allocator;
        _spatialRelation = spatialRelation;
        _token = token;
        (_xShapeCenter, _yShapeCenter) = (shape.Center.X, shape.Center.Y);
        
        _termGenerator = spatialRelation == Utils.Spatial.SpatialRelation.Disjoint 
            ? SpatialUtils.GetGeohashesForQueriesOutsideShape(_indexSearcher, tree, allocator, spatialContext, shape).GetEnumerator() 
            : SpatialUtils.GetGeohashesForQueriesInsideShape(_indexSearcher, tree, allocator, spatialContext, shape).GetEnumerator();
        GoNextMatch();
        _point = new Point(0, 0, spatialContext);
        _fieldRootPage = _indexSearcher.FieldCache.GetLookupRootPage(field.FieldName);
    }

    private bool GoNextMatch()
    {
        if (_termGenerator.MoveNext())
        {
            var result = _termGenerator.Current;
            _startsWithDisposeHandler?.Dispose();
            _startsWithDisposeHandler = Slice.From(_allocator, result.Geohash, out var term);
            _isTermMatch = result.isTermMatch;
            _currentMatch = _indexSearcher.TermQuery(_field, term, _tree);

            return true;
        }
        _currentMatch = TermMatch.CreateEmpty(_indexSearcher, _indexSearcher.Allocator);
        return false;
    }

    public long Count => long.MaxValue;

    public SkipSortingResult AttemptToSkipSorting() => SkipSortingResult.WillSkipSorting;
    public QueryCountConfidence Confidence => QueryCountConfidence.Low;
    public bool IsBoosting => false;

    public int Fill(Span<long> matches)
    {
        int currentIdx = 0;
        do
        {
            int read;
            if ((read = _currentMatch.Fill(matches.Slice(currentIdx))) == 0)
            {
                if (GoNextMatch() == false)
                {
                    break;
                }

                continue;
            }

            if (_isTermMatch)
            {
                currentIdx += read;
            }
            else if (read > 0)
            {
                var slicedMatches = matches.Slice(currentIdx);
                for (int i = 0; i < read; ++i)
                {
                    if (CheckEntryManually(slicedMatches[i]))
                    {
                        matches[currentIdx++] = slicedMatches[i];
                    }
                }
            }
        } while (currentIdx != matches.Length);

        return currentIdx;
    }

    private bool CheckEntryManually(long id)
    {
        if (_alreadyReturned?.TryGetValue(id, out var _) ?? false)
        {
            return false;
        }
        _alreadyReturned ??= new HashSet<long>();

        using var _ = _indexSearcher.Transaction.LowLevelTransaction.AcquireCompactKey(out var existingKey);

        _indexSearcher.GetEntryTermsReader(id, ref _lastPage, out var termsReader, existingKey);
        while (termsReader.MoveNextSpatial())
        {
            if(termsReader.FieldRootPage != _fieldRootPage)
                continue;
            
            _point.Reset(termsReader.Longitude, termsReader.Latitude);
            if (!IsTrue(_point.Relate(_shape))) 
                continue;
            
            if (_alreadyReturned.Add(id) && typeof(TBoosting) == typeof(HasBoosting))
            {
                ref var spatialScore = ref _spatialScore;
                spatialScore.Push(id, (float)SpatialUtils.HaverstineDistanceInInternationalNauticalMiles(_yShapeCenter, _xShapeCenter, termsReader.Longitude, termsReader.Latitude));
            }
                
            return true;
        }
        
        return false;
    }

    private bool IsTrue(SpatialRelation answer) => answer switch
    {
        SpatialRelation.Within or SpatialRelation.Contains => _spatialRelation is Utils.Spatial.SpatialRelation.Within
            or Utils.Spatial.SpatialRelation.Contains,
        SpatialRelation.Disjoint => _spatialRelation is Utils.Spatial.SpatialRelation.Disjoint,
        SpatialRelation.Intersects => _spatialRelation is Utils.Spatial.SpatialRelation.Intersects,
        _ => throw new NotSupportedException()
    };

    public int AndWith(Span<long> buffer, int matches)
    {
        var currentIdx = 0;
        for (int i = 0; i < matches; ++i)
        {
            if (i % 1024 == 0)
                _token.ThrowIfCancellationRequested();
            if (CheckEntryManually(buffer[i]))
            {
                buffer[currentIdx++] = buffer[i];
            }
        }

        return currentIdx;
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (typeof(TBoosting) != typeof(HasBoosting))
            ThrowPrimitiveHasNoBoostingData();
     
        _spatialScore.CalculateScore(matches, scores, boostFactor, _spatialRelation);
        _spatialScore.Dispose();
    }

    private void ThrowPrimitiveHasNoBoostingData()
    {
        throw new InvalidDataException($"{nameof(SpatialMatch<TBoosting>)}");
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode($"{nameof(SpatialMatch)}",
            parameters: new Dictionary<string, string>()
            {
                {"Field", _field.ToString()},
                {"Shape", _shape.ToString()},
                {"Error", _error.ToString(CultureInfo.InvariantCulture)},
                {"SpatialRelation", _spatialRelation.ToString()},
            });
    }
}

internal struct SpatialScore
{
    private ByteStringContext _context;
    private NativeList<long> _matches;
    private NativeList<double> _distances;
    private double _maxDistance;

    public SpatialScore()
    {
        _matches = default;
        _distances = default;
        _maxDistance = double.MinValue;
    }

    public void Init(ByteStringContext allocator)
    {
        _context = allocator;
    }

    public void Push(long id, double distance)
    {
        _matches.Add(_context, id);
        _distances.Add(_context, distance);
        _maxDistance = Math.Max(distance, _maxDistance);
    }

    public void Dispose()
    {
        _matches.Dispose(_context);
        _distances.Dispose(_context);
    }

    /// <summary>
    /// Calculates relevance by distance to the center of the figure. When spatial relation is not disjoint, we treat the center as the most relevant point and grant it a score of 1.01 (*boostFactor). 
    /// We take the whole result set as a subset, so the farthest point returned by this query is the least relevant point and gets a score of 0.01 (just not being 0). 
    /// Scores for points in between are just proportions between the center and the farthest.
    ///
    /// On the other hand, when the query is DISJOINT, we negate the formula. Now the center is the least relevant, and the farthest is most relevant. In this case center is 0.01
    /// but for most queries there is impossible go get this (it's possible when center is outside body of figure). This allow us to avoid cases when points are very close to each other but gets
    /// very different scores.
    /// </summary>
    /// <param name="matches">Requires sorted, non-encoded ids</param>
    public void CalculateScore(Span<long> matches, Span<float> scores, float boostFactor, Utils.Spatial.SpatialRelation spatialRelation)
    {
        const double bias = 0.01;
        if (_maxDistance < double.Epsilon)
            return;
        
        var results = _matches.ToSpan();
        var distances = _distances.ToSpan();
        
        for (int idX = 0; idX < results.Length; ++idX)
        {
            var incomingIdx = matches.BinarySearch(results[idX]);
            if (incomingIdx < 0) continue;
            
            var relativeDistance = bias +
                                   (spatialRelation is not Utils.Spatial.SpatialRelation.Disjoint
                                       ? 1.0 - (distances[idX] / _maxDistance)
                                       : (distances[idX] / _maxDistance));
            scores[incomingIdx] += (float)relativeDistance * boostFactor;
        }
    }
}

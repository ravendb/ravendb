using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Utils.Spatial;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Spatial4n.Context;
using Spatial4n.Shapes;
using Voron;
using Voron.Data.CompactTrees;
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
    // entry -> distance to the shape centre, -1 when it has no matching point
    private Dictionary<long, double> _alreadyReturned;
    private double _maxDistance; // the farthest entry this match returned - the least relevant one, which Score grades against
    private long _fieldRootPage;
    private double _xShapeCenter;
    private double _yShapeCenter;
    

    public SpatialMatch(Querying.IndexSearcher indexSearcher, ByteStringContext allocator, SpatialContext spatialContext, in FieldMetadata field, IShape shape,
        CompactTree tree,
        double errorInPercentage, Utils.Spatial.SpatialRelation spatialRelation, CancellationToken token)
    {
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

    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.Possible;
    
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

    public long Count => _indexSearcher.NumberOfEntries;

    public SkipSortingResult AttemptToSkipSorting() => SkipSortingResult.WillSkipSorting;
    public QueryCountConfidence Confidence => QueryCountConfidence.Low;
    public bool IsBoosting => typeof(TBoosting) == typeof(HasBoosting);

    public int Fill(Span<long> matches)
    {
        int iterations = 1;
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

                iterations++;
                continue;
            }

            if (_isTermMatch)
            {
                // the cell is entirely inside the shape, so every entry is a match; Score still needs a distance for each
                if (typeof(TBoosting) == typeof(HasBoosting))
                    RecordDistances(matches.Slice(currentIdx, read));
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

        
        if (iterations > 1)
            currentIdx = Sorting.SortAndRemoveDuplicates(matches.Slice(0, currentIdx));
        
        return currentIdx;
    }

    private bool CheckEntryManually(long id)
    {
        if (_alreadyReturned?.ContainsKey(id) ?? false)
        {
            return false;
        }
        _alreadyReturned ??= new Dictionary<long, double>();
        if (TryGetMatchingPoint(id, out var latitude, out var longitude) == false)
            return false;

        // the entry is open right here, so keep its distance for Score instead of reading it again there
        Record(id, typeof(TBoosting) == typeof(HasBoosting) ? DistanceFromCentre(latitude, longitude) : 0);
        return true;
    }

    // Every entry this match returns has to reach Score with its distance measured, so an entry taken in bulk is opened
    // here, once - the read Score would otherwise do for it, minus the repeat for an entry that two cells share.
    private void RecordDistances(Span<long> ids)
    {
        _alreadyReturned ??= new Dictionary<long, double>();
        for (int i = 0; i < ids.Length; ++i)
        {
            if (i % 1024 == 0)
                _token.ThrowIfCancellationRequested();

            var id = ids[i];
            if (_alreadyReturned.ContainsKey(id))
                continue;

            Record(id, TryGetMatchingPoint(id, out var latitude, out var longitude) ? DistanceFromCentre(latitude, longitude) : -1);
        }
    }

    private void Record(long id, double distance)
    {
        _alreadyReturned.Add(id, distance);
        if (distance > _maxDistance)
            _maxDistance = distance;
    }

    private double DistanceFromCentre(double latitude, double longitude)
        => SpatialUtils.HaverstineDistanceInInternationalNauticalMiles(_yShapeCenter, _xShapeCenter, latitude, longitude);

    // The first point of the entry that satisfies the relation.
    private bool TryGetMatchingPoint(long id, out double latitude, out double longitude)
    {
        var termsReader = _indexSearcher.GetEntryTermsReader(id, ref _lastPage);
        while (termsReader.MoveNextSpatial())
        {
            if (termsReader.FieldRootPage != _fieldRootPage)
                continue;
            _point.Reset(termsReader.Longitude, termsReader.Latitude);
            if (IsTrue(_point.Relate(_shape)))
            {
                (latitude, longitude) = (termsReader.Latitude, termsReader.Longitude);
                return true;
            }
        }

        latitude = longitude = default;
        return false;
    }

    // Corax indexes points only, so a point inside the shape satisfies within, contains and intersects alike.
    private bool IsTrue(SpatialRelation answer) => answer switch
    {
        SpatialRelation.Within or SpatialRelation.Contains or SpatialRelation.Intersects
            => _spatialRelation is not Utils.Spatial.SpatialRelation.Disjoint,
        SpatialRelation.Disjoint => _spatialRelation is Utils.Spatial.SpatialRelation.Disjoint,
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

    /// <summary>
    /// Calculates relevance by distance to the center of the figure. When spatial relation is not disjoint, we treat the center as the most relevant point and grant it a score of 1.01 (*boostFactor).
    /// We take the whole result set as a subset, so the farthest point returned by this query is the least relevant point and gets a score of 0.01 (just not being 0).
    /// Scores for points in between are just proportions between the center and the farthest.
    ///
    /// On the other hand, when the query is DISJOINT, we negate the formula. Now the center is the least relevant, and the farthest is most relevant. In this case center is 0.01
    /// but for most queries there is impossible go get this (it's possible when center is outside body of figure). This allow us to avoid cases when points are very close to each other but gets
    /// very different scores.
    /// </summary>
    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (typeof(TBoosting) != typeof(HasBoosting))
            ThrowPrimitiveHasNoBoostingData();

        // Fill and AndWith recorded every entry this match returned together with its distance and kept the farthest one,
        // so this is a single pass with nothing read and nothing allocated. A miss means the entry came from the other
        // side of an OR and is not ours.
        if (_maxDistance == 0) // nothing returned, or every matched point sits at the centre - nothing to grade
            return;

        const double bias = 0.01;
        for (int i = 0; i < matches.Length; ++i)
        {
            if (_alreadyReturned.TryGetValue(matches[i], out var distance) == false || distance < 0)
                continue;

            var relativeDistance = bias +
                                   (_spatialRelation is not Utils.Spatial.SpatialRelation.Disjoint
                                       ? 1.0 - (distance / _maxDistance)
                                       : (distance / _maxDistance));
            scores[i] += (float)relativeDistance * boostFactor;
        }
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

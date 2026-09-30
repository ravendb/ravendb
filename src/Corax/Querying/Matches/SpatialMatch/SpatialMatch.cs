using System;
using System.Collections.Generic;
using System.Globalization;
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
using Voron.Data.RoaringBitmaps;
using SpatialRelation = Spatial4n.Shapes.SpatialRelation;

namespace Corax.Querying.Matches.SpatialMatch;

public sealed class SpatialMatch<TBoosting> : IPostFilterMatch, ISpatialFilterQuery, IDisposable
    where TBoosting : IBoostingMarker
{
    private const int CandidateToIndexRatio = 32; // cost of reading a candidate entry vs. scanning the index 
    /// <summary>Set by <c>QueryPlanBuilder.ApplyPostFilters</c> when this spatial match was lifted to a top-level
    /// post-filter. Left false when it is an ordinary leaf inside an OR branch.</summary>
    public bool IsPostFilter { get; set; }

    public IQueryMatch FilterQuery
    {
        get => _filterQuery;
        set => _filterQuery = value;
    }

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
    private IQueryMatch _filterQuery;

    private RoaringBitmap _filterBitmap;
    private RoaringBitmapIterator _filterIterator;
    private bool _filterInitialized;
    private bool _ownsFilterBitmap;
    private bool _driveByShape;
    private bool _disposed;

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
        _currentMatch = TermMatch.CreateEmpty();
        return false;
    }

    public long Count => -1;

    public bool IsBoosting => typeof(TBoosting) == typeof(HasBoosting);

    public int Fill(Span<long> matches)
    {
        if (_filterQuery != null)
        {
            EnsureFilterInitialized();
            if (_driveByShape == false)
                return FillCandidateDriven(matches);
        }

        return FillShapeDriven(matches);
    }

    private void EnsureFilterInitialized()
    {
        if (_filterInitialized)
            return;
        _filterInitialized = true;

        _filterBitmap = IndexSearcher.VectorSearchUtils.LoadFilterMatches(_indexSearcher, ref _filterQuery, out _ownsFilterBitmap);

        long candidateCount = _filterBitmap.ComputeCount();
        long indexSize = _indexSearcher.NumberOfEntries;

        bool driveByCandidates = candidateCount <= SpatialUtils.Threshold || candidateCount * CandidateToIndexRatio < indexSize;
        _driveByShape = driveByCandidates == false;

        if (driveByCandidates)
            _filterIterator = _filterBitmap.GetIterator();
    }

    private int FillCandidateDriven(Span<long> matches)
    {
        while (true)
        {
            int read = _filterIterator.Fill(ref _filterBitmap, matches);
            if (read == 0)
                return 0;

            int w = 0;
            for (int i = 0; i < read; ++i)
            {
                if ((i & 1023) == 0)
                    _token.ThrowIfCancellationRequested();

                if (CheckEntryManually(matches[i]))
                    matches[w++] = matches[i];
            }

            if (w > 0)
                return w;
        }
    }

    private int FillShapeDriven(Span<long> matches)
    {
        int currentIdx = 0;
        do
        {
            _token.ThrowIfCancellationRequested();

            int read;
            if ((read = _currentMatch.Fill(matches.Slice(currentIdx))) == 0)
            {
                if (GoNextMatch() == false)
                    break;

                continue;
            }

            var slicedMatches = matches.Slice(currentIdx);
            // Scoped to a candidate set: drop non-candidates (compacted to the front) before any geo work.
            int kept = _driveByShape ? _filterBitmap.AndWith(slicedMatches[..read], read) : read;

            if (_isTermMatch)
            {
                // the cell is entirely inside the shape, so every (surviving) entry is a match; Score still needs a distance for each
                if (typeof(TBoosting) == typeof(HasBoosting))
                    RecordDistances(slicedMatches[..kept]);
                currentIdx += kept;
            }
            else
            {
                for (int i = 0; i < kept; ++i)
                {
                    if ((i & 1023) == 0)
                        _token.ThrowIfCancellationRequested();

                    if (CheckEntryManually(slicedMatches[i]))
                        matches[currentIdx++] = slicedMatches[i];
                }
            }
        } while (currentIdx != matches.Length);

        // A single Fill spans several geohash cells (each sorted on its own, but concatenated across cells), and an
        // entry can appear in both an interior and a boundary cell within the page. IQueryMatch.Fill must return
        // sorted, unique ids per call, so normalize the page before returning.
        return Sorting.SortAndRemoveDuplicates(matches[..currentIdx]);
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

    private bool IsTrue(SpatialRelation answer) => answer switch
    {
        // RavenDB-27508: Corax indexes points only, so a point inside the shape satisfies within, contains and
        // intersects alike. Anything but Disjoint is a match for all three.
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

    // Spatial scoring is distance-based per entry, independent of order; no sorted fast path.
    public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor) => Score(matches, scores, boostFactor);

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
        // CompiledQueryMatch.Score calls every resolved leaf, negated ones included, and a negated leaf is resolved
        // without boost - it has no score data to hand over. Same contract as TermMatch.Score.
        if (typeof(TBoosting) != typeof(HasBoosting))
            return;

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

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode($"{nameof(SpatialMatch)}",
            parameters: new Dictionary<string, string>()
            {
                {"Field", _field.FieldName.ToString()},
                {"Shape", _shape.ToString()},
                {"Error", _error.ToString(CultureInfo.InvariantCulture)},
                {"SpatialRelation", _spatialRelation.ToString()},
            })
        {
            // Reflects the lifting decision recorded on this match, not the type: a spatial leaf inside an OR is
            // a pipeline leaf, not a post-filter (see IPostFilterMatch).
            IsPostFilter = IsPostFilter
        };
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;

        // Iterator first, then the bitmap it was built over (reverse allocation order). The iterator is default
        // (a no-op Dispose) unless the candidate-driven path created it; the bitmap is disposed only when owned
        // — a borrowed IBitmapQueryMatch bitmap belongs to its source.
        _filterIterator.Dispose();
        if (_ownsFilterBitmap)
            _filterBitmap.Dispose();

        _startsWithDisposeHandler?.Dispose();
        _termGenerator?.Dispose();
    }
}

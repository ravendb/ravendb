using Corax.Utils;
using Corax.Utils.Spatial;
using Voron;

namespace Corax.Querying.Matches.SortingMatches.Meta;

/// <summary>
/// Calculates the distance of an entry from the ordering center. Uses either the single point that is kept per entry for sorting,
/// or (when <see cref="OrderMetadata.UseNearestPoint"/> is set) the nearest of all the points the entry has in the field.
/// </summary>
internal struct SpatialDistanceReader
{
    private readonly IndexSearcher _searcher;
    private readonly SpatialReader _reader;
    private readonly bool _useNearestPoint;
    private readonly long _fieldRootPage;
    private readonly (double X, double Y) _center;
    private readonly double _round;
    private readonly SpatialUnits _units;
    private Page _lastPage;

    public SpatialDistanceReader(IndexSearcher searcher, in OrderMetadata orderMetadata)
    {
        _searcher = searcher;
        _reader = searcher.SpatialReader(orderMetadata.Field.FieldName);
        _useNearestPoint = orderMetadata.UseNearestPoint;
        _fieldRootPage = _useNearestPoint && _reader.IsValid ? searcher.FieldCache.GetLookupRootPage(orderMetadata.Field.FieldName) : -1;
        _center = (orderMetadata.Point.X, orderMetadata.Point.Y);
        _round = orderMetadata.Round;
        _units = orderMetadata.Units;
        _lastPage = default;
    }

    public bool IsValid => _reader.IsValid;

    public bool TryGetDistance(long entryId, out SpatialResult result)
    {
        if (_useNearestPoint == false)
        {
            if (_reader.TryGetSpatialPoint(entryId, out var coords) == false)
            {
                result = default;
                return false;
            }

            result = new SpatialResult { Distance = SpatialUtils.GetGeoDistance(coords, _center, _round, _units), Latitude = coords.Lat, Longitude = coords.Lng };
            return true;
        }

        var found = false;
        result = default;

        var termsReader = _searcher.GetEntryTermsReader(entryId, ref _lastPage);
        while (termsReader.FindNextSpatial(_fieldRootPage))
        {
            var coordinates = (termsReader.Latitude, termsReader.Longitude);
            var distance = SpatialUtils.GetGeoDistance(coordinates, _center, _round, _units);
            if (found && distance >= result.Distance)
                continue;

            result = new SpatialResult { Distance = distance, Latitude = coordinates.Latitude, Longitude = coordinates.Longitude };
            found = true;
        }

        return found;
    }
}

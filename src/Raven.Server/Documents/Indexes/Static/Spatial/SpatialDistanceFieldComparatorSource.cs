using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Server.Documents.Queries;
using Spatial4n.Distance;
using Spatial4n.Shapes;

namespace Raven.Server.Documents.Indexes.Static.Spatial
{
    public sealed class SpatialDistanceFieldComparatorSource : FieldComparatorSource
    {
        private readonly IPoint _center;
        private readonly IndexQueryServerSide _query;
        private readonly double _roundFactor;
        private readonly SpatialField _spatialField;
        private readonly bool _useNearestPoint;

        public SpatialDistanceFieldComparatorSource(SpatialField spatialField, IPoint center, IndexQueryServerSide query, double roundFactor, bool useNearestPoint)
        {
            _spatialField = spatialField;
            _center = center;
            _query = query;
            _roundFactor = roundFactor;
            _useNearestPoint = useNearestPoint;
        }

        public override FieldComparator NewComparator(string fieldname, int numHits, int sortPos, bool reversed)
        {
            SpatialDistanceFieldComparator comp = _useNearestPoint
                ? new NearestPointSpatialDistanceFieldComparator(_spatialField, _center, numHits, _roundFactor, reversed)
                : new StoredShapeSpatialDistanceFieldComparator(_spatialField, _center, numHits, _roundFactor, reversed);

            if (_query.Distances == null)
                _query.Distances = comp; // we will only push the distance for the first one
            return comp;
        }

        public abstract class SpatialDistanceFieldComparator : FieldComparator
        {
            protected readonly SpatialField SpatialField;
            private readonly DistanceValue[] _values;
            private DistanceValue _bottom;
            private readonly IPoint _originPt;
            private readonly bool _isGeo;
            private readonly Dictionary<int, SpatialResult> _cache = new Dictionary<int, SpatialResult>();
            private int _currentDocBase;
            private readonly double _roundFactor;
            private readonly double _missingDistance;

            public SpatialUnits Units => SpatialField.Units;

            public SpatialResult? Get(int doc)
            {
                if (_cache.TryGetValue(doc, out var cache))
                    return cache;
                return null;
            }

            protected SpatialDistanceFieldComparator(SpatialField spatialField, IPoint origin, int numHits, double roundFactor, bool reversed)
            {
                SpatialField = spatialField;
                _values = new DistanceValue[numHits];
                _originPt = origin;
                _isGeo = SpatialField.GetContext().IsGeo;
                _roundFactor = roundFactor;
                _missingDistance = reversed ? double.MinValue : double.MaxValue;
            }

            public override int Compare(int slot1, int slot2)
            {
                var a = _values[slot1];
                var b = _values[slot2];
                if (a.Value > b.Value)
                    return 1;
                if (a.Value < b.Value)
                    return -1;

                return 0;
            }

            public override void SetBottom(int slot)
            {
                _bottom = _values[slot];
            }

            public override int CompareBottom(int doc, IState state)
            {
                var v2 = CalculateDistance(doc, state);
                if (_bottom.Value > v2)
                {
                    return 1;
                }

                if (_bottom.Value < v2)
                {
                    return -1;
                }

                return 0;
            }

            public override void Copy(int slot, int doc, IState state)
            {
                _values[slot] = new DistanceValue
                {
                    Value = CalculateDistance(doc, state)
                };
            }

            private double CalculateDistance(int doc, IState state)
            {
                var actualDocId = doc + _currentDocBase;
                if (_cache.TryGetValue(actualDocId, out var cache))
                    return GetRoundedValue(cache.Distance);

                cache = ActuallyCalculateDistance(doc, state);

                if (double.IsNaN(cache.Distance))
                    cache.Distance = _missingDistance;

                _cache[actualDocId] = cache;

                return GetRoundedValue(cache.Distance);
            }

            private double GetRoundedValue(double cache)
            {
                if (_roundFactor == 0)
                    return cache;
                return cache - cache % _roundFactor;
            }

            protected abstract SpatialResult ActuallyCalculateDistance(int doc, IState state);

            protected SpatialResult CreateResult(double latitude, double longitude)
            {
                var result = new SpatialResult
                {
                    Latitude = latitude,
                    Longitude = longitude
                };

                if (_isGeo == false)
                {
                    result.Distance = CartesianDistance(latitude, longitude, _originPt.Y, _originPt.X);
                    return result;
                }

                result.Distance = HaverstineDistanceInMiles(latitude, longitude, _originPt.Y, _originPt.X);

                switch (Units)
                {
                    case SpatialUnits.Kilometers:
                        result.Distance *= DistanceUtils.MilesToKilometers;
                        break;
                    case SpatialUnits.Miles:
                    default:
                        break;
                }

                return result;
            }

            public static double HaverstineDistanceInMiles(double lat1, double lng1, double lat2, double lng2)
            {
                // from : https://www.geodatasource.com/developers/javascript
                if ((lat1 == lat2) && (lng1 == lng2))
                {
                    return 0;
                }
                else
                {
                    var radlat1 = DistanceUtils.DegreesToRadians * lat1;
                    var radlat2 = DistanceUtils.DegreesToRadians * lat2;
                    var theta = lng1 - lng2;
                    var radtheta = DistanceUtils.DegreesToRadians * theta;
                    var dist = Math.Sin(radlat1) * Math.Sin(radlat2) + Math.Cos(radlat1) * Math.Cos(radlat2)
                        * Math.Cos(radtheta);
                    if (dist > 1)
                    {
                        dist = 1;
                    }
                    dist = Math.Acos(dist);
                    dist = dist * DistanceUtils.RadiansToDegrees;

                    const double NumberOfMilesInNauticalMile = 1.1515;
                    const double NauticalMilePerDegree = 60;
                    dist = dist * NauticalMilePerDegree * NumberOfMilesInNauticalMile;
                    return dist;
                }
            }

            public static double CartesianDistance(double lat1, double lng1, double lat2, double lng2)
            {
                double result = 0;

                double v = lat1 - lat2;
                result += (v * v);

                v = lng1 - lng2;
                result += (v * v);

                return Math.Sqrt(result);
            }

            public override void SetNextReader(IndexReader reader, int docBase, IState state)
            {
                _currentDocBase = docBase;
            }

            public override IComparable this[int slot] => _values[slot];
        }

        private sealed class StoredShapeSpatialDistanceFieldComparator : SpatialDistanceFieldComparator
        {
            private static readonly FieldSelector ShapeFieldSelector = new SpatialShapeFieldSelector();

            private IndexReader _currentIndexReader;

            public StoredShapeSpatialDistanceFieldComparator(SpatialField spatialField, IPoint origin, int numHits, double roundFactor, bool reversed)
                : base(spatialField, origin, numHits, roundFactor, reversed)
            {
            }

            protected override SpatialResult ActuallyCalculateDistance(int doc, IState state)
            {
                var document = _currentIndexReader.Document(doc, ShapeFieldSelector, state);
                if (document == null)
                    return SpatialResult.Invalid;
                var field = document.GetField(Constants.Documents.Indexing.Fields.SpatialShapeFieldName);
                if (field == null)
                    return SpatialResult.Invalid;
                var shapeAsText = field.StringValue(state);
                IShape shape;
                try
                {
                    shape = SpatialField.ReadShape(shapeAsText);
                }
                catch (InvalidOperationException)
                {
                    return SpatialResult.Invalid;
                }
                var pt = shape as IPoint;
                if (pt == null)
                    pt = shape.Center;

                return CreateResult(pt.Y, pt.X);
            }

            public override void SetNextReader(IndexReader reader, int docBase, IState state)
            {
                base.SetNextReader(reader, docBase, state);
                _currentIndexReader = reader;
            }

            private sealed class SpatialShapeFieldSelector : FieldSelector
            {
                public FieldSelectorResult Accept(string fieldName)
                {
                    return fieldName == Constants.Documents.Indexing.Fields.SpatialShapeFieldName
                        ? FieldSelectorResult.LOAD_AND_BREAK
                        : FieldSelectorResult.NO_LOAD;
                }
            }
        }

        private sealed class NearestPointSpatialDistanceFieldComparator : SpatialDistanceFieldComparator
        {
            private StringIndex _centersIndex;
            private SpatialCenters _centers;

            public NearestPointSpatialDistanceFieldComparator(SpatialField spatialField, IPoint origin, int numHits, double roundFactor, bool reversed)
                : base(spatialField, origin, numHits, roundFactor, reversed)
            {
            }

            protected override SpatialResult ActuallyCalculateDistance(int doc, IState state)
            {
                var ordinal = _centersIndex.order[doc];
                if (ordinal == 0)
                    return SpatialResult.Invalid;

                var points = _centers.GetPoints(ordinal);
                if (points.IsEmpty)
                    return SpatialResult.Invalid;

                var nearest = CreateResult(points[0].Latitude, points[0].Longitude);
                for (var i = 1; i < points.Length; i++)
                {
                    var result = CreateResult(points[i].Latitude, points[i].Longitude);
                    if (result.Distance < nearest.Distance)
                        nearest = result;
                }

                return nearest;
            }

            public override void SetNextReader(IndexReader reader, int docBase, IState state)
            {
                base.SetNextReader(reader, docBase, state);
                _centersIndex = FieldCache_Fields.DEFAULT.GetStringIndex(reader, SpatialField.LuceneCentersFieldName, state);
                _centers = SpatialCenters.For(_centersIndex);
            }
        }

        /// <summary>
        /// The parsed center points of every term of a FieldCache entry
        /// </summary>
        private sealed class SpatialCenters
        {
            private static readonly ConditionalWeakTable<StringIndex, SpatialCenters> Cache = new();
            private readonly int[] _offsets;
            private readonly (double Latitude, double Longitude)[] _points;

            private SpatialCenters(int[] offsets, (double Latitude, double Longitude)[] points)
            {
                _offsets = offsets;
                _points = points;
            }

            public static SpatialCenters For(StringIndex index) => Cache.GetValue(index, Create);

            public ReadOnlySpan<(double Latitude, double Longitude)> GetPoints(int ordinal)
            {
                var start = _offsets[ordinal];
                return new ReadOnlySpan<(double Latitude, double Longitude)>(_points, start, _offsets[ordinal + 1] - start);
            }

            private static SpatialCenters Create(StringIndex index)
            {
                var numberOfTerms = index.lookup.Length;
                var offsets = new int[numberOfTerms + 1];
                var points = new List<(double Latitude, double Longitude)>(numberOfTerms);

                for (var ordinal = 1; ordinal < numberOfTerms; ordinal++) // ordinal 0 is reserved for entries without a value
                {
                    offsets[ordinal] = points.Count;

                    var centers = index.lookup[ordinal];
                    if (centers.IsNull == false)
                        SpatialField.ParseCenters(centers.StringAsBytes, points);
                }

                offsets[numberOfTerms] = points.Count;

                return new SpatialCenters(offsets, points.ToArray());
            }
        }

        private struct DistanceValue : IComparable
        {
            public double Value;

            public override string ToString()
            {
                return Value.ToString();
            }

            public int CompareTo(object obj)
            {
                if (obj == null)
                    return 1;
                return Value.CompareTo(((DistanceValue)obj).Value);
            }
        }
    }
}

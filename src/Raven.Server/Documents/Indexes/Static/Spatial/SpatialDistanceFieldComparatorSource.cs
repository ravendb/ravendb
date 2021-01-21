using System;
using System.Collections.Generic;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Server.Documents.Queries;
using Spatial4n.Core.Distance;
using Spatial4n.Core.Shapes;

namespace Raven.Server.Documents.Indexes.Static.Spatial
{
    public class SpatialDistanceFieldComparatorSource : FieldComparatorSource
    {
        private readonly IPoint _center;
        private readonly IndexQueryServerSide _query;
        private readonly double _roundFactor;
        private readonly SpatialField _spatialField;

        public SpatialDistanceFieldComparatorSource(SpatialField spatialField, IPoint center, IndexQueryServerSide query, double roundFactor)
        {
            _spatialField = spatialField;
            _center = center;
            _query = query;
            _roundFactor = roundFactor;
        }

        public override FieldComparator NewComparator(string fieldname, int numHits, int sortPos, bool reversed)
        {
            var comp = new SpatialDistanceFieldComparator(_spatialField, _center, numHits, _roundFactor);
            if (_query.Distances == null)
                _query.Distances = comp; // we will only push the distance for the first one
            return comp;
        }

        public class SpatialDistanceFieldComparator : FieldComparator
        {
            private readonly SpatialField _spatialField;
            private readonly DistanceValue[] _values;
            private DistanceValue _bottom;
            private readonly IPoint _originPt;
            private bool _isGeo;
            private Dictionary<int, SpatialResult> _cache = new Dictionary<int, SpatialResult>();
            private int _currentDocBase;
            private double _roundFactor;

            public SpatialUnits Units => _spatialField.Units;

            private IndexReader _currentIndexReader;

            public SpatialResult? Get(int doc)
            {
                if (_cache.TryGetValue(doc, out var cache))
                    return cache;
                return null;
            }

            public SpatialDistanceFieldComparator(SpatialField spatialField, IPoint origin, int numHits, double roundFactor)
            {
                _spatialField = spatialField;
                _values = new DistanceValue[numHits];
                _originPt = origin;
                _isGeo = _spatialField.GetContext().IsGeo;
                _roundFactor = roundFactor;
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

                _cache[actualDocId] = cache;

                return GetRoundedValue(cache.Distance);
            }

            private double GetRoundedValue(double cache)
            {
                if (_roundFactor == 0)
                    return cache;
                return cache - cache % _roundFactor;
            }

            private SpatialResult ActuallyCalculateDistance(int doc, IState state)
            {
                var document = _currentIndexReader.Document(doc, state);
                if (document == null)
                    return SpatialResult.Invalid;
                var field = document.GetField(Constants.Documents.Indexing.Fields.SpatialShapeFieldName);
                if (field == null)
                    return SpatialResult.Invalid;
                var shapeAsText = field.StringValue(state);
                IShape shape;
                try
                {
                    shape = _spatialField.ReadShape(shapeAsText);
                }
                catch (InvalidOperationException)
                {
                    return SpatialResult.Invalid;
                }
                var pt = shape as IPoint;
                if (pt == null)
                    pt = shape.Center;

                var result = new SpatialResult
                {
                    Latitude = pt.Y,
                    Longitude = pt.X
                };

                if (_isGeo == false)
                {
                    result.Distance = CartesianDistance(pt.Y, pt.X, _originPt.Y, _originPt.X);
                    return result;
                }

                result.Distance = HaverstineDistanceInMiles(pt.Y, pt.X, _originPt.Y, _originPt.X);

                switch (Units)
                {
                    case SpatialUnits.Kilometers:
                        result.Distance *= DistanceUtils.MILES_TO_KM;
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
                    var radlat1 = DistanceUtils.DEGREES_TO_RADIANS * lat1;
                    var radlat2 = DistanceUtils.DEGREES_TO_RADIANS * lat2;
                    var theta = lng1 - lng2;
                    var radtheta = DistanceUtils.DEGREES_TO_RADIANS * theta;
                    var dist = Math.Sin(radlat1) * Math.Sin(radlat2) + Math.Cos(radlat1) * Math.Cos(radlat2)
                        * Math.Cos(radtheta);
                    if (dist > 1)
                    {
                        dist = 1;
                    }
                    dist = Math.Acos(dist);
                    dist = dist * DistanceUtils.RADIANS_TO_DEGREES;

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
                _currentIndexReader = reader;
                _currentDocBase = docBase;
            }

            public override IComparable this[int slot] => _values[slot];
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

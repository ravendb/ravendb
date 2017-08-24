using System;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Queries.Spatial
{
    public abstract class SpatialCriteria
    {
        private readonly SpatialRelation _relation;
        private readonly double _distanceErrorPct;

        protected SpatialCriteria(SpatialRelation relation, double distanceErrorPct)
        {
            _relation = relation;
            _distanceErrorPct = distanceErrorPct;
        }

        protected abstract ShapeToken GetShapeToken(Func<object, string> addQueryParameter);

        public QueryToken ToQueryToken(string fieldName, Func<object, string> addQueryParameter)
        {
            var shapeToken = GetShapeToken(addQueryParameter);

            QueryToken relationToken;
            switch (_relation)
            {
                case SpatialRelation.Within:
                    relationToken = WhereToken.Within(fieldName, shapeToken, _distanceErrorPct);
                    break;
                case SpatialRelation.Contains:
                    relationToken = WhereToken.Contains(fieldName, shapeToken, _distanceErrorPct);
                    break;
                case SpatialRelation.Disjoint:
                    relationToken = WhereToken.Disjoint(fieldName, shapeToken, _distanceErrorPct);
                    break;
                case SpatialRelation.Intersects:
                    relationToken = WhereToken.Intersects(fieldName, shapeToken, _distanceErrorPct);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            return relationToken;
        }
    }

    public class WktCriteria : SpatialCriteria
    {
        private readonly string _shapeWKT;

        internal WktCriteria(string shapeWKT, SpatialRelation relation, double distanceErrorPct)
            : base(relation, distanceErrorPct)
        {
            _shapeWKT = shapeWKT;
        }

        protected override ShapeToken GetShapeToken(Func<object, string> addQueryParameter)
        {
            return ShapeToken.Wkt(addQueryParameter(_shapeWKT));
        }
    }

    public class CircleCriteria : SpatialCriteria
    {
        private readonly double _radius;
        private readonly double _latitude;
        private readonly double _longitude;
        private readonly SpatialUnits? _radiusUnits;

        public CircleCriteria(double radius, double latitude, double longitude, SpatialUnits? radiusUnits, SpatialRelation relation, double distErrorPercent)
            : base(relation, distErrorPercent)
        {
            _radius = radius;
            _latitude = latitude;
            _longitude = longitude;
            _radiusUnits = radiusUnits;
        }

        protected override ShapeToken GetShapeToken(Func<object, string> addQueryParameter)
        {
            return ShapeToken.Circle(addQueryParameter(_radius), addQueryParameter(_latitude), addQueryParameter(_longitude), _radiusUnits);
        }
    }
}

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

            WhereOperator whereOperator;
            switch (_relation)
            {
                case SpatialRelation.Within:
                    whereOperator = WhereOperator.Spatial_Within;
                    break;
                case SpatialRelation.Contains:
                    whereOperator = WhereOperator.Spatial_Contains;
                    break;
                case SpatialRelation.Disjoint:
                    whereOperator = WhereOperator.Spatial_Disjoint;
                    break;
                case SpatialRelation.Intersects:
                    whereOperator = WhereOperator.Spatial_Intersects;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(_relation), _relation, null);
            }
            
            return WhereToken.Create(whereOperator,fieldName, null, new WhereToken.WhereOptions(shapeToken, _distanceErrorPct));
        }
    }

    public class WktCriteria : SpatialCriteria
    {
        private readonly string _shapeWkt;

        internal WktCriteria(string shapeWkt, SpatialRelation relation, double distanceErrorPct)
            : base(relation, distanceErrorPct)
        {
            _shapeWkt = shapeWkt;
        }

        protected override ShapeToken GetShapeToken(Func<object, string> addQueryParameter)
        {
            return ShapeToken.Wkt(addQueryParameter(_shapeWkt));
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

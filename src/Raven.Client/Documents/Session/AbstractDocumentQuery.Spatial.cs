using System;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Queries.Spatial;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Session
{
    public abstract partial class AbstractDocumentQuery<T, TSelf>
    {
        protected void WithinRadiusOf(string fieldName, double radius, double latitude, double longitude, SpatialUnits? radiusUnits, double distErrorPercent)
        {
            fieldName = EnsureValidFieldName(fieldName, isNestedPath: false);

            AppendOperatorIfNeeded(WhereTokens);
            NegateIfNeeded(fieldName);

            WhereTokens.AddLast(WhereToken.Within(fieldName, ShapeToken.Circle(AddQueryParameter(radius), AddQueryParameter(latitude), AddQueryParameter(longitude), radiusUnits), distErrorPercent));
        }

        protected void Spatial(string fieldName, string shapeWKT, SpatialRelation relation, double distErrorPercent)
        {
            fieldName = EnsureValidFieldName(fieldName, isNestedPath: false);

            AppendOperatorIfNeeded(WhereTokens);
            NegateIfNeeded(fieldName);

            var wktToken = ShapeToken.Wkt(AddQueryParameter(shapeWKT));
            QueryToken relationToken;
            switch (relation)
            {
                case SpatialRelation.Within:
                    relationToken = WhereToken.Within(fieldName, wktToken, distErrorPercent);
                    break;
                case SpatialRelation.Contains:
                    relationToken = WhereToken.Contains(fieldName, wktToken, distErrorPercent);
                    break;
                case SpatialRelation.Disjoint:
                    relationToken = WhereToken.Disjoint(fieldName, wktToken, distErrorPercent);
                    break;
                case SpatialRelation.Intersects:
                    relationToken = WhereToken.Intersects(fieldName, wktToken, distErrorPercent);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(relation), relation, null);
            }

            WhereTokens.AddLast(relationToken);
        }

        public void Spatial(SpatialDynamicField dynamicField, SpatialCriteria criteria)
        {
            AppendOperatorIfNeeded(WhereTokens);
            NegateIfNeeded(null);

            WhereTokens.AddLast(criteria.ToQueryToken(dynamicField.ToField(EnsureValidFieldName), AddQueryParameter));
        }

        /// <inheritdoc />
        public void Spatial(string fieldName, SpatialCriteria criteria)
        {
            fieldName = EnsureValidFieldName(fieldName, isNestedPath: false);

            AppendOperatorIfNeeded(WhereTokens);
            NegateIfNeeded(fieldName);

            WhereTokens.AddLast(criteria.ToQueryToken(fieldName, AddQueryParameter));
        }

        /// <inheritdoc />
        public void OrderByDistance(string fieldName, double latitude, double longitude)
        {
            OrderByTokens.AddLast(OrderByToken.CreateDistanceAscending(fieldName, AddQueryParameter(latitude), AddQueryParameter(longitude)));
        }

        /// <inheritdoc />
        public void OrderByDistance(string fieldName, string shapeWkt)
        {
            OrderByTokens.AddLast(OrderByToken.CreateDistanceAscending(fieldName, AddQueryParameter(shapeWkt)));
        }

        /// <inheritdoc />
        public void OrderByDistanceDescending(string fieldName, double latitude, double longitude)
        {
            OrderByTokens.AddLast(OrderByToken.CreateDistanceDescending(fieldName, AddQueryParameter(latitude), AddQueryParameter(longitude)));
        }

        /// <inheritdoc />
        public void OrderByDistanceDescending(string fieldName, string shapeWkt)
        {
            OrderByTokens.AddLast(OrderByToken.CreateDistanceDescending(fieldName, AddQueryParameter(shapeWkt)));
        }
    }
}

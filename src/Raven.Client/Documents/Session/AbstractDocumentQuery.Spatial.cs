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

            var tokens = GetCurrentWhereTokens();
            AppendOperatorIfNeeded(tokens);
            NegateIfNeeded(tokens, fieldName);

            var whereToken = WhereToken.Create(WhereOperator.Spatial_Within, fieldName, null,
                new WhereToken.WhereOptions(ShapeToken.Circle(AddQueryParameter(radius), AddQueryParameter(latitude), AddQueryParameter(longitude), radiusUnits),
                    distErrorPercent));
            
            tokens.AddLast(whereToken);
        }

        protected void Spatial(string fieldName, string shapeWkt, SpatialRelation relation, SpatialUnits? units, double distErrorPercent)
        {
            fieldName = EnsureValidFieldName(fieldName, isNestedPath: false);

            var tokens = GetCurrentWhereTokens();
            AppendOperatorIfNeeded(tokens);
            NegateIfNeeded(tokens, fieldName);

            var wktToken = ShapeToken.Wkt(AddQueryParameter(shapeWkt), units);
            WhereOperator whereOperator;

            switch (relation)
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
                    throw new ArgumentOutOfRangeException(nameof(relation), relation, null);
            }

            tokens.AddLast(WhereToken.Create(whereOperator,fieldName, null, new WhereToken.WhereOptions(wktToken, distErrorPercent)));
        }

        /// <inheritdoc />
        public void Spatial(DynamicSpatialField dynamicField, SpatialCriteria criteria)
        {
            AssertIsDynamicQuery(dynamicField, nameof(Spatial));

            var tokens = GetCurrentWhereTokens();
            AppendOperatorIfNeeded(tokens);
            NegateIfNeeded(tokens, null);

            tokens.AddLast(criteria.ToQueryToken(dynamicField.ToField(EnsureValidFieldName), AddQueryParameter));
        }

        /// <inheritdoc />
        public void Spatial(string fieldName, SpatialCriteria criteria)
        {
            fieldName = EnsureValidFieldName(fieldName, isNestedPath: false);

            var tokens = GetCurrentWhereTokens();
            AppendOperatorIfNeeded(tokens);
            NegateIfNeeded(tokens, fieldName);

            tokens.AddLast(criteria.ToQueryToken(fieldName, AddQueryParameter));
        }

        /// <inheritdoc />
        public void OrderByDistance(DynamicSpatialField field, double latitude, double longitude)
        {
            if (field == null)
                throw new ArgumentNullException(nameof(field));

            AssertIsDynamicQuery(field, nameof(OrderByDistance));

            OrderByDistance($"'{field.ToField(EnsureValidFieldName)}'", latitude, longitude, field.RoundFactor);
        }

        /// <inheritdoc />
        public void OrderByDistance(string fieldName, double latitude, double longitude)
        {
            OrderByDistance(fieldName, latitude, longitude, 0);
        }

        /// <inheritdoc />
        public void OrderByDistance(string fieldName, double latitude, double longitude, double roundFactor)
        {
            var roundFactorParameterName = roundFactor == 0 ? null : AddQueryParameter(roundFactor);
            OrderByTokens.AddLast(OrderByToken.CreateDistanceAscending(fieldName, AddQueryParameter(latitude), AddQueryParameter(longitude), roundFactorParameterName));
        }

        /// <inheritdoc />
        public void OrderByDistance(DynamicSpatialField field, string shapeWkt)
        {
            if (field == null)
                throw new ArgumentNullException(nameof(field));

            AssertIsDynamicQuery(field, nameof(OrderByDistance));

            OrderByDistance($"'{field.ToField(EnsureValidFieldName)}'", shapeWkt, field.RoundFactor);
        }

        /// <inheritdoc />
        public void OrderByDistance(string fieldName, string shapeWkt)
        {
            OrderByDistance(fieldName, shapeWkt);
        }

        /// <inheritdoc />
        public void OrderByDistance(string fieldName, string shapeWkt, double roundFactor)
        {
            var roundFactorParameterName = roundFactor == 0 ? null : AddQueryParameter(roundFactor);
            OrderByTokens.AddLast(OrderByToken.CreateDistanceAscending(fieldName, AddQueryParameter(shapeWkt), roundFactorParameterName));
        }

        /// <inheritdoc />
        public void OrderByDistanceDescending(DynamicSpatialField field, double latitude, double longitude)
        {
            if (field == null)
                throw new ArgumentNullException(nameof(field));

            AssertIsDynamicQuery(field, nameof(OrderByDistanceDescending));

            OrderByDistanceDescending($"'{field.ToField(EnsureValidFieldName)}'", latitude, longitude, field.RoundFactor);
        }

        public void OrderByDistanceDescending(string fieldName, double latitude, double longitude)
        {
            OrderByDistanceDescending(fieldName, latitude, longitude, 0);
        }
        /// <inheritdoc />
        public void OrderByDistanceDescending(string fieldName, double latitude, double longitude, double roundFactor)
        {
            var roundFactorParameterName = roundFactor == 0 ? null : AddQueryParameter(roundFactor);
            OrderByTokens.AddLast(OrderByToken.CreateDistanceDescending(fieldName, AddQueryParameter(latitude), AddQueryParameter(longitude), roundFactorParameterName));
        }

        /// <inheritdoc />
        public void OrderByDistanceDescending(DynamicSpatialField field, string shapeWkt)
        {
            if (field == null)
                throw new ArgumentNullException(nameof(field));

            AssertIsDynamicQuery(field, nameof(OrderByDistanceDescending));

            OrderByDistanceDescending($"'{field.ToField(EnsureValidFieldName)}'", shapeWkt, field.RoundFactor);
        }

        public void OrderByDistanceDescending(string fieldName, string shapeWkt)
        {
            OrderByDistanceDescending(fieldName, shapeWkt, 0);
        }

        /// <inheritdoc />
        public void OrderByDistanceDescending(string fieldName, string shapeWkt, double roundFactor)
        {
            string factorParamName = roundFactor == 0 ? null : AddQueryParameter(roundFactor);

            OrderByTokens.AddLast(OrderByToken.CreateDistanceDescending(fieldName, AddQueryParameter(shapeWkt), factorParamName));
        }
        
        private void AssertIsDynamicQuery(DynamicSpatialField dynamicField, string methodName)
        {
            if (FromToken.IsDynamic == false)
                throw new InvalidOperationException($"Cannot execute query method '{methodName}'. Field '{dynamicField.ToField(EnsureValidFieldName)}' cannot be used when static index '{FromToken.IndexName}' is queried. Dynamic spatial fields can only be used with dynamic queries, for static index queries please use valid spatial fields defined in index definition.");
        }
    }
}

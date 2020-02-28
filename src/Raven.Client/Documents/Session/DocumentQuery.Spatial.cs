using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Queries.Spatial;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Session
{
    public partial class DocumentQuery<T>
    {
        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.Spatial(Expression<Func<T, object>> path, Func<SpatialCriteriaFactory, SpatialCriteria> clause)
        {
            var criteria = clause(SpatialCriteriaFactory.Instance);
            Spatial(path.ToPropertyPath(), criteria);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.Spatial(string fieldName, Func<SpatialCriteriaFactory, SpatialCriteria> clause)
        {
            var criteria = clause(SpatialCriteriaFactory.Instance);
            Spatial(fieldName, criteria);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.Spatial(DynamicSpatialField field, Func<SpatialCriteriaFactory, SpatialCriteria> clause)
        {
            var criteria = clause(SpatialCriteriaFactory.Instance);
            Spatial(field, criteria);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.Spatial(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, Func<SpatialCriteriaFactory, SpatialCriteria> clause)
        {
            var criteria = clause(SpatialCriteriaFactory.Instance);
            var dynamicField = field(DynamicSpatialFieldFactory<T>.Instance);
            Spatial(dynamicField, criteria);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WithinRadiusOf<TValue>(Expression<Func<T, TValue>> propertySelector, double radius, double latitude, double longitude, SpatialUnits? radiusUnits, double distanceErrorPct)
        {
            WithinRadiusOf(propertySelector.ToPropertyPath(), radius, latitude, longitude, radiusUnits, distanceErrorPct);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WithinRadiusOf(string fieldName, double radius, double latitude, double longitude, SpatialUnits? radiusUnits, double distanceErrorPct)
        {
            WithinRadiusOf(fieldName, radius, latitude, longitude, radiusUnits, distanceErrorPct);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.RelatesToShape<TValue>(Expression<Func<T, TValue>> propertySelector, string shapeWkt, SpatialRelation relation, double distanceErrorPct)
        {
            Spatial(propertySelector.ToPropertyPath(), shapeWkt, relation, null, distanceErrorPct);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.RelatesToShape<TValue>(Expression<Func<T, TValue>> propertySelector, string shapeWkt, SpatialRelation relation, SpatialUnits units, double distanceErrorPct)
        {
            Spatial(propertySelector.ToPropertyPath(), shapeWkt, relation, units, distanceErrorPct);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.RelatesToShape(string fieldName, string shapeWkt, SpatialRelation relation, double distanceErrorPct)
        {
            Spatial(fieldName, shapeWkt, relation, null, distanceErrorPct);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.RelatesToShape(string fieldName, string shapeWkt, SpatialRelation relation, SpatialUnits units, double distanceErrorPct)
        {
            Spatial(fieldName, shapeWkt, relation, units, distanceErrorPct);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(DynamicSpatialField field, double latitude, double longitude)
        {
            OrderByDistance(field, latitude, longitude);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, double latitude, double longitude)
        {
            OrderByDistance(field(DynamicSpatialFieldFactory<T>.Instance), latitude, longitude);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(DynamicSpatialField field, string shapeWkt)
        {
            OrderByDistance(field, shapeWkt);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, string shapeWkt)
        {
            OrderByDistance(field(DynamicSpatialFieldFactory<T>.Instance), shapeWkt);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(Expression<Func<T, object>> propertySelector, double latitude, double longitude)
        {
            OrderByDistance(propertySelector.ToPropertyPath(), latitude, longitude);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(string fieldName, double latitude, double longitude)
        {
            OrderByDistance(fieldName, latitude, longitude);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(Expression<Func<T, object>> propertySelector, string shapeWkt)
        {
            OrderByDistance(propertySelector.ToPropertyPath(), shapeWkt);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(string fieldName, string shapeWkt)
        {
            OrderByDistance(fieldName, shapeWkt);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(DynamicSpatialField field, double latitude, double longitude)
        {
            OrderByDistanceDescending(field, latitude, longitude);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, double latitude, double longitude)
        {
            OrderByDistanceDescending(field(DynamicSpatialFieldFactory<T>.Instance), latitude, longitude);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(DynamicSpatialField field, string shapeWkt)
        {
            OrderByDistanceDescending(field, shapeWkt);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, string shapeWkt)
        {
            OrderByDistanceDescending(field(DynamicSpatialFieldFactory<T>.Instance), shapeWkt);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(Expression<Func<T, object>> propertySelector, double latitude, double longitude)
        {
            OrderByDistanceDescending(propertySelector.ToPropertyPath(), latitude, longitude);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(string fieldName, double latitude, double longitude)
        {
            OrderByDistanceDescending(fieldName, latitude, longitude);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(Expression<Func<T, object>> propertySelector, string shapeWkt)
        {
            OrderByDistanceDescending(propertySelector.ToPropertyPath(), shapeWkt);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(string fieldName, string shapeWkt)
        {
            OrderByDistanceDescending(fieldName, shapeWkt);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(Expression<Func<T, object>> propertySelector, double latitude, double longitude, double roundFactor)
        {
            OrderByDistance(propertySelector.ToPropertyPath(), latitude, longitude, roundFactor);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(string fieldName, double latitude, double longitude, double roundFactor)
        {
            OrderByDistance(fieldName, latitude, longitude, roundFactor);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(Expression<Func<T, object>> propertySelector, string shapeWkt, double roundFactor)
        {
            OrderByDistance(propertySelector.ToPropertyPath(), shapeWkt, roundFactor);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(string fieldName, string shapeWkt, double roundFactor)
        {
            OrderByDistance(fieldName, shapeWkt, roundFactor);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(Expression<Func<T, object>> propertySelector, double latitude, double longitude, double roundFactor)
        {
            OrderByDistanceDescending(propertySelector.ToPropertyPath(), latitude, longitude, roundFactor);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(string fieldName, double latitude, double longitude, double roundFactor)
        {
            OrderByDistanceDescending(fieldName, latitude, longitude, roundFactor);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(Expression<Func<T, object>> propertySelector, string shapeWkt, double roundFactor)
        {
            OrderByDistanceDescending(propertySelector.ToPropertyPath(), shapeWkt, roundFactor);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(string fieldName, string shapeWkt, double roundFactor)
        {
            OrderByDistanceDescending(fieldName, shapeWkt, roundFactor);
            return this;
        }
    }
}

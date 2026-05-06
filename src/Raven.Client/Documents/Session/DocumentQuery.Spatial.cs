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
            Spatial(path.ToPropertyPath(Conventions), criteria);
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
            var dynamicField = field(new DynamicSpatialFieldFactory<T>(Conventions));
            Spatial(dynamicField, criteria);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.WithinRadiusOf<TValue>(Expression<Func<T, TValue>> propertySelector, double radius, double latitude, double longitude, SpatialUnits? radiusUnits, double distanceErrorPct)
        {
            WithinRadiusOf(propertySelector.ToPropertyPath(Conventions), radius, latitude, longitude, radiusUnits, distanceErrorPct);
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
            Spatial(propertySelector.ToPropertyPath(Conventions), shapeWkt, relation, null, distanceErrorPct);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IFilterDocumentQueryBase<T, IDocumentQuery<T>>.RelatesToShape<TValue>(Expression<Func<T, TValue>> propertySelector, string shapeWkt, SpatialRelation relation, SpatialUnits units, double distanceErrorPct)
        {
            Spatial(propertySelector.ToPropertyPath(Conventions), shapeWkt, relation, units, distanceErrorPct);
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
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(DynamicSpatialField field, double latitude, double longitude, NullsOrdering nulls)
        {
            OrderByDistance(field, latitude, longitude, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, double latitude, double longitude, NullsOrdering nulls)
        {
            OrderByDistance(field(new DynamicSpatialFieldFactory<T>(Conventions)), latitude, longitude, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(DynamicSpatialField field, string shapeWkt, NullsOrdering nulls)
        {
            OrderByDistance(field, shapeWkt, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, string shapeWkt, NullsOrdering nulls)
        {
            OrderByDistance(field(new DynamicSpatialFieldFactory<T>(Conventions)), shapeWkt, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(Expression<Func<T, object>> propertySelector, double latitude, double longitude, NullsOrdering nulls)
        {
            OrderByDistance(propertySelector.ToPropertyPath(Conventions), latitude, longitude, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(string fieldName, double latitude, double longitude, NullsOrdering nulls)
        {
            OrderByDistance(fieldName, latitude, longitude, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(Expression<Func<T, object>> propertySelector, string shapeWkt, NullsOrdering nulls)
        {
            OrderByDistance(propertySelector.ToPropertyPath(Conventions), shapeWkt, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(string fieldName, string shapeWkt, NullsOrdering nulls)
        {
            OrderByDistance(fieldName, shapeWkt, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(DynamicSpatialField field, double latitude, double longitude, NullsOrdering nulls)
        {
            OrderByDistanceDescending(field, latitude, longitude, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, double latitude, double longitude, NullsOrdering nulls)
        {
            OrderByDistanceDescending(field(new DynamicSpatialFieldFactory<T>(Conventions)), latitude, longitude, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(DynamicSpatialField field, string shapeWkt, NullsOrdering nulls)
        {
            OrderByDistanceDescending(field, shapeWkt, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(Func<DynamicSpatialFieldFactory<T>, DynamicSpatialField> field, string shapeWkt, NullsOrdering nulls)
        {
            OrderByDistanceDescending(field(new DynamicSpatialFieldFactory<T>(Conventions)), shapeWkt, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(Expression<Func<T, object>> propertySelector, double latitude, double longitude, NullsOrdering nulls)
        {
            OrderByDistanceDescending(propertySelector.ToPropertyPath(Conventions), latitude, longitude, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(string fieldName, double latitude, double longitude, NullsOrdering nulls)
        {
            OrderByDistanceDescending(fieldName, latitude, longitude, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(Expression<Func<T, object>> propertySelector, string shapeWkt, NullsOrdering nulls)
        {
            OrderByDistanceDescending(propertySelector.ToPropertyPath(Conventions), shapeWkt, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(string fieldName, string shapeWkt, NullsOrdering nulls)
        {
            OrderByDistanceDescending(fieldName, shapeWkt, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(Expression<Func<T, object>> propertySelector, double latitude, double longitude, double roundFactor, NullsOrdering nulls)
        {
            OrderByDistance(propertySelector.ToPropertyPath(Conventions), latitude, longitude, roundFactor, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(string fieldName, double latitude, double longitude, double roundFactor, NullsOrdering nulls)
        {
            OrderByDistance(fieldName, latitude, longitude, roundFactor, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(Expression<Func<T, object>> propertySelector, string shapeWkt, double roundFactor, NullsOrdering nulls)
        {
            OrderByDistance(propertySelector.ToPropertyPath(Conventions), shapeWkt, roundFactor, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistance(string fieldName, string shapeWkt, double roundFactor, NullsOrdering nulls)
        {
            OrderByDistance(fieldName, shapeWkt, roundFactor, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(Expression<Func<T, object>> propertySelector, double latitude, double longitude, double roundFactor, NullsOrdering nulls)
        {
            OrderByDistanceDescending(propertySelector.ToPropertyPath(Conventions), latitude, longitude, roundFactor, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(string fieldName, double latitude, double longitude, double roundFactor, NullsOrdering nulls)
        {
            OrderByDistanceDescending(fieldName, latitude, longitude, roundFactor, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(Expression<Func<T, object>> propertySelector, string shapeWkt, double roundFactor, NullsOrdering nulls)
        {
            OrderByDistanceDescending(propertySelector.ToPropertyPath(Conventions), shapeWkt, roundFactor, nulls);
            return this;
        }

        /// <inheritdoc />
        IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderByDistanceDescending(string fieldName, string shapeWkt, double roundFactor, NullsOrdering nulls)
        {
            OrderByDistanceDescending(fieldName, shapeWkt, roundFactor, nulls);
            return this;
        }
    }
}

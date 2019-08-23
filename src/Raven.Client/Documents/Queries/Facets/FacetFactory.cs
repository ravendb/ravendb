using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Queries.Facets
{
    public interface IFacetOperations<T>
    {
        IFacetOperations<T> WithDisplayName(string displayName);

        IFacetOperations<T> WithOptions(FacetOptions options);

        IFacetOperations<T> SumOn(Expression<Func<T, object>> path, string displayName = null);

        IFacetOperations<T> MinOn(Expression<Func<T, object>> path, string displayName = null);

        IFacetOperations<T> MaxOn(Expression<Func<T, object>> path, string displayName = null);

        IFacetOperations<T> AverageOn(Expression<Func<T, object>> path, string displayName = null);
    }

    public interface IFacetBuilder<T>
    {
        IFacetOperations<T> ByRanges(Expression<Func<T, bool>> path, params Expression<Func<T, bool>>[] paths);

        IFacetOperations<T> ByField(Expression<Func<T, object>> path);

        IFacetOperations<T> ByField(string fieldName);

        IFacetOperations<T> AllResults();
    }

    internal class FacetBuilder<T> : IFacetBuilder<T>, IFacetOperations<T>
    {
        private RangeFacet<T> _range;
        private Facet _default;
        private readonly HashSet<string> _rqlKeywords = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "AS",
            "SELECT",
            "WHERE",
            "LOAD",
            "GROUP",
            "ORDER",
            "INCLUDE",
            "UPDATE"
        };

        public IFacetOperations<T> ByRanges(Expression<Func<T, bool>> path, params Expression<Func<T, bool>>[] paths)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            if (_range == null)
                _range = new RangeFacet<T>();

            _range.Ranges.Add(path);

            if (paths != null)
            {
                foreach (var p in paths)
                {
                    _range.Ranges.Add(p);
                }
            }

            return this;
        }

        public IFacetOperations<T> ByField(Expression<Func<T, object>> path)
        {
            return ByField(path.ToPropertyPath('_'));
        }

        public IFacetOperations<T> ByField(string fieldName)
        {
            if (_default == null)
                _default = new Facet();

            if (_rqlKeywords.Contains(fieldName))
            {
                fieldName = "'" + fieldName + "'";
            }

            _default.FieldName = fieldName;

            return this;
        }

        public IFacetOperations<T> AllResults()
        {
            if (_default == null)
                _default = new Facet();

            _default.FieldName = null;

            return this;
        }

        public IFacetOperations<T> WithOptions(FacetOptions options)
        {
            Facet.Options = options;
            return this;
        }

        public IFacetOperations<T> WithDisplayName(string displayName)
        {
            Facet.DisplayFieldName = displayName;
            return this;
        }

        public IFacetOperations<T> SumOn(Expression<Func<T, object>> path, string displayName = null)
        {
            if (Facet.Aggregations.TryGetValue(FacetAggregation.Sum, out var aggregations) == false)
                Facet.Aggregations[FacetAggregation.Sum] = aggregations = new HashSet<FacetAggregationField>();

            aggregations.Add(new FacetAggregationField
            {
                Name = path.ToPropertyPath(), 
                DisplayName = displayName
            });

            return this;
        }

        public IFacetOperations<T> MinOn(Expression<Func<T, object>> path, string displayName = null)
        {
            if (Facet.Aggregations.TryGetValue(FacetAggregation.Min, out var aggregations) == false)
                Facet.Aggregations[FacetAggregation.Min] = aggregations = new HashSet<FacetAggregationField>();

            aggregations.Add(new FacetAggregationField
            {
                Name = path.ToPropertyPath(),
                DisplayName = displayName
            });
            return this;
        }

        public IFacetOperations<T> MaxOn(Expression<Func<T, object>> path, string displayName = null)
        {
            if (Facet.Aggregations.TryGetValue(FacetAggregation.Max, out var aggregations) == false)
                Facet.Aggregations[FacetAggregation.Max] = aggregations = new HashSet<FacetAggregationField>();

            aggregations.Add(new FacetAggregationField
            {
                Name = path.ToPropertyPath(),
                DisplayName = displayName
            });
            return this;
        }

        public IFacetOperations<T> AverageOn(Expression<Func<T, object>> path, string displayName = null)
        {
            if (Facet.Aggregations.TryGetValue(FacetAggregation.Average, out var aggregations) == false)
                Facet.Aggregations[FacetAggregation.Average] = aggregations = new HashSet<FacetAggregationField>();

            aggregations.Add(new FacetAggregationField
            {
                Name = path.ToPropertyPath(),
                DisplayName = displayName
            });
            return this;
        }

        internal FacetBase Facet
        {
            get
            {
                if (_default != null)
                    return _default;

                return _range;
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Queries.Facets
{
    public interface IFacetOperationsBase<T, out TSelf>
        where TSelf : class
    {
        TSelf WithDisplayName(string displayName);

        TSelf SumOn(Expression<Func<T, object>> path, string displayName = null);

        TSelf MinOn(Expression<Func<T, object>> path, string displayName = null);

        TSelf MaxOn(Expression<Func<T, object>> path, string displayName = null);

        TSelf AverageOn(Expression<Func<T, object>> path, string displayName = null);
    }

    public interface IFacetOperations<T> : IFacetOperationsBase<T, IFacetOperations<T>>
    {
        IFacetOperations<T> WithOptions(FacetOptions options);
    }

    public interface IRangeFacetOperations<T> : IFacetOperationsBase<T, IRangeFacetOperations<T>>
    {
    }
    
    public interface IFacetBuilder<T>
    {
        IRangeFacetOperations<T> ByRanges(Expression<Func<T, bool>> path, params Expression<Func<T, bool>>[] paths);

        IFacetOperations<T> ByField(Expression<Func<T, object>> path);

        IFacetOperations<T> ByField(string fieldName);

        IFacetOperations<T> AllResults();
    }

    internal sealed class FacetBuilder<T> : IFacetBuilder<T>, IFacetOperations<T>, IRangeFacetOperations<T>
    {
        private readonly DocumentConventions _conventions;
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

        public FacetBuilder(DocumentConventions conventions)
        {
            _conventions = conventions;
        }

        public IRangeFacetOperations<T> ByRanges(Expression<Func<T, bool>> path, params Expression<Func<T, bool>>[] paths)
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
            return ByField(path.ToPropertyPath(_conventions,'_'));
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
            if (Facet is Facet facet)
                facet.Options = options;
            else if (Facet is Facet<T> facetT)
                facetT.Options = options;
            return this;
        }

        public IFacetOperations<T> WithDisplayName(string displayName)
        {
            Facet.DisplayFieldName = displayName;
            return this;
        }

        IRangeFacetOperations<T> IFacetOperationsBase<T, IRangeFacetOperations<T>>.SumOn(Expression<Func<T, object>> path, string displayName)
        {
            SumOn(path, displayName);
            return this;
        }

        IRangeFacetOperations<T> IFacetOperationsBase<T, IRangeFacetOperations<T>>.MinOn(Expression<Func<T, object>> path, string displayName)
        {
            MinOn(path, displayName);
            return this;
        }

        IRangeFacetOperations<T> IFacetOperationsBase<T, IRangeFacetOperations<T>>.MaxOn(Expression<Func<T, object>> path, string displayName)
        {
            MaxOn(path, displayName);
            return this;
        }

        IRangeFacetOperations<T> IFacetOperationsBase<T, IRangeFacetOperations<T>>.AverageOn(Expression<Func<T, object>> path, string displayName)
        {
            AverageOn(path, displayName);
            return this;
        }

        IRangeFacetOperations<T> IFacetOperationsBase<T, IRangeFacetOperations<T>>.WithDisplayName(string displayName)
        {
            WithDisplayName(displayName);
            return this;
            
        }

        public IFacetOperations<T> SumOn(Expression<Func<T, object>> path, string displayName = null)
        {
            if (Facet.Aggregations.TryGetValue(FacetAggregation.Sum, out var aggregations) == false)
                Facet.Aggregations[FacetAggregation.Sum] = aggregations = new HashSet<FacetAggregationField>();

            aggregations.Add(new FacetAggregationField
            {
                Name = path.ToPropertyPath(_conventions), 
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
                Name = path.ToPropertyPath(_conventions),
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
                Name = path.ToPropertyPath(_conventions),
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
                Name = path.ToPropertyPath(_conventions),
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

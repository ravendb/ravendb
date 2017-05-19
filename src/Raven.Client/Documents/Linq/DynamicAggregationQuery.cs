using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Linq
{

    public class DynamicAggregationQuery<T>
    {
        private readonly IQueryable<T> _queryable;
        private readonly List<AggregationQuery<T>> _facets;
        private readonly Dictionary<string, string> _renames = new Dictionary<string, string>();

        public DynamicAggregationQuery(IQueryable<T> queryable, Expression<Func<T, object>> path, string displayName = null)
        {
            _facets = new List<AggregationQuery<T>>();
            _queryable = queryable;
            AndAggregateOn(path, displayName);
        }


        public DynamicAggregationQuery(IQueryable<T> queryable, string path, string displayName = null)
        {
            _facets = new List<AggregationQuery<T>>();
            _queryable = queryable;
            _facets.Add(new AggregationQuery<T> { Name = path, DisplayName = displayName });

        }

        public DynamicAggregationQuery<T> AndAggregateOn(Expression<Func<T, object>> path, string displayName = null)
        {
            var propertyPath = path.ToPropertyPath('_');

            var rangeType = GetRangeType(path);
            if (rangeType != RangeType.None)
            {
                var tmp = FieldUtil.ApplyRangeSuffixIfNecessary(propertyPath, rangeType);
                _renames[propertyPath] = tmp;
                propertyPath = tmp;
            }

            if (displayName == null)
                displayName = propertyPath;
            if (_facets.Count > 0)
            {
                if (_facets.Any(facet => facet.DisplayName == displayName))
                {
                    throw new InvalidOperationException("Cannot use the more than one aggregation function with the same name/without name");
                }
            }
            _facets.Add(new AggregationQuery<T> { Name = propertyPath, DisplayName = displayName });

            return this;
        }

        public DynamicAggregationQuery<T> AndAggregateOn(string path, string displayName = null)
        {
            _facets.Add(new AggregationQuery<T> { Name = path, DisplayName = displayName });

            return this;
        }

        private static RangeType GetRangeType(Expression<Func<T, object>> path)
        {
            var unaryExpression = path.Body as UnaryExpression;
            if (unaryExpression == null)
                return RangeType.None;
            if (unaryExpression.NodeType != ExpressionType.Convert &&
                unaryExpression.NodeType != ExpressionType.ConvertChecked)
                return RangeType.None;

            return DocumentConventions.GetRangeType(unaryExpression.Operand.Type);
        }

        public DynamicAggregationQuery<T> AddRanges(params Expression<Func<T, bool>>[] paths)
        {
            var last = _facets.Last();

            last.Ranges = last.Ranges ?? new List<Expression<Func<T, bool>>>();

            foreach (var func in paths)
            {
                last.Ranges.Add(func);
            }

            return this;
        }

        private void SetFacet(Expression<Func<T, object>> path, FacetAggregation facetAggregation)
        {
            var last = _facets.Last();
            last.Aggregation |= facetAggregation;
            if (facetAggregation == FacetAggregation.Count &&
                string.IsNullOrEmpty(last.AggregationField) == false)
            {
                return;
            }
            if ((string.IsNullOrEmpty(last.AggregationField) == false) && (!last.AggregationField.Equals(path.ToPropertyPath())))
                throw new InvalidOperationException("Cannot call different aggregation function with different parameters at the same aggregation. Use AndAggregateOn");

            last.AggregationField = path.ToPropertyPath();
            last.AggregationType = path.ExtractTypeFromPath().FullName;
        }

        public DynamicAggregationQuery<T> MaxOn(Expression<Func<T, object>> path)
        {
            SetFacet(path, FacetAggregation.Max);

            return this;
        }

        public DynamicAggregationQuery<T> MinOn(Expression<Func<T, object>> path)
        {
            SetFacet(path, FacetAggregation.Min);

            return this;
        }

        public DynamicAggregationQuery<T> SumOn(Expression<Func<T, object>> path)
        {
            SetFacet(path, FacetAggregation.Sum);

            return this;
        }

        public DynamicAggregationQuery<T> AverageOn(Expression<Func<T, object>> path)
        {
            SetFacet(path, FacetAggregation.Average);

            return this;
        }

        public DynamicAggregationQuery<T> CountOn(Expression<Func<T, object>> path)
        {
            SetFacet(path, FacetAggregation.Count);

            return this;
        }

        public FacetedQueryResult ToList()
        {
            return HandleRenames(_queryable.ToFacets(AggregationQuery<T>.GetFacets(_facets)));
        }

        public Lazy<FacetedQueryResult> ToListLazy()
        {
            var facetsLazy = _queryable.ToFacetsLazy(AggregationQuery<T>.GetFacets(_facets));
            return new Lazy<FacetedQueryResult>(() => HandleRenames(facetsLazy.Value));
        }

        public async Task<FacetedQueryResult> ToListAsync()
        {
            return HandleRenames(await _queryable.ToFacetsAsync(AggregationQuery<T>.GetFacets(_facets)).ConfigureAwait(false));
        }

        private FacetedQueryResult HandleRenames(FacetedQueryResult facetedQueryResult)
        {
            foreach (var rename in _renames)
            {
                FacetResult value;
                if (facetedQueryResult.Results.TryGetValue(rename.Value, out value) &&
                    facetedQueryResult.Results.ContainsKey(rename.Key) == false)
                {
                    facetedQueryResult.Results[rename.Key] = value;
                    facetedQueryResult.Results.Remove(rename.Value);
                }
            }
            return facetedQueryResult;
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Linq
{

	public class DynamicAggregationQuery<T>
	{
		private class AggregationQuery
		{
			public string Name { get; set; }
			public string AggregationField { get; set; }
			public FacetAggregation? Aggregation { get; set; }
			public List<Expression<Func<T, bool>>> Ranges { get; set; }
		}

		private readonly IQueryable<T> queryable;
		private readonly List<AggregationQuery> facets;

		public DynamicAggregationQuery(IQueryable<T> queryable, string name)
		{
			facets = new List<AggregationQuery> {new AggregationQuery {Name = name}};
			this.queryable = queryable;
		}

		public DynamicAggregationQuery<T> AndAggregateOn(Expression<Func<T, object>> path)
		{
			facets.Add(new AggregationQuery {Name = path.ToPropertyPath()});

			return this;
		}

		public DynamicAggregationQuery<T> AddRanges(params Expression<Func<T, bool>>[] paths)
		{
			var last = GetLast();
			last.Ranges = new List<Expression<Func<T, bool>>>();

			foreach (var func in paths)
			{
				last.Ranges.Add(func);
			}

			return this;
		}

		private AggregationQuery GetLast()
		{
			var last = facets.Last();

			if (last.AggregationField != null)
				throw new InvalidOperationException("Can not set multipule Facet aggregation on a single facet");

			return last;
		}

		private void SetFacet(Expression<Func<T, object>> path, FacetAggregation facetAggregation)
		{
			var last = GetLast();
			last.AggregationField = path.ToPropertyPath();
			last.Aggregation = facetAggregation;
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

#if !SILVERLIGHT
		public FacetResults ToList()
		{
			return queryable.ToFacets(GetFacets());
		}

		public Lazy<FacetResults> ToListLazy()
		{
			return queryable.ToFacetsLazy(GetFacets());
		}
#endif

		public Task<FacetResults> ToListAsync()
		{
			return queryable.ToFacetsAsync(GetFacets());
		}

		private IEnumerable<Facet> GetFacets()
		{
			var facetsList = new List<Facet>();

			foreach (var aggregationQuery in facets)
			{
				if (aggregationQuery.Aggregation == null)
					throw new InvalidOperationException("All aggregations must have a type");

				var shouldUseRanges = aggregationQuery.Ranges != null && aggregationQuery.Ranges.Count > 0;

				List<string> ranges = null;
				if (shouldUseRanges)
					ranges = aggregationQuery.Ranges.Select(Facet<T>.Parse).ToList();

				var mode = shouldUseRanges ? FacetMode.Ranges : FacetMode.Default;
				facetsList.Add(new Facet
				{
					Name = aggregationQuery.Name,
					Aggregation = (FacetAggregation) aggregationQuery.Aggregation,
					AggregationField = aggregationQuery.AggregationField,
					Ranges = ranges,
					Mode = mode
				});
			}
			return facetsList;
		}
	}
}

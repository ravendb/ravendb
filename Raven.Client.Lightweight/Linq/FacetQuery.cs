using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Client;

namespace Raven.Abstractions.Data
{
	public class FacetQuery<T>
	{
		private readonly IQueryable<T> queryable;
		private readonly string name;
		private string aggregationField;
		private FacetAggregation aggregation;


		public FacetQuery(IQueryable<T> queryable, string name)
		{
			this.queryable = queryable;
			this.name = name;
		}

		public FacetQuery<T> MaxOn(Expression<Func<T, object>> path)
		{
			aggregationField = path.ToPropertyPath();
			aggregation = FacetAggregation.Max;

			return this;
		}

		public FacetQuery<T> MinOn(Expression<Func<T, object>> path)
		{
			aggregationField = path.ToPropertyPath();
			aggregation = FacetAggregation.Min;

			return this;
		}

		public FacetQuery<T> SumOn(Expression<Func<T, object>> path)
		{
			aggregationField = path.ToPropertyPath();
			aggregation = FacetAggregation.Sum;

			return this;
		}

		public FacetQuery<T> AverageOn(Expression<Func<T, object>> path)
		{
			aggregationField = path.ToPropertyPath();
			aggregation = FacetAggregation.Average;

			return this;
		}
		public FacetQuery<T> CountOn(Expression<Func<T, object>> path)
		{
			aggregationField = path.ToPropertyPath();
			aggregation = FacetAggregation.Count;

			return this;
		}

#if !SILVERLIGHT
		public FacetResults ToList()
		{
			return queryable.ToFacets(new[]
			{
				new Facet
				{
					Aggregation = aggregation,
					AggregationField = aggregationField,
					Name = name
				}
			});
		}
#endif
		public Task<FacetResults> ToListAsync()
		{
			return queryable.ToFacetsAsync(new[]
			{
				new Facet
				{
					Aggregation = aggregation,
					AggregationField = aggregationField,
					Name = name
				}
			});
		}
	}
}

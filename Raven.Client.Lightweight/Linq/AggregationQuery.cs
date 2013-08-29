using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Abstractions.Data;

namespace Raven.Client.Linq
{
	public class AggregationQuery<T> : AggregationQuery
	{
		public List<Expression<Func<T, bool>>> Ranges { get; set; }

		public static IEnumerable<Facet> GetFacets(List<AggregationQuery<T>> aggregationQueries)
		{
			var facetsList = new List<Facet>();

			foreach (var aggregationQuery in aggregationQueries)
			{
				if (aggregationQuery.Aggregation == FacetAggregation.None)
					throw new InvalidOperationException("All aggregations must have a type");

				var shouldUseRanges = aggregationQuery.Ranges != null && aggregationQuery.Ranges.Count > 0;

				List<string> ranges = null;
				if (shouldUseRanges)
					ranges = aggregationQuery.Ranges.Select(Facet<T>.Parse).ToList();

				var mode = shouldUseRanges ? FacetMode.Ranges : FacetMode.Default;
				facetsList.Add(new Facet
				{
					Name = aggregationQuery.Name,
					DisplayName = aggregationQuery.DisplayName,
					Aggregation = aggregationQuery.Aggregation,
					AggregationType = aggregationQuery.AggregationType,
					AggregationField = aggregationQuery.AggregationField,
					Ranges = ranges,
					Mode = mode
				});
			}
			return facetsList;
		}
	}

	public class AggregationQuery
	{
		public string Name { get; set; }
		public string DisplayName { get; set; }
		public string AggregationField { get; set; }
		public string AggregationType { get; set; }
		public FacetAggregation Aggregation { get; set; }

		public static List<Facet> GetFacets(List<AggregationQuery> aggregationQueries)
		{
			var facetsList = new List<Facet>();

			foreach (var aggregationQuery in aggregationQueries)
			{
				if (aggregationQuery.Aggregation == FacetAggregation.None)
					throw new InvalidOperationException("All aggregations must have a type");

				facetsList.Add(new Facet
				{
					Name = aggregationQuery.Name,
					DisplayName = aggregationQuery.DisplayName,
					Aggregation = aggregationQuery.Aggregation,
					AggregationType = aggregationQuery.AggregationType,
					AggregationField = aggregationQuery.AggregationField,
					Mode = FacetMode.Default
				});
			}
			return facetsList;
		}
	}
}

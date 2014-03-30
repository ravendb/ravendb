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
		private readonly IQueryable<T> queryable;
		private readonly List<AggregationQuery<T>> facets;
        private readonly Dictionary<string,string> renames = new Dictionary<string, string>();

		public DynamicAggregationQuery(IQueryable<T> queryable, Expression<Func<T, object>> path, string displayName = null)
		{
			facets = new List<AggregationQuery<T>>();
			this.queryable = queryable;
			AndAggregateOn(path, displayName);
		}


		public DynamicAggregationQuery(IQueryable<T> queryable, string path, string displayName = null)
		{
			facets = new List<AggregationQuery<T>>();
			this.queryable = queryable;
			facets.Add(new AggregationQuery<T> { Name = path, DisplayName = displayName });

		}

		public DynamicAggregationQuery<T> AndAggregateOn(Expression<Func<T, object>> path, string displayName = null)
		{
			var propertyPath = path.ToPropertyPath('_');
			if (IsNumeric(path))
			{
				var tmp = propertyPath + "_Range";
				renames[propertyPath] = tmp;
				propertyPath = tmp;
			}

			facets.Add(new AggregationQuery<T> { Name = propertyPath, DisplayName = displayName});

			return this;
		}

		public DynamicAggregationQuery<T> AndAggregateOn(string path, string displayName = null)
		{
			facets.Add(new AggregationQuery<T> { Name = path, DisplayName = displayName });

			return this;
		}

	    private bool IsNumeric(Expression<Func<T, object>> path)
	    {
	        var unaryExpression = path.Body as UnaryExpression;
	        if (unaryExpression == null)
	            return false;
	        if (unaryExpression.NodeType != ExpressionType.Convert &&
	            unaryExpression.NodeType != ExpressionType.ConvertChecked)
	            return false;
	        var type = unaryExpression.Operand.Type;
	        return type == typeof (int) ||
	               type == typeof (long) ||
	               type == typeof (short) ||
	               type == typeof (decimal) ||
	               type == typeof (double) ||
	               type == typeof (float);
	    }

	    public DynamicAggregationQuery<T> AddRanges(params Expression<Func<T, bool>>[] paths)
		{
			var last = facets.Last();
			
			last.Ranges = last.Ranges ?? new List<Expression<Func<T, bool>>>();

			foreach (var func in paths)
			{
				last.Ranges.Add(func);
			}

			return this;
		}

	    private void SetFacet(Expression<Func<T, object>> path, FacetAggregation facetAggregation)
		{
			var last = facets.Last();
			last.AggregationField = path.ToPropertyPath();
		    last.AggregationType = path.ExtractTypeFromPath().FullName;
			last.Aggregation |= facetAggregation;
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

#if !NETFX_CORE
		public FacetResults ToList()
		{
			return HandlRenames(queryable.ToFacets(AggregationQuery<T>.GetFacets(facets)));
		}

		public Lazy<FacetResults> ToListLazy()
		{
			var facetsLazy = queryable.ToFacetsLazy(AggregationQuery<T>.GetFacets(facets));
			return new Lazy<FacetResults>(() => HandlRenames(facetsLazy.Value));
		}
#endif

		public async Task<FacetResults> ToListAsync()
		{
			return HandlRenames(await queryable.ToFacetsAsync(AggregationQuery<T>.GetFacets(facets)));
		}

	    private FacetResults HandlRenames(FacetResults facetResults)
	    {
	        foreach (var rename in renames)
	        {
	            FacetResult value;
	            if (facetResults.Results.TryGetValue(rename.Value, out value) &&
	                facetResults.Results.ContainsKey(rename.Key) == false)
	                facetResults.Results[rename.Key] = value;
	        }
	        return facetResults;
	    }
	}
}

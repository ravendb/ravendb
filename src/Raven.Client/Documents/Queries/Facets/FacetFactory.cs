using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Queries.Facets
{
    public class FacetFactory<T>
    {
        internal readonly Facet Facet = new Facet();

        public FacetFactory<T> WithOptions(FacetOptions options)
        {
            Facet.Options = options;
            return this;
        }

        public FacetFactory<T> WithRanges(Expression<Func<T, bool>> path, params Expression<Func<T, bool>>[] paths)
        {
            throw new NotImplementedException();
        }

        public FacetFactory<T> WithDisplayName(string displayName)
        {
            Facet.DisplayName = displayName;
            return this;
        }

        public FacetFactory<T> SumOn(Expression<Func<T, object>> path)
        {
            Facet.Aggregations[FacetAggregation.Sum] = path.ToPropertyPath();
            return this;
        }

        public FacetFactory<T> MinOn(Expression<Func<T, object>> path)
        {
            Facet.Aggregations[FacetAggregation.Min] = path.ToPropertyPath();
            return this;
        }

        public FacetFactory<T> MaxOn(Expression<Func<T, object>> path)
        {
            Facet.Aggregations[FacetAggregation.Max] = path.ToPropertyPath();
            return this;
        }

        public FacetFactory<T> AverageOn(Expression<Func<T, object>> path)
        {
            Facet.Aggregations[FacetAggregation.Average] = path.ToPropertyPath();
            return this;
        }

        public FacetFactory<T> Count()
        {
            Facet.Aggregations[FacetAggregation.Count] = null;
            return this;
        }
    }
}

using System;
using System.Linq.Expressions;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Queries.Facets
{
    public class FacetFactory<T>
    {
        internal Facet Facet = new Facet();

        public FacetFactory(string name)
        {
            Facet.Name = name;
        }

        public FacetFactory<T> WithOptions(FacetOptions options)
        {
            Facet.Options = options;
            return this;
        }

        public FacetFactory<T> WithRanges(Expression<Func<T, bool>> path, params Expression<Func<T, bool>>[] paths)
        {
            if (path == null) 
                throw new ArgumentNullException(nameof(path));

            Facet.Ranges.Add(Facet<T>.Parse(path));

            if (paths != null)
            {
                foreach (var p in paths)
                    Facet.Ranges.Add(Facet<T>.Parse(p));
            }

            return this;
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
    }
}

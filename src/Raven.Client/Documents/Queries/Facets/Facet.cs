using System;
using System.Linq.Expressions;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Queries.Facets
{
    public class Facet : FacetBase
    {
        /// <summary>
        /// Name of field the facet aggregate on
        /// </summary>
        public string FieldName { get; set; }

        internal override Facet AsFacet()
        {
            return this;
        }

        internal override RangeFacet AsRangeFacet()
        {
            return null;
        }
    }

    public class Facet<T> : FacetBase
    {
        /// <summary>
        /// Name of field the facet aggregate on
        /// </summary>
        public Expression<Func<T, object>> FieldName { get; set; }

        public static implicit operator Facet(Facet<T> other)
        {
            return new Facet
            {
                FieldName = other.FieldName.ToPropertyPath('_'),
                Options = other.Options,
                Aggregations = other.Aggregations,
                DisplayFieldName = other.DisplayFieldName
            };
        }

        internal override Facet AsFacet()
        {
            return this;
        }

        internal override RangeFacet AsRangeFacet()
        {
            return null;
        }
    }
}

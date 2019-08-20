using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Session.Tokens;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Queries.Facets
{
    public class Facet : FacetBase
    {
        /// <summary>
        /// Name of field the facet aggregate on
        /// </summary>
        public string FieldName { get; set; }

        internal override FacetToken ToFacetToken(Func<object, string> addQueryParameter)
        {
            return FacetToken.Create(this, addQueryParameter);
        }

        internal static Facet Create(BlittableJsonReaderObject json)
        {
            if (json == null) 
                throw new ArgumentNullException(nameof(json));

            var facet = new Facet();

            if (json.TryGet(nameof(facet.FieldName), out string fieldName))
                facet.FieldName = fieldName;

            Fill(facet, json);

            return facet;
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

        internal override FacetToken ToFacetToken(Func<object, string> addQueryParameter)
        {
            var facet = (Facet)this;
            return facet.ToFacetToken(addQueryParameter);
        }
    }
}

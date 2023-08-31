using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session.Tokens;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Queries.Facets
{
    public sealed class Facet : FacetBase
    {
        internal Func<DocumentConventions, string> _originalFieldNamePathEvaluator;
        /// Name of field the facet aggregate on
        /// </summary>
        public string FieldName { get; set; }
        
        public FacetOptions Options { get; set; }

        internal override FacetToken ToFacetToken(DocumentConventions conventions, Func<object, string> addQueryParameter)
        {
            if (conventions != DocumentConventions.Default && _originalFieldNamePathEvaluator != null)
                FieldName = _originalFieldNamePathEvaluator(conventions);
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

    public sealed class Facet<T> : FacetBase
    {
        /// <summary>
        /// Name of field the facet aggregate on
        /// </summary>
        public Expression<Func<T, object>> FieldName { get; set; }

        public FacetOptions Options { get; set; }

        public static implicit operator Facet(Facet<T> other)
        {
            return new Facet
            {
                FieldName = other.FieldName.ToPropertyPath(DocumentConventions.Default, '_'),
                Options = other.Options,
                Aggregations = other.Aggregations,
                DisplayFieldName = other.DisplayFieldName,
                _originalFieldNamePathEvaluator = customConvention => other.FieldName.ToPropertyPath(customConvention, '_')
            };
        }

        internal override FacetToken ToFacetToken(DocumentConventions conventions,Func<object, string> addQueryParameter)
        {
            var facet = (Facet)this;
            return facet.ToFacetToken(conventions, addQueryParameter);
        }
    }
}

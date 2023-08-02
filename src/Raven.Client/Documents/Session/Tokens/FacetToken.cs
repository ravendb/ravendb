using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;

namespace Raven.Client.Documents.Session.Tokens
{
    internal sealed class FacetToken : QueryToken
    {
        private readonly string _facetSetupDocumentId;
        private readonly string _aggregateByFieldName;
        private readonly string _alias;
        private readonly List<string> _ranges;
        private readonly string _optionsParameterName;

        private readonly List<FacetAggregationToken> _aggregations;

        public string Name => _alias ?? _aggregateByFieldName;

        private FacetToken(string facetSetupDocumentId)
        {
            _facetSetupDocumentId = facetSetupDocumentId;
        }

        private FacetToken(string aggregateByFieldName, string alias, List<string> ranges, string optionsParameterName)
        {
            _aggregateByFieldName = aggregateByFieldName;
            _alias = alias;
            _ranges = ranges;
            _optionsParameterName = optionsParameterName;
            _aggregations = new List<FacetAggregationToken>();
        }

        public static FacetToken Create(string facetSetupDocumentId)
        {
            if (string.IsNullOrWhiteSpace(facetSetupDocumentId))
                throw new ArgumentNullException(nameof(facetSetupDocumentId));

            return new FacetToken(facetSetupDocumentId);
        }

        public static FacetToken Create(Facet facet, Func<object, string> addQueryParameter)
        {
            var optionsParameterName = GetOptionsParameterName(facet, addQueryParameter);
            var token = new FacetToken(QueryFieldUtil.EscapeIfNecessary(facet.FieldName), QueryFieldUtil.EscapeIfNecessary(facet.DisplayFieldName), null, optionsParameterName);

            ApplyAggregations(facet, token);

            return token;
        }

        public static FacetToken Create(RangeFacet facet, Func<object, string> addQueryParameter)
        {
            var optionsParameterName = GetOptionsParameterName(facet, addQueryParameter);
            var token = new FacetToken(null, QueryFieldUtil.EscapeIfNecessary(facet.DisplayFieldName), facet.Ranges, optionsParameterName);

            ApplyAggregations(facet, token);

            return token;
        }

        public static FacetToken Create<T>(RangeFacet<T> facet, Func<object, string> addQueryParameter, DocumentConventions conventions)
        {
            var optionsParameterName = GetOptionsParameterName(facet, addQueryParameter);

            var ranges = new List<string>();
            foreach (var expression in facet.Ranges)
                ranges.Add(RangeFacet<T>.Parse(null, expression, addQueryParameter, conventions));

            var token = new FacetToken(null, QueryFieldUtil.EscapeIfNecessary(facet.DisplayFieldName), ranges, optionsParameterName);

            ApplyAggregations(facet, token);

            return token;
        }

        public static FacetToken Create(FacetBase facet, Func<object, string> addQueryParameter, DocumentConventions conventions)
        {
            // this is just a dispatcher
            return facet.ToFacetToken(conventions, addQueryParameter);
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append("facet(");

            if (_facetSetupDocumentId != null)
            {
                writer
                    .Append("id('")
                    .Append(_facetSetupDocumentId)
                    .Append("'))");

                return;
            }

            var firstArgument = false;

            if (_aggregateByFieldName != null)
                writer.Append(_aggregateByFieldName);
            else if (_ranges != null)
            {
                var firstInRange = true;

                foreach (var range in _ranges)
                {
                    if (firstInRange == false)
                        writer.Append(", ");

                    firstInRange = false;

                    writer.Append(range);
                }
            }
            else
                firstArgument = true;

            foreach (var aggregation in _aggregations)
            {
                if (firstArgument == false)
                    writer.Append(", ");

                firstArgument = false;
                aggregation.WriteTo(writer);
            }

            if (string.IsNullOrWhiteSpace(_optionsParameterName) == false)
            {
                writer
                    .Append(", $")
                    .Append(_optionsParameterName);
            }

            writer.Append(")");

            if (string.IsNullOrWhiteSpace(_alias) || string.Equals(_aggregateByFieldName, _alias))
                return;

            writer
                .Append(" as ")
                .Append(_alias);
        }


        private static void ApplyAggregations(FacetBase facet, FacetToken token)
        {
            foreach (var aggregation in facet.Aggregations)
            {
                foreach (var value in aggregation.Value)
                {
                    FacetAggregationToken aggregationToken;
                    switch (aggregation.Key)
                    {
                        case FacetAggregation.Max:
                            aggregationToken = FacetAggregationToken.Max(value.Name, value.DisplayName);
                            break;
                        case FacetAggregation.Min:
                            aggregationToken = FacetAggregationToken.Min(value.Name, value.DisplayName);
                            break;
                        case FacetAggregation.Average:
                            aggregationToken = FacetAggregationToken.Average(value.Name, value.DisplayName);
                            break;
                        case FacetAggregation.Sum:
                            aggregationToken = FacetAggregationToken.Sum(value.Name, value.DisplayName);
                            break;
                        default:
                            throw new NotSupportedException($"Unsupported aggregation method: {aggregation.Key}");
                    }

                    token._aggregations.Add(aggregationToken);
                }
            }
        }

        private static string GetOptionsParameterName(FacetBase facet, Func<object, string> addQueryParameter)
        {
            return facet switch
            {
                Facet facetWithOptions => facetWithOptions.Options != null && facetWithOptions.Options != FacetOptions.Default ? addQueryParameter(facetWithOptions.Options) : null,
                _ => null
            };
        }

        private sealed class FacetAggregationToken : QueryToken
        {
            private readonly string _fieldName;
            private readonly string _fieldDisplayName;
            private readonly FacetAggregation _aggregation;

            private FacetAggregationToken(string fieldName, string fieldDisplayName, FacetAggregation aggregation)
            {
                _fieldName = fieldName;
                _fieldDisplayName = fieldDisplayName;
                _aggregation = aggregation;
            }

            public override void WriteTo(StringBuilder writer)
            {
                switch (_aggregation)
                {
                    case FacetAggregation.Max:
                        writer
                            .Append("max(")
                            .Append(_fieldName)
                            .Append(")");
                        break;
                    case FacetAggregation.Min:
                        writer
                            .Append("min(")
                            .Append(_fieldName)
                            .Append(")");
                        break;
                    case FacetAggregation.Average:
                        writer
                            .Append("avg(")
                            .Append(_fieldName)
                            .Append(")");
                        break;
                    case FacetAggregation.Sum:
                        writer
                            .Append("sum(")
                            .Append(_fieldName)
                            .Append(")");
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                if (string.IsNullOrWhiteSpace(_fieldDisplayName))
                    return;

                writer.Append(" as ");
                WriteField(writer, _fieldDisplayName);
            }

            public static FacetAggregationToken Max(string fieldName, string fieldDisplayName = null)
            {
                if (string.IsNullOrWhiteSpace(fieldName))
                    throw new ArgumentNullException(nameof(fieldName));

                return new FacetAggregationToken(fieldName, fieldDisplayName, FacetAggregation.Max);
            }

            public static FacetAggregationToken Min(string fieldName, string fieldDisplayName = null)
            {
                if (string.IsNullOrWhiteSpace(fieldName))
                    throw new ArgumentNullException(nameof(fieldName));

                return new FacetAggregationToken(fieldName, fieldDisplayName, FacetAggregation.Min);
            }

            public static FacetAggregationToken Average(string fieldName, string fieldDisplayName = null)
            {
                if (string.IsNullOrWhiteSpace(fieldName))
                    throw new ArgumentNullException(nameof(fieldName));

                return new FacetAggregationToken(fieldName, fieldDisplayName, FacetAggregation.Average);
            }

            public static FacetAggregationToken Sum(string fieldName, string fieldDisplayName = null)
            {
                if (string.IsNullOrWhiteSpace(fieldName))
                    throw new ArgumentNullException(nameof(fieldName));

                return new FacetAggregationToken(fieldName, fieldDisplayName, FacetAggregation.Sum);
            }
        }
    }
}

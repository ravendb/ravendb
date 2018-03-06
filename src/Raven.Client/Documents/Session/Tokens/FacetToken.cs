using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents.Queries.Facets;

namespace Raven.Client.Documents.Session.Tokens
{
    public class FacetToken : QueryToken
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
            var token = new FacetToken(facet.FieldName, facet.DisplayFieldName, null, optionsParameterName);

            ApplyAggregations(facet, token);

            return token;
        }

        public static FacetToken Create(RangeFacet facet, Func<object, string> addQueryParameter)
        {
            var optionsParameterName = GetOptionsParameterName(facet, addQueryParameter);
            var token = new FacetToken(null, facet.DisplayFieldName, facet.Ranges, optionsParameterName);

            ApplyAggregations(facet, token);

            return token;
        }

        public static FacetToken Create<T>(RangeFacet<T> facet, Func<object, string> addQueryParameter)
        {
            var optionsParameterName = GetOptionsParameterName(facet, addQueryParameter);

            var ranges = new List<string>();
            foreach (var expression in facet.Ranges)
                ranges.Add(RangeFacet<T>.Parse(null, expression, addQueryParameter));

            var token = new FacetToken(null, facet.DisplayFieldName, ranges, optionsParameterName);

            ApplyAggregations(facet, token);

            return token;
        }

        public static FacetToken Create(FacetBase facet, Func<object, string> addQueryParameter)
        {
            // this is just a dispatcher
            return facet.ToFacetToken(addQueryParameter);
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
                FacetAggregationToken aggregationToken;
                switch (aggregation.Key)
                {
                    case FacetAggregation.Max:
                        aggregationToken = FacetAggregationToken.Max(aggregation.Value);
                        break;
                    case FacetAggregation.Min:
                        aggregationToken = FacetAggregationToken.Min(aggregation.Value);
                        break;
                    case FacetAggregation.Average:
                        aggregationToken = FacetAggregationToken.Average(aggregation.Value);
                        break;
                    case FacetAggregation.Sum:
                        aggregationToken = FacetAggregationToken.Sum(aggregation.Value);
                        break;
                    default:
                        throw new NotSupportedException($"Unsupported aggregation method: {aggregation.Key}");
                }

                token._aggregations.Add(aggregationToken);
            }
        }

        private static string GetOptionsParameterName(FacetBase facet, Func<object, string> addQueryParameter)
        {
            return facet.Options != null && facet.Options != FacetOptions.Default ? addQueryParameter(facet.Options) : null;
        }

        private class FacetAggregationToken : QueryToken
        {
            private readonly string _fieldName;
            private readonly FacetAggregation _aggregation;

            private FacetAggregationToken(string fieldName, FacetAggregation aggregation)
            {
                _fieldName = fieldName;
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
            }

            public static FacetAggregationToken Max(string fieldName)
            {
                if (string.IsNullOrWhiteSpace(fieldName))
                    throw new ArgumentNullException(nameof(fieldName));

                return new FacetAggregationToken(fieldName, FacetAggregation.Max);
            }

            public static FacetAggregationToken Min(string fieldName)
            {
                if (string.IsNullOrWhiteSpace(fieldName))
                    throw new ArgumentNullException(nameof(fieldName));

                return new FacetAggregationToken(fieldName, FacetAggregation.Min);
            }

            public static FacetAggregationToken Average(string fieldName)
            {
                if (string.IsNullOrWhiteSpace(fieldName))
                    throw new ArgumentNullException(nameof(fieldName));

                return new FacetAggregationToken(fieldName, FacetAggregation.Average);
            }

            public static FacetAggregationToken Sum(string fieldName)
            {
                if (string.IsNullOrWhiteSpace(fieldName))
                    throw new ArgumentNullException(nameof(fieldName));

                return new FacetAggregationToken(fieldName, FacetAggregation.Sum);
            }
        }
    }
}

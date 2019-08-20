using System;
using System.Collections.Generic;
using Raven.Client.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Facets
{
    public class FacetField : SelectField
    {
        private string _optionsAsStringOrParameterName;

        private ValueTokenType _optionsType;

        public Dictionary<FacetAggregation, HashSet<string>> Aggregations;

        public List<QueryExpression> Ranges;

        public string FacetSetupDocumentId;

        private FacetOptions _options;

        public FacetField()
        {
            IsFacet = true;
            Aggregations = new Dictionary<FacetAggregation, HashSet<string>>();
            Ranges = new List<QueryExpression>();
        }

        public FacetOptions GetOptions(JsonOperationContext context, BlittableJsonReaderObject parameters)
        {
            if (_options != null)
                return _options;

            if (_optionsAsStringOrParameterName == null)
                return null;

            var options = FieldOptionsHelper.GetOptions<FacetOptions>(_optionsAsStringOrParameterName, _optionsType, parameters, context);

            if (_optionsType == AST.ValueTokenType.String)
                _options = options;

            return options;
        }

        public void AddAggregation(FacetAggregation aggregation, QueryFieldName name)
        {
            if (Aggregations.TryGetValue(aggregation, out var values) == false)
                Aggregations[aggregation] = values = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            values.Add(name);
        }

        public void AddOptions(string optionsAsStringOrParameterName, ValueTokenType type)
        {
            FieldOptionsHelper.ValidateOptions<FacetOptions>(optionsAsStringOrParameterName, type);

            _optionsAsStringOrParameterName = optionsAsStringOrParameterName;
            _optionsType = type;
        }
    }
}

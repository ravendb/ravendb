using System.Collections.Generic;
using Raven.Client.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.AST;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Facets
{
    public class FacetField : SelectField
    {
        private string _optionsAsStringOrParameterName;

        private ValueTokenType _optionsType;

        public Dictionary<FacetAggregation, HashSet<FacetAggregationField>> Aggregations;

        public List<QueryExpression> Ranges;

        public string FacetSetupDocumentId;

        private FacetOptions _options;

        public FacetField()
        {
            IsFacet = true;
            Aggregations = new Dictionary<FacetAggregation, HashSet<FacetAggregationField>>();
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

        public void AddAggregation(FacetAggregation aggregation, QueryFieldName name, StringSegment displayName)
        {
            if (Aggregations.TryGetValue(aggregation, out var values) == false)
                Aggregations[aggregation] = values = new HashSet<FacetAggregationField>();

            values.Add(new FacetAggregationField
            {
                Name = name,
                DisplayName = displayName.ToString()
            });
        }

        public void AddOptions(string optionsAsStringOrParameterName, ValueTokenType type)
        {
            FieldOptionsHelper.ValidateOptions<FacetOptions>(optionsAsStringOrParameterName, type);

            _optionsAsStringOrParameterName = optionsAsStringOrParameterName;
            _optionsType = type;
        }
    }
}

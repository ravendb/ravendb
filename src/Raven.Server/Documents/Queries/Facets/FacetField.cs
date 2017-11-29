using System;
using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Facets
{
    public class FacetField : SelectField
    {
        private string _optionsAsStringOrParameterName;

        private ValueTokenType _optionsType;

        public Dictionary<FacetAggregation, string> Aggregations;

        public List<QueryExpression> Ranges;

        public string FacetSetupDocumentId;

        private FacetOptions _options;

        public FacetField()
        {
            IsFacet = true;
            Aggregations = new Dictionary<FacetAggregation, string>();
            Ranges = new List<QueryExpression>();
        }

        public FacetOptions GetOptions(JsonOperationContext context, BlittableJsonReaderObject parameters)
        {
            if (_options != null)
                return _options;

            if (_optionsAsStringOrParameterName == null)
                return null;

            BlittableJsonReaderObject optionsJson;

            if (_optionsType == AST.ValueTokenType.Parameter)
            {
                if (parameters == null)
                    throw new ArgumentNullException(nameof(parameters));

                if (parameters.TryGetMember(_optionsAsStringOrParameterName, out var optionsObject) == false)
                    throw new InvalidOperationException($"Parameter '{_optionsAsStringOrParameterName}' containing '{nameof(FacetOptions)}' was not present in the list of parameters.");

                optionsJson = optionsObject as BlittableJsonReaderObject;

                if (optionsJson == null)
                    throw new InvalidOperationException($"Parameter '{_optionsAsStringOrParameterName}' should contain JSON object.");
            }
            else if (_optionsType == AST.ValueTokenType.String)
            {
                optionsJson = IndexReadOperation.ParseJsonStringIntoBlittable(_optionsAsStringOrParameterName, context);
            }
            else
                throw new InvalidOperationException($"Unknown options type '{_optionsType}'.");

            var options = (FacetOptions)EntityToBlittable.ConvertToEntity(typeof(FacetOptions), "facet/options", optionsJson, DocumentConventions.Default);
            if (_optionsType == AST.ValueTokenType.String)
                _options = options;

            return options;
        }

        public void AddAggregation(FacetAggregation aggregation, QueryFieldName name)
        {
            if (Aggregations.ContainsKey(aggregation))
                throw new InvalidOperationException($"Detected duplicate facet aggregation operation '{aggregation}'. Each facet can only contain one of each available operations.");

            Aggregations.Add(aggregation, name);
        }

        public void AddOptions(string optionsAsStringOrParameterName, ValueTokenType type)
        {
            if (type != AST.ValueTokenType.String && type != AST.ValueTokenType.Parameter)
                throw new InvalidOperationException($"{nameof(FacetOptions)} can only be passed as JSON string or as a parameter pointing to JSON object, but was '{type}'.");

            _optionsAsStringOrParameterName = optionsAsStringOrParameterName;
            _optionsType = type;
        }
    }
}

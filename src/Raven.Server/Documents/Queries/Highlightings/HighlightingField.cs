using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.Highlighting;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Highlightings
{
    public class HighlightingField
    {
        private string _optionsAsStringOrParameterName;

        private ValueTokenType _optionsType;

        private HighlightingOptions _options;
        private int _fragmentCount;
        private int _fragmentLength;

        public HighlightingField(QueryFieldName field)
        {
            Field = field;
        }

        public readonly QueryFieldName Field;

        public int FragmentCount => _fragmentCount;

        public int FragmentLength => _fragmentLength;

        public HighlightingOptions GetOptions(JsonOperationContext context, BlittableJsonReaderObject parameters)
        {
            if (_options != null)
                return _options;

            if (_optionsAsStringOrParameterName == null)
                return null;

            BlittableJsonReaderObject optionsJson;

            if (_optionsType == ValueTokenType.Parameter)
            {
                if (parameters == null)
                    throw new ArgumentNullException(nameof(parameters));

                if (parameters.TryGetMember(_optionsAsStringOrParameterName, out var optionsObject) == false)
                    throw new InvalidOperationException($"Parameter '{_optionsAsStringOrParameterName}' containing '{nameof(FacetOptions)}' was not present in the list of parameters.");

                optionsJson = optionsObject as BlittableJsonReaderObject;

                if (optionsJson == null)
                    throw new InvalidOperationException($"Parameter '{_optionsAsStringOrParameterName}' should contain JSON object.");
            }
            else if (_optionsType == ValueTokenType.String)
            {
                optionsJson = IndexReadOperation.ParseJsonStringIntoBlittable(_optionsAsStringOrParameterName, context);
            }
            else
                throw new InvalidOperationException($"Unknown options type '{_optionsType}'.");

            var options = (HighlightingOptions)EntityToBlittable.ConvertToEntity(typeof(HighlightingOptions), "highlighting/options", optionsJson, DocumentConventions.Default);
            if (_optionsType == ValueTokenType.String)
                _options = options;

            return options;
        }

        public void AddOptions(string optionsAsStringOrParameterName, ValueTokenType type)
        {
            if (type != ValueTokenType.String && type != ValueTokenType.Parameter)
                throw new InvalidOperationException($"{nameof(HighlightingOptions)} can only be passed as JSON string or as a parameter pointing to JSON object, but was '{type}'.");

            _optionsAsStringOrParameterName = optionsAsStringOrParameterName;
            _optionsType = type;
        }

        public void AddFragmentLength(string fragmentLengthAsString, ValueTokenType type)
        {
            if (type != ValueTokenType.Long)
                throw new InvalidOperationException($"FragmentLength can only be passed as long, but was '{type}'.");

            if (int.TryParse(fragmentLengthAsString, out _fragmentLength) == false)
                throw new InvalidOperationException($"FragmentLength needs to be a number, but was '{fragmentLengthAsString}'.");
        }

        public void AddFragmentCount(string fragmentCountAsString, ValueTokenType type)
        {
            if (type != ValueTokenType.Long)
                throw new InvalidOperationException($"FragmentCount can only be passed as long, but was '{type}'.");

            if (int.TryParse(fragmentCountAsString, out _fragmentCount) == false)
                throw new InvalidOperationException($"FragmentCount needs to be a number, but was '{fragmentCountAsString}'.");
        }
    }
}

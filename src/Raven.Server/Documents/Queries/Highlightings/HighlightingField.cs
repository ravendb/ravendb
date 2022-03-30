using System;
using Raven.Client.Documents.Queries.Highlighting;
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

            var options = FieldOptionsHelper.GetOptions<HighlightingOptions>(_optionsAsStringOrParameterName, _optionsType, parameters, context);

            if (_optionsType == ValueTokenType.String)
                _options = options;

            return options;
        }

        public void AddOptions(string optionsAsStringOrParameterName, ValueTokenType type)
        {
            FieldOptionsHelper.ValidateOptions<HighlightingOptions>(optionsAsStringOrParameterName, type);

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

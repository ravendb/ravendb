using Raven.Client.Documents.Queries.Explanation;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Explanation
{
    public sealed class ExplanationField
    {
        private string _optionsAsStringOrParameterName;

        private ValueTokenType _optionsType;

        private ExplanationOptions _options;

        public ExplanationOptions GetOptions(JsonOperationContext context, BlittableJsonReaderObject parameters)
        {
            if (_options != null)
                return _options;

            if (_optionsAsStringOrParameterName == null)
                return null;

            var options = FieldOptionsHelper.GetOptions<ExplanationOptions>(_optionsAsStringOrParameterName, _optionsType, parameters, context);

            if (_optionsType == ValueTokenType.String)
                _options = options;

            return options;
        }

        public void AddOptions(string optionsAsStringOrParameterName, ValueTokenType type)
        {
            FieldOptionsHelper.ValidateOptions<ExplanationOptions>(optionsAsStringOrParameterName, type);

            _optionsAsStringOrParameterName = optionsAsStringOrParameterName;
            _optionsType = type;
        }
    }
}

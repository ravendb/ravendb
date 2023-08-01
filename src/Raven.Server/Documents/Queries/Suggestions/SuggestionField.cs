using System;
using System.Collections.Generic;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Suggestions
{
    public sealed class SuggestionField : SelectField
    {
        private string _termAsStringOrParameterName;

        private ValueTokenType _termType;

        private List<string> _terms;

        private string _optionsAsStringOrParameterName;

        private ValueTokenType _optionsType;

        private SuggestionOptions _options;

        public SuggestionField()
        {
            IsSuggest = true;
        }

        internal bool HasOptions => _options != null || _optionsAsStringOrParameterName != null;

        public List<string> GetTerms(JsonOperationContext context, BlittableJsonReaderObject parameters)
        {
            if (_terms != null)
                return _terms;

            if (_termAsStringOrParameterName == null)
                return null;

            var terms = new List<string>();
            if (_termType == AST.ValueTokenType.Parameter)
            {
                if (parameters == null)
                    throw new ArgumentNullException(nameof(parameters));

                if (parameters.TryGetMember(_termAsStringOrParameterName, out var termsJson) == false)
                    throw new InvalidOperationException($"Parameter '{_termAsStringOrParameterName}' containing terms was not present in the list of parameters.");

                if (termsJson is BlittableJsonReaderArray termsArray)
                {
                    foreach (var item in termsArray)
                        terms.Add(item.ToString());
                }

                if (termsJson is LazyStringValue lsv)
                    terms.Add(lsv);

                if (termsJson is LazyCompressedStringValue lcsv)
                    terms.Add(lcsv);

                return terms;
            }

            if (_termType == AST.ValueTokenType.String)
            {
                terms.Add(_termAsStringOrParameterName);
                _terms = terms;

                return terms;
            }

            throw new InvalidOperationException($"Unknown options type '{_optionsType}'.");
        }

        public SuggestionOptions GetOptions(JsonOperationContext context, BlittableJsonReaderObject parameters)
        {
            if (_options != null)
                return _options;

            if (_optionsAsStringOrParameterName == null)
                return null;

            var options = FieldOptionsHelper.GetOptions<SuggestionOptions>(_optionsAsStringOrParameterName, _optionsType, parameters, context);

            if (_optionsType == AST.ValueTokenType.String)
                _options = options;

            return options;
        }

        public void AddTerm(string termAsStringOrParameterName, ValueTokenType type)
        {
            if (type != AST.ValueTokenType.String && type != AST.ValueTokenType.Parameter)
                throw new InvalidOperationException($"Term can only be passed as string or as a parameter pointing to string, but was '{type}'.");

            _termAsStringOrParameterName = termAsStringOrParameterName;
            _termType = type;
        }

        public void AddOptions(string optionsAsStringOrParameterName, ValueTokenType type)
        {
            FieldOptionsHelper.ValidateOptions<SuggestionOptions>(optionsAsStringOrParameterName, type);

            _optionsAsStringOrParameterName = optionsAsStringOrParameterName;
            _optionsType = type;
        }
    }
}

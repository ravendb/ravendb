using System;
using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.Suggestions;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Suggestions
{
    public class SuggestionField : SelectField
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

            BlittableJsonReaderObject optionsJson;

            if (_optionsType == AST.ValueTokenType.Parameter)
            {
                if (parameters == null)
                    throw new ArgumentNullException(nameof(parameters));

                if (parameters.TryGetMember(_optionsAsStringOrParameterName, out var optionsObject) == false)
                    throw new InvalidOperationException($"Parameter '{_optionsAsStringOrParameterName}' containing '{nameof(SuggestionOptions)}' was not present in the list of parameters.");

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

            var options = (SuggestionOptions)EntityToBlittable.ConvertToEntity(typeof(SuggestionOptions), "suggestion/options", optionsJson, DocumentConventions.Default);
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
            if (type != AST.ValueTokenType.String && type != AST.ValueTokenType.Parameter)
                throw new InvalidOperationException($"{nameof(FacetOptions)} can only be passed as JSON string or as a parameter pointing to JSON object, but was '{type}'.");

            _optionsAsStringOrParameterName = optionsAsStringOrParameterName;
            _optionsType = type;
        }
    }
}

using System;
using Raven.Client.Documents.Conventions;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public static class FieldOptionsHelper
    {
        public static T GetOptions<T>(string optionsAsStringOrParameterName, ValueTokenType optionsType, BlittableJsonReaderObject parameters, JsonOperationContext context)
        {
            BlittableJsonReaderObject optionsJson;

            if (optionsType == ValueTokenType.Parameter)
            {
                if (parameters == null)
                    throw new ArgumentNullException(nameof(parameters));

                if (parameters.TryGetMember(optionsAsStringOrParameterName, out var optionsObject) == false)
                    throw new InvalidOperationException($"Parameter '{optionsAsStringOrParameterName}' containing '{typeof(T).Name}' was not present in the list of parameters.");

                optionsJson = optionsObject as BlittableJsonReaderObject;

                if (optionsJson == null)
                    throw new InvalidOperationException($"Parameter '{optionsAsStringOrParameterName}' should contain JSON object.");
            }
            else if (optionsType == ValueTokenType.String)
            {
                optionsJson = LuceneIndexReadOperation.ParseJsonStringIntoBlittable(optionsAsStringOrParameterName, context);
            }
            else
                throw new InvalidOperationException($"Unknown options type '{optionsType}'.");

            return DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<T>(optionsJson, "options");
        }

        public static void ValidateOptions<T>(string optionsAsStringOrParameterName, ValueTokenType optionsType)
        {
            if (optionsType != ValueTokenType.String && optionsType != ValueTokenType.Parameter)
                throw new InvalidOperationException($"{typeof(T).Name} can only be passed as JSON string or as a parameter pointing to JSON object, but was '{optionsType}'.");
        }
    }
}

using System;
using Raven.NewClient.Abstractions.Indexing;
using Raven.Server.Documents;
using Raven.Server.Documents.Transformers;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents.Processors
{
    public class TransformerProcessor
    {
        public static TransformerDefinition ReadTransformerDefinition(BlittableJsonReaderObject reader, long buildVersion)
        {
            if (buildVersion == 0) // pre 4.0 support
                return ReadLegacyTransformerDefinition(reader);

            if (buildVersion >= 40000 && buildVersion <= 44999 || buildVersion == 40)
                return JsonDeserializationServer.TransformerDefinition(reader);

            throw new NotSupportedException($"We do not support importing transformers from '{buildVersion}' build.");
        }

        public static void Import(BlittableJsonReaderObject transformerDefinitionDoc, DocumentDatabase database, long buildVersion)
        {
            var transformerDefinition = ReadTransformerDefinition(transformerDefinitionDoc, buildVersion);
            database.TransformerStore.CreateTransformer(transformerDefinition);
        }

        public static void Export(BlittableJsonTextWriter writer, Transformer transformer, JsonOperationContext context)
        {
            writer.WriteTransformerDefinition(context, transformer.Definition);
        }

        private static TransformerDefinition ReadLegacyTransformerDefinition(BlittableJsonReaderObject reader)
        {
            string name;
            if (reader.TryGet("name", out name) == false)
                throw new InvalidOperationException("Could not read legacy index definition.");

            BlittableJsonReaderObject definition;
            if (reader.TryGet("definition", out definition) == false)
                throw new InvalidOperationException("Could not read legacy index definition.");

            var transformerDefinition = JsonDeserializationServer.TransformerDefinition(definition);
            transformerDefinition.Name = name;

            return transformerDefinition;
        }
    }
}
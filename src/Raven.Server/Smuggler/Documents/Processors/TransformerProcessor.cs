using System;
using Raven.Abstractions.Indexing;
using Raven.Server.Documents;
using Raven.Server.Documents.Transformers;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents.Processors
{
    public class TransformerProcessor
    {
        public static void Import(BlittableJsonDocumentBuilder builder, DocumentDatabase database, long buildVersion)
        {
            using (var reader = builder.CreateReader())
            {
                TransformerDefinition transformerDefinition;
                if (buildVersion == 0) // pre 4.0 support
                {
                    transformerDefinition = ReadLegacyTransformerDefinition(reader);
                }
                else if (buildVersion >= 40000 && buildVersion <= 44999)
                {
                    transformerDefinition = JsonDeserializationServer.TransformerDefinition(reader);
                }
                else
                    throw new NotSupportedException($"We do not support importing transformers from '{buildVersion}' build.");

                database.TransformerStore.CreateTransformer(transformerDefinition);
            }
        }

        public static void Export(BlittableJsonTextWriter writer, Transformer transformer, DocumentsOperationContext context)
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
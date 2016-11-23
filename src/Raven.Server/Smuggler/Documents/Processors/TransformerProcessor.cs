using System;
using System.Runtime.CompilerServices;
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Import(BlittableJsonDocumentBuilder builder, DocumentDatabase database, long buildVersion)
        {
            using (var reader = builder.CreateReader())
                Import(reader, database, buildVersion);
        }

        public static void Import(BlittableJsonReaderObject transformerDefinitionDoc, DocumentDatabase database, long buildVersion)
        {
            TransformerDefinition transformerDefinition;
            if (buildVersion == 0) // pre 4.0 support
            {
                transformerDefinition = ReadLegacyTransformerDefinition(transformerDefinitionDoc);
            }
            //I think supporting only major version as a number should be here,
            //so we can use ServerVersion.Build to get the build and not hardcode it
            else if ((buildVersion >= 40000 && buildVersion <= 44999) || (buildVersion >= 40 && buildVersion <= 44))
            {
                transformerDefinition = JsonDeserializationServer.TransformerDefinition(transformerDefinitionDoc);
            }
            else
                throw new NotSupportedException($"We do not support importing transformers from '{buildVersion}' build.");

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
using System;
using Raven.Client.Documents.Transformers;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Transformers;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents.Processors
{
    public class TransformerProcessor
    {
        public static TransformerDefinition ReadTransformerDefinition(BlittableJsonReaderObject reader, BuildVersionType buildVersionType)
        {
            switch (buildVersionType)
            {
                case BuildVersionType.V3:
                    // pre 4.0 support
                    return ReadLegacyTransformerDefinition(reader);
                case BuildVersionType.V4:
                    return JsonDeserializationServer.TransformerDefinition(reader);
                default:
                    throw new ArgumentOutOfRangeException(nameof(buildVersionType), buildVersionType, null);
            }   
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
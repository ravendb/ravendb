using Raven.Abstractions.Extensions;
using Raven.Client.Indexing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Json
{
    public static class BlittableJsonTextWriterExtensions
    {
        public static void WriteIndexDefinition(this BlittableJsonTextWriter writer, MemoryOperationContext context, IndexDefinition indexDefinition)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.Name)));
            writer.WriteString(context.GetLazyString(indexDefinition.Name));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.IndexId)));
            writer.WriteInteger(indexDefinition.IndexId);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.Type)));
            writer.WriteString(context.GetLazyString(indexDefinition.Type.ToString()));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.IsTestIndex)));
            writer.WriteBool(indexDefinition.IsTestIndex);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.LockMode)));
            writer.WriteString(context.GetLazyString(indexDefinition.LockMode.ToString()));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.MaxIndexOutputsPerDocument)));
            if (indexDefinition.MaxIndexOutputsPerDocument.HasValue)
                writer.WriteInteger(indexDefinition.MaxIndexOutputsPerDocument.Value);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.IndexVersion)));
            if (indexDefinition.IndexVersion.HasValue)
                writer.WriteInteger(indexDefinition.IndexVersion.Value);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.IsSideBySideIndex)));
            writer.WriteBool(indexDefinition.IsSideBySideIndex);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.IsTestIndex)));
            writer.WriteBool(indexDefinition.IsTestIndex);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.Reduce)));
            if (string.IsNullOrWhiteSpace(indexDefinition.Reduce) == false)
                writer.WriteString(context.GetLazyString(indexDefinition.Reduce));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.Maps)));
            writer.WriteStartArray();
            var isFirstInternal = true;
            foreach (var map in indexDefinition.Maps)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;
                writer.WriteString(context.GetLazyString(map));
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.Fields)));
            writer.WriteStartObject();
            isFirstInternal = true;
            foreach (var kvp in indexDefinition.Fields)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;
                writer.WritePropertyName(context.GetLazyString(kvp.Key));
                if (kvp.Value != null)
                    writer.WriteIndexFieldOptions(context, kvp.Value);
                else
                    writer.WriteNull();
            }
            writer.WriteEndObject();

            writer.WriteEndObject();
        }

        private static void WriteIndexFieldOptions(this BlittableJsonTextWriter writer, MemoryOperationContext context, IndexFieldOptions options)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyString(nameof(options.Analyzer)));
            if (string.IsNullOrWhiteSpace(options.Analyzer) == false)
                writer.WriteString(context.GetLazyString(options.Analyzer));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(options.Indexing)));
            if (options.Indexing.HasValue)
                writer.WriteString(context.GetLazyString(options.Indexing.ToString()));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(options.Sort)));
            if (options.Sort.HasValue)
                writer.WriteString(context.GetLazyString(options.Sort.ToString()));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(options.Storage)));
            if (options.Storage.HasValue)
                writer.WriteString(context.GetLazyString(options.Storage.ToString()));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(options.Suggestions)));
            if (options.Suggestions.HasValue)
                writer.WriteBool(options.Suggestions.Value);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(options.TermVector)));
            if (options.TermVector.HasValue)
                writer.WriteString(context.GetLazyString(options.TermVector.ToString()));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(options.Spatial)));
            if (options.Spatial != null)
            {
                writer.WriteStartObject();

                writer.WritePropertyName(context.GetLazyString(nameof(options.Spatial.Type)));
                writer.WriteString(context.GetLazyString(options.Spatial.Type.ToString()));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(options.Spatial.MaxTreeLevel)));
                writer.WriteInteger(options.Spatial.MaxTreeLevel);
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(options.Spatial.MaxX)));
                writer.WriteDouble(new LazyDoubleValue(context.GetLazyString(options.Spatial.MaxX.ToInvariantString())));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(options.Spatial.MaxY)));
                writer.WriteDouble(new LazyDoubleValue(context.GetLazyString(options.Spatial.MaxY.ToInvariantString())));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(options.Spatial.MinX)));
                writer.WriteDouble(new LazyDoubleValue(context.GetLazyString(options.Spatial.MinX.ToInvariantString())));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(options.Spatial.MinY)));
                writer.WriteDouble(new LazyDoubleValue(context.GetLazyString(options.Spatial.MinY.ToInvariantString())));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(options.Spatial.Strategy)));
                writer.WriteString(context.GetLazyString(options.Spatial.Strategy.ToString()));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(options.Spatial.Units)));
                writer.WriteString(context.GetLazyString(options.Spatial.Units.ToString()));

                writer.WriteEndObject();
            }
            else
                writer.WriteNull();

            writer.WriteEndObject();
        }
    }
}
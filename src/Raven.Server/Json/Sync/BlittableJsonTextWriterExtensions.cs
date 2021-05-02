using System;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Server.Documents;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Json.Sync
{
    internal static class BlittableJsonTextWriterExtensions
    {
        public static void WriteIndexDefinition(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexDefinition indexDefinition, bool removeAnalyzers = false)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(indexDefinition.Name));
            writer.WriteString(indexDefinition.Name);
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.SourceType));
            writer.WriteString(indexDefinition.SourceType.ToString());
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.Type));
            writer.WriteString(indexDefinition.Type.ToString());
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.LockMode));
            if (indexDefinition.LockMode.HasValue)
                writer.WriteString(indexDefinition.LockMode.ToString());
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.Priority));
            if (indexDefinition.Priority.HasValue)
                writer.WriteString(indexDefinition.Priority.ToString());
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.OutputReduceToCollection));
            writer.WriteString(indexDefinition.OutputReduceToCollection);
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.ReduceOutputIndex));

            if (indexDefinition.ReduceOutputIndex.HasValue)
                writer.WriteInteger(indexDefinition.ReduceOutputIndex.Value);
            else
                writer.WriteNull();
            writer.WriteComma();
            
            writer.WritePropertyName(nameof(indexDefinition.Rolling));
            if (indexDefinition.Rolling.HasValue)
                writer.WriteBool(indexDefinition.Rolling.Value);
            else 
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.PatternForOutputReduceToCollectionReferences));
            writer.WriteString(indexDefinition.PatternForOutputReduceToCollectionReferences);
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.PatternReferencesCollectionName));
            writer.WriteString(indexDefinition.PatternReferencesCollectionName);
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.Configuration));
            writer.WriteStartObject();
            var isFirstInternal = true;
            foreach (var kvp in indexDefinition.Configuration)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;

                writer.WritePropertyName(kvp.Key);
                writer.WriteString(kvp.Value);
            }
            writer.WriteEndObject();
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.AdditionalSources));
            writer.WriteStartObject();
            isFirstInternal = true;
            foreach (var kvp in indexDefinition.AdditionalSources)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;

                writer.WritePropertyName(kvp.Key);
                writer.WriteString(kvp.Value);
            }
            writer.WriteEndObject();
            writer.WriteComma();

#if FEATURE_TEST_INDEX
            writer.WritePropertyName(nameof(indexDefinition.IsTestIndex));
            writer.WriteBool(indexDefinition.IsTestIndex);
            writer.WriteComma();
#endif

            writer.WritePropertyName(nameof(indexDefinition.Reduce));
            if (string.IsNullOrWhiteSpace(indexDefinition.Reduce) == false)
                writer.WriteString(indexDefinition.Reduce);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.Maps));
            writer.WriteStartArray();
            isFirstInternal = true;
            foreach (var map in indexDefinition.Maps)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;
                writer.WriteString(map);
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName(nameof(indexDefinition.Fields));
            writer.WriteStartObject();
            isFirstInternal = true;
            foreach (var kvp in indexDefinition.Fields)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;
                writer.WritePropertyName(kvp.Key);
                if (kvp.Value != null)
                    writer.WriteIndexFieldOptions(context, kvp.Value, removeAnalyzers);
                else
                    writer.WriteNull();
            }
            writer.WriteEndObject();

            writer.WriteEndObject();
        }

        private static void WriteIndexFieldOptions(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexFieldOptions options, bool removeAnalyzers)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(options.Analyzer));
            if (string.IsNullOrWhiteSpace(options.Analyzer) == false && !removeAnalyzers)
                writer.WriteString(options.Analyzer);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(options.Indexing));
            if (options.Indexing.HasValue)
                writer.WriteString(options.Indexing.ToString());
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(options.Storage));
            if (options.Storage.HasValue)
                writer.WriteString(options.Storage.ToString());
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(options.Suggestions));
            if (options.Suggestions.HasValue)
                writer.WriteBool(options.Suggestions.Value);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(options.TermVector));
            if (options.TermVector.HasValue)
                writer.WriteString(options.TermVector.ToString());
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(nameof(options.Spatial));
            if (options.Spatial != null)
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(options.Spatial.Type));
                writer.WriteString(options.Spatial.Type.ToString());
                writer.WriteComma();

                writer.WritePropertyName(nameof(options.Spatial.MaxTreeLevel));
                writer.WriteInteger(options.Spatial.MaxTreeLevel);
                writer.WriteComma();

                writer.WritePropertyName(nameof(options.Spatial.MaxX));
                LazyStringValue lazyStringValue;
                using (lazyStringValue = context.GetLazyString(CharExtensions.ToInvariantString(options.Spatial.MaxX)))
                    writer.WriteDouble(new LazyNumberValue(lazyStringValue));
                writer.WriteComma();

                writer.WritePropertyName(nameof(options.Spatial.MaxY));
                using (lazyStringValue = context.GetLazyString(CharExtensions.ToInvariantString(options.Spatial.MaxY)))
                    writer.WriteDouble(new LazyNumberValue(lazyStringValue));
                writer.WriteComma();

                writer.WritePropertyName(nameof(options.Spatial.MinX));
                using (lazyStringValue = context.GetLazyString(CharExtensions.ToInvariantString(options.Spatial.MinX)))
                    writer.WriteDouble(new LazyNumberValue(lazyStringValue));
                writer.WriteComma();

                writer.WritePropertyName(nameof(options.Spatial.MinY));
                using (lazyStringValue = context.GetLazyString(CharExtensions.ToInvariantString(options.Spatial.MinY)))
                    writer.WriteDouble(new LazyNumberValue(lazyStringValue));
                writer.WriteComma();

                writer.WritePropertyName(nameof(options.Spatial.Strategy));
                writer.WriteString(options.Spatial.Strategy.ToString());
                writer.WriteComma();

                writer.WritePropertyName(nameof(options.Spatial.Units));
                writer.WriteString(options.Spatial.Units.ToString());

                writer.WriteEndObject();
            }
            else
                writer.WriteNull();

            writer.WriteEndObject();
        }

        public static void WriteDocument(this BlittableJsonTextWriter writer, JsonOperationContext context, Document document, bool metadataOnly, Func<LazyStringValue, bool> filterMetadataProperty = null)
        {
            if (document == null)
            {
                writer.WriteNull();
                return;
            }

            if (document == Document.ExplicitNull)
            {
                writer.WriteNull();
                return;
            }

            // Explicitly not disposing it, a single document can be
            // used multiple times in a single query, for example, due to projections
            // so we will let the context handle it, rather than handle it directly ourselves
            //using (document.Data)
            {
                if (metadataOnly == false)
                    writer.WriteDocumentInternal(context, document, filterMetadataProperty);
                else
                    writer.WriteDocumentMetadata(context, document, filterMetadataProperty);
            }
        }

        internal static void WriteDocumentInternal(this BlittableJsonTextWriter writer, JsonOperationContext context, Document document, Func<LazyStringValue, bool> filterMetadataProperty = null)
        {
            writer.WriteStartObject();
            WriteDocumentProperties(writer, context, document, filterMetadataProperty);
            writer.WriteEndObject();
        }

        private static void WriteDocumentProperties(this BlittableJsonTextWriter writer, JsonOperationContext context, Document document, Func<LazyStringValue, bool> filterMetadataProperty = null)
        {
            var first = true;
            BlittableJsonReaderObject metadata = null;
            var metadataField = context.GetLazyStringForFieldWithCaching(Raven.Server.Json.BlittableJsonTextWriterExtensions.MetadataKeySegment);

            var prop = new BlittableJsonReaderObject.PropertyDetails();
            using (var buffers = document.Data.GetPropertiesByInsertionOrder())
            {
                for (var i = 0; i < buffers.Size; i++)
                {
                    unsafe
                    {
                        document.Data.GetPropertyByIndex(buffers.Properties[i], ref prop);
                    }

                    if (metadataField.Equals(prop.Name))
                    {
                        metadata = (BlittableJsonReaderObject)prop.Value;
                        continue;
                    }
                    if (first == false)
                    {
                        writer.WriteComma();
                    }
                    first = false;
                    writer.WritePropertyName((string)prop.Name);
                    writer.WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                }
            }

            if (first == false)
                writer.WriteComma();
            WriteMetadata(writer, document, metadata, filterMetadataProperty);
        }

        private static void WriteDocumentMetadata(this BlittableJsonTextWriter writer, JsonOperationContext context, Document document, Func<LazyStringValue, bool> filterMetadataProperty = null)
        {
            writer.WriteStartObject();
            document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata);
            WriteMetadata(writer, document, metadata, filterMetadataProperty);

            writer.WriteEndObject();
        }

        private static void WriteMetadata(BlittableJsonTextWriter writer, Document document, BlittableJsonReaderObject metadata, Func<LazyStringValue, bool> filterMetadataProperty = null)
        {
            writer.WritePropertyName(Constants.Documents.Metadata.Key);
            writer.WriteStartObject();
            bool first = true;
            if (metadata != null)
            {
                var size = metadata.Count;
                var prop = new BlittableJsonReaderObject.PropertyDetails();

                for (int i = 0; i < size; i++)
                {
                    metadata.GetPropertyByIndex(i, ref prop);

                    if (filterMetadataProperty != null && filterMetadataProperty(prop.Name))
                        continue;

                    if (first == false)
                    {
                        writer.WriteComma();
                    }
                    first = false;
                    writer.WritePropertyName((string)prop.Name);
                    writer.WriteValue(prop.Token & BlittableJsonReaderBase.TypesMask, prop.Value);
                }
            }

            if (first == false)
            {
                writer.WriteComma();
            }
            writer.WritePropertyName(Constants.Documents.Metadata.ChangeVector);
            writer.WriteString(document.ChangeVector);

            if (document.Flags != DocumentFlags.None)
            {
                writer.WriteComma();
                writer.WritePropertyName(Constants.Documents.Metadata.Flags);
                writer.WriteString(document.Flags.ToString());
            }
            if (document.Id != null)
            {
                writer.WriteComma();
                writer.WritePropertyName(Constants.Documents.Metadata.Id);
                writer.WriteString(document.Id);
            }
            if (document.IndexScore != null)
            {
                writer.WriteComma();
                writer.WritePropertyName(Constants.Documents.Metadata.IndexScore);
                writer.WriteDouble(document.IndexScore.Value);
            }
            if (document.Distance != null)
            {
                writer.WriteComma();
                var result = document.Distance.Value;
                writer.WritePropertyName(Constants.Documents.Metadata.SpatialResult);
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(result.Distance));
                writer.WriteDouble(result.Distance);
                writer.WriteComma();
                writer.WritePropertyName(nameof(result.Latitude));
                writer.WriteDouble(result.Latitude);
                writer.WriteComma();
                writer.WritePropertyName(nameof(result.Longitude));
                writer.WriteDouble(result.Longitude);
                writer.WriteEndObject();
            }
            if (document.LastModified != DateTime.MinValue)
            {
                writer.WriteComma();
                writer.WritePropertyName(Constants.Documents.Metadata.LastModified);
                writer.WriteDateTime(document.LastModified, isUtc: true);
            }
            writer.WriteEndObject();
        }
    }
}

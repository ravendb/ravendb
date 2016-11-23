using System;
using System.Runtime.CompilerServices;
using Raven.Abstractions.Indexing;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Json;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents.Processors
{
    public static class IndexProcessor
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Import(BlittableJsonDocumentBuilder builder, DocumentDatabase database, long buildVersion)
        {
            using (var reader = builder.CreateReader())
                Import(reader, database, buildVersion);
        }

        public static void Import(BlittableJsonReaderObject indexDefinitionDoc, DocumentDatabase database, long buildVersion)
        {
            if (buildVersion == 0) // pre 4.0 support
            {
                var indexDefinition = ReadLegacyIndexDefinition(indexDefinitionDoc);
                if (string.Equals(indexDefinition.Name, "Raven/DocumentsByEntityName", StringComparison.OrdinalIgnoreCase))
                    // skipping not needed old default index
                    return;

                database.IndexStore.CreateIndex(indexDefinition);
            }
            //I think supporting only major version as a number should be here,
            //so we can use ServerVersion.Build to get the build and not hardcode it
            else if (buildVersion == 13 || (buildVersion >= 40000 && buildVersion <= 44999) || (buildVersion >= 40 && buildVersion <= 44))
            {
                var indexType = ReadIndexType(indexDefinitionDoc);
                var definition = ReadIndexDefinition(indexDefinitionDoc);
                switch (indexType)
                {
                    case IndexType.AutoMap:
                        var autoMapIndexDefinition = AutoMapIndexDefinition.LoadFromJson(definition);
                        database.IndexStore.CreateIndex(autoMapIndexDefinition);
                        break;
                    case IndexType.AutoMapReduce:
                        var autoMapReduceIndexDefinition = AutoMapReduceIndexDefinition.LoadFromJson(definition);
                        database.IndexStore.CreateIndex(autoMapReduceIndexDefinition);
                        break;
                    case IndexType.Map:
                    case IndexType.MapReduce:
                        var indexDefinition = JsonDeserializationServer.IndexDefinition(definition);
                        database.IndexStore.CreateIndex(indexDefinition);
                        break;
                    default:
                        throw new NotSupportedException(indexType.ToString());
                }
            }
            else
                throw new NotSupportedException($"We do not support importing indexes from '{buildVersion}' build.");
        }

        public static void Export(BlittableJsonTextWriter writer, Index index, JsonOperationContext context, bool removeAnalyzers)
        {
            if (index.Type == IndexType.Faulty)
                return;

            writer.WriteStartObject();

            writer.WritePropertyName(nameof(IndexDefinition.Type));
            writer.WriteString(index.Type.ToString());
            writer.WriteComma();

            writer.WritePropertyName(nameof(IndexDefinition));

            if (index.Type == IndexType.Map || index.Type == IndexType.MapReduce)
            {
                var indexDefinition = index.GetIndexDefinition();
                writer.WriteIndexDefinition(context, indexDefinition, removeAnalyzers);
            }
            else if (index.Type == IndexType.AutoMap || index.Type == IndexType.AutoMapReduce)
            {
                index.Definition.Persist(context, writer);
            }
            else
            {
                throw new NotSupportedException(index.Type.ToString());
            }

            writer.WriteEndObject();
        }

        private static BlittableJsonReaderObject ReadIndexDefinition(BlittableJsonReaderObject reader)
        {
            BlittableJsonReaderObject json;
            if (reader.TryGet(nameof(IndexDefinition), out json) == false)
                throw new InvalidOperationException("Could not read index definition.");

            return json;
        }

        private static IndexType ReadIndexType(BlittableJsonReaderObject reader)
        {
            string typeAsString;
            if (reader.TryGet(nameof(IndexDefinition.Type), out typeAsString) == false)
                throw new InvalidOperationException("Could not read index type.");

            return (IndexType)Enum.Parse(typeof(IndexType), typeAsString, ignoreCase: true);
        }

        private static IndexDefinition ReadLegacyIndexDefinition(BlittableJsonReaderObject reader)
        {
            string name;
            if (reader.TryGet("name", out name) == false)
                throw new InvalidOperationException("Could not read legacy index definition.");

            BlittableJsonReaderObject definition;
            if (reader.TryGet("definition", out definition) == false)
                throw new InvalidOperationException("Could not read legacy index definition.");

            var legacyIndexDefinition = JsonDeserializationServer.LegacyIndexDefinition(definition);

            var indexDefinition = new IndexDefinition
            {
                IndexId = legacyIndexDefinition.IndexId,
                LockMode = legacyIndexDefinition.LockMode,
                Maps = legacyIndexDefinition.Maps,
                Name = name,
                Reduce = legacyIndexDefinition.Reduce
            };

            if (legacyIndexDefinition.MaxIndexOutputsPerDocument.HasValue)
                indexDefinition.Configuration.MaxIndexOutputsPerDocument = legacyIndexDefinition.MaxIndexOutputsPerDocument;

            foreach (var kvp in legacyIndexDefinition.Analyzers)
            {
                if (indexDefinition.Fields.ContainsKey(kvp.Key) == false)
                    indexDefinition.Fields[kvp.Key] = new IndexFieldOptions();

                indexDefinition.Fields[kvp.Key].Analyzer = kvp.Value;
            }

            foreach (var kvp in legacyIndexDefinition.Indexes)
            {
                if (indexDefinition.Fields.ContainsKey(kvp.Key) == false)
                    indexDefinition.Fields[kvp.Key] = new IndexFieldOptions();

                indexDefinition.Fields[kvp.Key].Indexing = kvp.Value;
            }

            foreach (var kvp in legacyIndexDefinition.SortOptions)
            {
                if (indexDefinition.Fields.ContainsKey(kvp.Key) == false)
                    indexDefinition.Fields[kvp.Key] = new IndexFieldOptions();

                SortOptions sortOptions;
                switch (kvp.Value)
                {
                    case LegacyIndexDefinition.LegacySortOptions.None:
                        sortOptions = SortOptions.None;
                        break;
                    case LegacyIndexDefinition.LegacySortOptions.String:
                        sortOptions = SortOptions.String;
                        break;
                    case LegacyIndexDefinition.LegacySortOptions.Short:
                    case LegacyIndexDefinition.LegacySortOptions.Long:
                    case LegacyIndexDefinition.LegacySortOptions.Int:
                    case LegacyIndexDefinition.LegacySortOptions.Byte:
                        sortOptions = SortOptions.NumericLong;
                        break;
                    case LegacyIndexDefinition.LegacySortOptions.Float:
                    case LegacyIndexDefinition.LegacySortOptions.Double:
                        sortOptions = SortOptions.NumericDouble;
                        break;
                    case LegacyIndexDefinition.LegacySortOptions.Custom:
                        throw new NotImplementedException(kvp.Value.ToString());
                    case LegacyIndexDefinition.LegacySortOptions.StringVal:
                        sortOptions = SortOptions.StringVal;
                        break;
                    default:
                        throw new NotSupportedException(kvp.Value.ToString());
                }

                indexDefinition.Fields[kvp.Key].Sort = sortOptions;
            }

            foreach (var kvp in legacyIndexDefinition.SpatialIndexes)
            {
                if (indexDefinition.Fields.ContainsKey(kvp.Key) == false)
                    indexDefinition.Fields[kvp.Key] = new IndexFieldOptions();

                indexDefinition.Fields[kvp.Key].Spatial = kvp.Value;
            }

            foreach (var kvp in legacyIndexDefinition.Stores)
            {
                if (indexDefinition.Fields.ContainsKey(kvp.Key) == false)
                    indexDefinition.Fields[kvp.Key] = new IndexFieldOptions();

                indexDefinition.Fields[kvp.Key].Storage = kvp.Value;
            }

            foreach (var kvp in legacyIndexDefinition.TermVectors)
            {
                if (indexDefinition.Fields.ContainsKey(kvp.Key) == false)
                    indexDefinition.Fields[kvp.Key] = new IndexFieldOptions();

                indexDefinition.Fields[kvp.Key].TermVector = kvp.Value;
            }

            foreach (var field in legacyIndexDefinition.SuggestionsOptions)
            {
                if (indexDefinition.Fields.ContainsKey(field) == false)
                    indexDefinition.Fields[field] = new IndexFieldOptions();

                indexDefinition.Fields[field].Suggestions = true;
            }

            return indexDefinition;
        }
    }
}
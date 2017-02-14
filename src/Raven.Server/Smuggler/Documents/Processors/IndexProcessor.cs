using System;
using System.Diagnostics;
using Raven.Client.Documents.Indexes;
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
        public static object ReadIndexDefinition(BlittableJsonReaderObject reader, long buildVersion, out IndexType type)
        {
            if (buildVersion == 0) // pre 4.0 support
            {
                var indexDefinition = ReadLegacyIndexDefinition(reader);

                type = indexDefinition.Type;
                Debug.Assert(type.IsStatic());

                return indexDefinition;
            }

            if (buildVersion >= 40000 && buildVersion <= 44999 || buildVersion == 40)
            {
                type = ReadIndexType(reader, out reader);
                switch (type)
                {
                    case IndexType.AutoMap:
                        return AutoMapIndexDefinition.LoadFromJson(reader);
                    case IndexType.AutoMapReduce:
                        return AutoMapReduceIndexDefinition.LoadFromJson(reader);
                    case IndexType.Map:
                    case IndexType.MapReduce:
                        return JsonDeserializationServer.IndexDefinition(reader);
                    default:
                        throw new NotSupportedException(type.ToString());
                }
            }

            throw new NotSupportedException($"We do not support importing indexes from '{buildVersion}' build.");
        }

        public static void Import(BlittableJsonReaderObject indexDefinitionDoc, DocumentDatabase database, long buildVersion, bool removeAnalyzers)
        {
            IndexType indexType;
            var definition = ReadIndexDefinition(indexDefinitionDoc, buildVersion, out indexType);
            
            switch (indexType)
            {
                case IndexType.AutoMap:
                    var autoMapIndexDefinition = (AutoMapIndexDefinition)definition;
                    database.IndexStore.CreateIndex(autoMapIndexDefinition);
                    break;
                case IndexType.AutoMapReduce:
                    var autoMapReduceIndexDefinition = (AutoMapReduceIndexDefinition)definition;
                    database.IndexStore.CreateIndex(autoMapReduceIndexDefinition);
                    break;
                case IndexType.Map:
                case IndexType.MapReduce:
                    var indexDefinition = (IndexDefinition)definition;
                    if (string.Equals(indexDefinition.Name, "Raven/DocumentsByEntityName", StringComparison.OrdinalIgnoreCase))
                        return;

                    if (removeAnalyzers)
                    {
                        foreach (var indexDefinitionField in indexDefinition.Fields)
                        {
                            indexDefinitionField.Value.Analyzer = null;
                        }
                    }
                    database.IndexStore.CreateIndex(indexDefinition);
                    break;
                default:
                    throw new NotSupportedException(indexType.ToString());
            }
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

        private static IndexType ReadIndexType(BlittableJsonReaderObject reader, out BlittableJsonReaderObject indexDef)
        {
            string typeAsString;
            if (reader.TryGet(nameof(IndexDefinition.Type), out typeAsString) == false)
                throw new InvalidOperationException("Could not read index type.");

            if (reader.TryGet(nameof(IndexDefinition), out indexDef) == false)
                throw new InvalidOperationException("Could not read index definition");


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
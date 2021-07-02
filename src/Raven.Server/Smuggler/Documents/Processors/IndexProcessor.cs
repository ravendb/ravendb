using System;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Json;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents.Processors
{
    public static class IndexProcessor
    {
        public static object ReadIndexDefinition(BlittableJsonReaderObject reader, BuildVersionType buildVersionType, out IndexType type)
        {
            switch (buildVersionType)
            {
                case BuildVersionType.V3:
                    // pre 4.0 support
                    var indexDefinition = ReadLegacyIndexDefinition(reader);

                    type = indexDefinition.Type;
                    Debug.Assert(type.IsStatic());

                    return indexDefinition;

                case BuildVersionType.V4:
                case BuildVersionType.V5:
                case BuildVersionType.GreaterThanCurrent:
                    type = ReadIndexType(reader, out reader);
                    switch (type)
                    {
                        case IndexType.AutoMap:
                            return AutoMapIndexDefinition.LoadFromJson(reader);

                        case IndexType.AutoMapReduce:
                            return AutoMapReduceIndexDefinition.LoadFromJson(reader);

                        case IndexType.Map:
                        case IndexType.MapReduce:
                        case IndexType.JavaScriptMap:
                        case IndexType.JavaScriptMapReduce:
                            return JsonDeserializationServer.IndexDefinition(reader);

                        default:
                            throw new NotSupportedException(type.ToString());
                    }
                default:
                    throw new ArgumentOutOfRangeException(nameof(buildVersionType), buildVersionType, null);
            }
        }

        public static void Import(BlittableJsonReaderObject indexDefinitionDoc, DocumentDatabase database, BuildVersionType buildType, bool removeAnalyzers)
        {
            var definition = ReadIndexDefinition(indexDefinitionDoc, buildType, out IndexType indexType);

            switch (indexType)
            {
                case IndexType.AutoMap:
                    var autoMapIndexDefinition = (AutoMapIndexDefinition)definition;
                    AsyncHelpers.RunSync(() => database.IndexStore.CreateIndex(autoMapIndexDefinition, RaftIdGenerator.NewId()));
                    break;

                case IndexType.AutoMapReduce:
                    var autoMapReduceIndexDefinition = (AutoMapReduceIndexDefinition)definition;
                    AsyncHelpers.RunSync(() => database.IndexStore.CreateIndex(autoMapReduceIndexDefinition, RaftIdGenerator.NewId()));
                    break;

                case IndexType.Map:
                case IndexType.MapReduce:
                case IndexType.JavaScriptMap:
                case IndexType.JavaScriptMapReduce:
                    var indexDefinition = (IndexDefinition)definition;
                    if (removeAnalyzers)
                    {
                        foreach (var indexDefinitionField in indexDefinition.Fields)
                        {
                            indexDefinitionField.Value.Analyzer = null;
                        }
                    }
                    AsyncHelpers.RunSync(() => database.IndexStore.CreateIndex(indexDefinition, RaftIdGenerator.NewId()));
                    break;

                default:
                    throw new NotSupportedException(indexType.ToString());
            }
        }

        private static IndexType ReadIndexType(BlittableJsonReaderObject reader, out BlittableJsonReaderObject indexDef)
        {
            if (reader.TryGet(nameof(IndexDefinition.Type), out string typeAsString) == false)
                throw new InvalidOperationException("Could not read index type.");

            if (reader.TryGet(nameof(IndexDefinition), out indexDef) == false)
                throw new InvalidOperationException("Could not read index definition");

            return (IndexType)Enum.Parse(typeof(IndexType), typeAsString, ignoreCase: true);
        }

        private static IndexDefinition ReadLegacyIndexDefinition(BlittableJsonReaderObject reader)
        {
            if (reader.TryGet("name", out string name) == false &&
                reader.TryGet("Name", out name) == false)
                throw new InvalidOperationException("Could not read legacy index definition.");

            if (reader.TryGet("definition", out BlittableJsonReaderObject definition) == false &&
                reader.TryGet("Definition", out definition) == false)
                throw new InvalidOperationException("Could not read legacy index definition.");

            var legacyIndexDefinition = JsonDeserializationServer.LegacyIndexDefinition(definition);

            var indexDefinition = new IndexDefinition
            {
                LockMode = legacyIndexDefinition.LockMode,
                Maps = legacyIndexDefinition.Maps,
                Name = name,
                Reduce = legacyIndexDefinition.Reduce
            };

            if (indexDefinition.Maps != null)
            {
                indexDefinition.Maps = indexDefinition.Maps.Select(ReplaceLegacyProperties).ToHashSet();

                foreach (var map in indexDefinition.Maps)
                {
                    if (IsFunctionValid(map, out var message) == false)
                        throw new InvalidOperationException($"Map function of legacy index '{name}' is invalid. {message}");
                }
            }

            if (indexDefinition.Reduce != null)
            {
                indexDefinition.Reduce = ReplaceLegacyProperties(indexDefinition.Reduce);

                if (IsFunctionValid(indexDefinition.Reduce, out var message) == false)
                    throw new InvalidOperationException($"Reduce function of legacy index '{name}' is invalid. {message}");
            }

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

                FieldIndexing indexing;
                switch (kvp.Value)
                {
                    case LegacyIndexDefinition.LegacyFieldIndexing.No:
                        indexing = FieldIndexing.No;
                        break;

                    case LegacyIndexDefinition.LegacyFieldIndexing.Analyzed:
                        indexing = FieldIndexing.Search;
                        break;

                    case LegacyIndexDefinition.LegacyFieldIndexing.NotAnalyzed:
                        indexing = FieldIndexing.Exact;
                        break;

                    case LegacyIndexDefinition.LegacyFieldIndexing.Default:
                        indexing = FieldIndexing.Default;
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }

                indexDefinition.Fields[kvp.Key].Indexing = indexing;
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

        private static string ReplaceLegacyProperties(string str)
        {
            if (string.IsNullOrWhiteSpace(str))
                return str;

            str = str.Replace("new Raven.Abstractions.Linq.DynamicList", "new DynamicArray");
            var regex = new Regex(@"([\w_.]+)\.__document_id");
            return regex.Replace(str, "Id($1)");
        }

        private static bool IsFunctionValid(string function, out string message)
        {
            message = null;

            if (string.IsNullOrWhiteSpace(function))
            {
                message = "Function cannot be null.";
                return false;
            }

            if (function.Contains("__document_id"))
            {
                message = "Function cannot contain '__document_id'.";
                return false;
            }

            return true;
        }
    }
}

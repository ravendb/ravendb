using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Server;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    public class PutAutoIndexCommand : UpdateDatabaseCommand
    {
        public AutoIndexDefinition Definition;

        public PutAutoIndexCommand()
            : base(null)
        {
        }

        public PutAutoIndexCommand(AutoIndexDefinition definition, string databaseName)
            : base(databaseName)
        {
            Definition = definition;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Definition.Etag = etag;
            record.AddIndex(Definition);
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Definition)] = TypeConverter.ToBlittableSupportedType(Definition);
        }

        public static PutAutoIndexCommand Create(IndexDefinitionBase definition, string databaseName)
        {
            var indexType = IndexType.None;
            var map = definition as AutoMapIndexDefinition;
            if (map != null)
                indexType = IndexType.AutoMap;

            var reduce = definition as AutoMapReduceIndexDefinition;
            if (reduce != null)
                indexType = IndexType.AutoMapReduce;

            if (indexType == IndexType.None)
                throw new NotSupportedException("Invalid definition type: " + definition.GetType());

            return new PutAutoIndexCommand(GetAutoIndexDefinition(definition, indexType), databaseName);
        }

        public static AutoIndexDefinition GetAutoIndexDefinition(IndexDefinitionBase definition, IndexType indexType)
        {
            Debug.Assert(indexType == IndexType.AutoMap || indexType == IndexType.AutoMapReduce);

            return new AutoIndexDefinition
            {
                Collection = definition.Collections.First(),
                Etag = 0,
                MapFields = CreateFields(definition.MapFields),
                GroupByFields = indexType == IndexType.AutoMap ? null : CreateFields(((AutoMapReduceIndexDefinition)definition).GroupByFields),
                LockMode = definition.LockMode,
                Priority = definition.Priority,
                Name = definition.Name,
                Type = indexType
            };
        }

        private static Dictionary<string, AutoIndexDefinition.AutoIndexFieldOptions> CreateFields(Dictionary<string, IndexField> fields)
        {
            if (fields == null)
                return null;

            var result = new Dictionary<string, AutoIndexDefinition.AutoIndexFieldOptions>();
            foreach (var kvp in fields)
                result[kvp.Key] = new AutoIndexDefinition.AutoIndexFieldOptions
                {
                    TermVector = kvp.Value.TermVector,
                    Storage = kvp.Value.Storage,
                    Indexing = kvp.Value.Indexing,
                    Analyzer = kvp.Value.Analyzer,
                    Spatial = null,
                    Suggestions = null,
                    Sort = kvp.Value.Sort,
                    MapReduceOperation = kvp.Value.Aggregation
                };

            return result;
        }
    }
}
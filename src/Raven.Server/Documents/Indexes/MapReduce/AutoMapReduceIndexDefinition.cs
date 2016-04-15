using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexing;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class AutoMapReduceIndexDefinition : IndexDefinitionBase
    {
        public readonly IndexField[] GroupByFields;

        public AutoMapReduceIndexDefinition(string[] collections, IndexField[] mapFields, IndexField[] groupByFields)
            : base(IndexNameFinder.FindMapReduceIndexName(collections, mapFields, groupByFields), collections, IndexLockMode.Unlock, mapFields)
        {
            foreach (var field in mapFields)
            {
                if (field.Storage != FieldStorage.Yes)
                    throw new ArgumentException($"Map-reduce field has to be stored. Field name: {field.Name}");
            }

            foreach (var field in groupByFields)
            {
                if (field.Storage != FieldStorage.Yes)
                    throw new ArgumentException($"GroupBy field has to be stored. Field name: {field.Name}");
            }

            GroupByFields = groupByFields;
        }

        public bool ContainsGroupByField(string name)
        {
            return GroupByFields.Any(x => x.Name == name);
        }

        protected override void Persist(TransactionOperationContext context, BlittableJsonTextWriter writer)
        {
        }

        protected override void FillIndexDefinition(IndexDefinition indexDefinition)
        {
        }

        public override bool Equals(IndexDefinitionBase indexDefinition, bool ignoreFormatting, bool ignoreMaxIndexOutputs)
        {
            return false;
        }
    }
}
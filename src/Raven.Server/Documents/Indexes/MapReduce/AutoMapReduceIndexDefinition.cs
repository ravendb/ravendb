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
            GroupByFields = groupByFields;
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
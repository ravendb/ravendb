using Raven.Abstractions.Indexing;
using Raven.Client.Indexing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class AutoMapReduceIndexDefinition : IndexDefinitionBase
    {
        public IndexField[] GroupByFields;

        public AutoMapReduceIndexDefinition(string name, string[] collections, IndexField[] mapFields, IndexField[] groupByFields)
            : base(name, collections, IndexLockMode.Unlock, mapFields)
        {
            GroupByFields = groupByFields;
        }

        public override void Persist(TransactionOperationContext context)
        {
        }

        protected override void FillIndexDefinition(IndexDefinition indexDefinition)
        {
        }
    }
}
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes
{
    public class AutoMapReduceIndexDefinition : IndexDefinitionBase
    {
        public IndexField[] GroupByFields;

        public AutoMapReduceIndexDefinition(string name, string[] collections, IndexField[] mapFields, IndexField[] groupByFields) : base(name, collections,mapFields)
        {
            GroupByFields = groupByFields;
        }

        public override void Persist(TransactionOperationContext context)
        {
            throw new System.NotImplementedException();
        }
    }
}
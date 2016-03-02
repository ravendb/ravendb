using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes
{
    public class AutoMapReduceIndexDefinition : IndexDefinitionBase
    {
        public AutoMapReduceIndexDefinition(string name, string[] collections, IndexField[] mapFields) : base(name, collections,mapFields)
        {
        }

        public override void Persist(TransactionOperationContext context)
        {
            throw new System.NotImplementedException();
        }
    }
}
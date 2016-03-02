using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes
{
    public abstract class IndexDefinitionBase
    {
        protected static readonly Slice DefinitionSlice = "Definition";

        protected IndexDefinitionBase(string name, string[] collections)
        {
            Name = name;
            Collections = collections;
        }

        public string Name { get; private set; }

        public string[] Collections { get; private set; }

        public abstract IndexField[] MapFields { get; }

        public abstract void Persist(TransactionOperationContext context);
    }

    public class AutoMapReduceIndexDefinition : IndexDefinitionBase
    {
        public AutoMapReduceIndexDefinition(string name, string[] collections) : base(name, collections)
        {
        }

        public override IndexField[] MapFields { get; }
        public override void Persist(TransactionOperationContext context)
        {
            throw new System.NotImplementedException();
        }
    }
}
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes
{
    public abstract class IndexDefinitionBase
    {
        protected static readonly Slice DefinitionSlice = "Definition";

        protected IndexDefinitionBase(string name, string[] collections, IndexField[] mapFields)
        {
            Name = name;
            Collections = collections;
            MapFields = mapFields;
        }

        public string Name { get; set; }

        public string[] Collections { get; set; }

        public IndexField[] MapFields { get; set; }

        public abstract void Persist(TransactionOperationContext context);
    }

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
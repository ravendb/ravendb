using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class MapReduceIndexDefinition : StaticMapIndexDefinition
    {
        public MapReduceIndexDefinition(IndexDefinition definition, string[] collections)
            : base(definition, collections)
        {
        }

        protected override void PersistFields(TransactionOperationContext context, BlittableJsonTextWriter writer)
        {
            PersistMapFields(context, writer);

            // TODO arek - groupby fields, definitions etc
        }

        protected override void FillIndexDefinition(IndexDefinition indexDefinition)
        {
            throw new System.NotImplementedException();
        }

        public override bool Equals(IndexDefinitionBase indexDefinition, bool ignoreFormatting, bool ignoreMaxIndexOutputs)
        {
            throw new System.NotImplementedException();
        }

        public override bool Equals(IndexDefinition indexDefinition, bool ignoreFormatting, bool ignoreMaxIndexOutputs)
        {
            throw new System.NotImplementedException();
        }
    }
}
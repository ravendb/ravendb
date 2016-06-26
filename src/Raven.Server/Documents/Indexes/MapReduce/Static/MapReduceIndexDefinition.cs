using System.Collections.Generic;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class MapReduceIndexDefinition : StaticMapIndexDefinition
    {
        public MapReduceIndexDefinition(IndexDefinition definition, string[] collections, string[] groupByFields)
            : base(definition, collections)
        {
            GroupByFields = new HashSet<string>(groupByFields);
        }

        public HashSet<string> GroupByFields { get; private set; }

        protected override void PersistFields(TransactionOperationContext context, BlittableJsonTextWriter writer)
        {
            PersistMapFields(context, writer);

            // TODO arek - groupby fields, definitions etc
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
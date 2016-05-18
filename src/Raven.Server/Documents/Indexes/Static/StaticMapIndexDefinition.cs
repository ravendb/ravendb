using System;

using Raven.Abstractions.Indexing;
using Raven.Client.Indexing;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static
{
    public class StaticMapIndexDefinition : IndexDefinitionBase
    {
        private IndexDefinition _definition;

        public StaticMapIndexDefinition(string name, string[] collections, IndexLockMode lockMode, IndexField[] mapFields)
            : base(name, collections, lockMode, mapFields)
        {
        }

        public StaticMapIndexDefinition(IndexDefinition definition, string[] collections)
            : base(definition.Name, collections, definition.LockMode, null)
        {
            _definition = definition;
        }

        protected override void PersistFields(TransactionOperationContext context, BlittableJsonTextWriter writer)
        {
            throw new System.NotImplementedException();
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
            throw new NotImplementedException();
        }
    }
}
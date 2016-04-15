using System;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexing;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Errors
{
    public class FaultyIndexDefinition : IndexDefinitionBase
    {
        public FaultyIndexDefinition(string name, string[] collections, IndexLockMode lockMode, IndexField[] mapFields) 
            : base(name, collections, lockMode, mapFields)
        {
        }

        protected override void PersisFields(TransactionOperationContext context, BlittableJsonTextWriter writer)
        {
            throw new NotSupportedException($"Definition of a faulty '{Name}' index does not support that");
        }

        protected override void FillIndexDefinition(IndexDefinition indexDefinition)
        {
            throw new NotSupportedException($"Definition of a faulty '{Name}' index does not support that");
        }

        public override bool Equals(IndexDefinitionBase indexDefinition, bool ignoreFormatting, bool ignoreMaxIndexOutputs)
        {
            throw new NotSupportedException($"Definition of a faulty '{Name}' index does not support that");
        }
    }
}
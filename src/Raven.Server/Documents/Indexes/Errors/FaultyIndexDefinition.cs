using System;
using System.Collections.Generic;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Indexing;

using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Errors
{
    public class FaultyIndexDefinition : IndexDefinitionBase
    {
        public FaultyIndexDefinition(string name, HashSet<string> collections, IndexLockMode lockMode, IndexField[] mapFields)
            : base(name, collections, lockMode, mapFields)
        {
        }

        protected override void PersistFields(JsonOperationContext context, BlittableJsonTextWriter writer)
        {
            throw new NotSupportedException($"Definition of a faulty '{Name}' index does not support that");
        }

        protected override IndexDefinition CreateIndexDefinition()
        {
            throw new NotSupportedException($"Definition of a faulty '{Name}' index does not support that");
        }

        protected override int ComputeRestOfHash(int hashCode)
        {
            return (hashCode * 397) ^ -1337;
        }

        public override IndexDefinitionCompareDifferences Compare(IndexDefinitionBase indexDefinition)
        {
            return IndexDefinitionCompareDifferences.All;
        }

        public override IndexDefinitionCompareDifferences Compare(IndexDefinition indexDefinition)
        {
            return IndexDefinitionCompareDifferences.All;
        }
    }
}
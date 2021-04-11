using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Errors
{
    public class FaultyAutoIndexDefinition : IndexDefinitionBaseServerSide<IndexField>
    {
        public readonly AutoIndexDefinitionBase Definition;

        public FaultyAutoIndexDefinition(string name, HashSet<string> collections, IndexLockMode lockMode, IndexPriority priority, IndexState state, IndexField[] mapFields, AutoIndexDefinitionBase definition)
            : base(name, collections, lockMode, priority, state, mapFields, definition.Version, definition.ClusterIndex?.ClusterIndexForState)
        {
            Definition = definition;
        }

        protected override void PersistMapFields(JsonOperationContext context, BlittableJsonTextWriter writer)
        {
            throw new NotSupportedException($"Definition of a faulty '{Name}' auto index does not support that");
        }

        protected override void PersistFields(JsonOperationContext context, BlittableJsonTextWriter writer)
        {
            throw new NotSupportedException($"Definition of a faulty '{Name}' auto index does not support that");
        }

        protected internal override IndexDefinition GetOrCreateIndexDefinitionInternal()
        {
            var definition = Definition.GetOrCreateIndexDefinitionInternal();
            definition.Name = Name;
            definition.LockMode = LockMode;
            definition.Priority = Priority;
            definition.State = State;
            return definition;
        }

        protected override int ComputeRestOfHash(int hashCode)
        {
            return (hashCode * 397) ^ -1337;
        }

        public override IndexDefinitionCompareDifferences Compare(IndexDefinitionBaseServerSide indexDefinition)
        {
            return Definition.Compare(indexDefinition);
        }

        public override IndexDefinitionCompareDifferences Compare(IndexDefinition indexDefinition)
        {
            return Definition.Compare(indexDefinition);
        }

        internal override void Reset()
        {
            Definition.Reset();
        }
    }
}

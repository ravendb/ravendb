using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Extensions;
using Raven.Server.Json;

using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Indexes.Static
{
    public class MapIndexDefinition : IndexDefinitionBase
    {
        private readonly bool _hasDynamicFields;
        public readonly IndexDefinition IndexDefinition;

        public MapIndexDefinition(IndexDefinition definition, HashSet<string> collections, string[] outputFields, bool hasDynamicFields)
            : base(definition.Name, collections, definition.LockMode ?? IndexLockMode.Unlock, definition.Priority ?? IndexPriority.Normal, GetFields(definition, outputFields))
        {
            _hasDynamicFields = hasDynamicFields;
            IndexDefinition = definition;
        }

        public override bool HasDynamicFields => _hasDynamicFields;

        private static IndexField[] GetFields(IndexDefinition definition, string[] outputFields)
        {
            IndexFieldOptions allFields;
            definition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out allFields);

            var result = definition.Fields
                .Where(x => x.Key != Constants.Documents.Indexing.Fields.AllFields)
                .Select(x => IndexField.Create(x.Key, x.Value, allFields)).ToList();

            foreach (var outputField in outputFields)
            {
                if (definition.Fields.ContainsKey(outputField))
                    continue;

                result.Add(IndexField.Create(outputField, new IndexFieldOptions(), allFields));
            }

            return result.ToArray();
        }

        protected override void PersistFields(JsonOperationContext context, BlittableJsonTextWriter writer)
        {
            var builder = IndexDefinition.ToJson();
            using (var json = context.ReadObject(builder, nameof(IndexDefinition), BlittableJsonDocumentBuilder.UsageMode.ToDisk))
            {
                writer.WritePropertyName(nameof(IndexDefinition));
                writer.WriteObject(json);
            }
        }

        protected internal override IndexDefinition GetOrCreateIndexDefinitionInternal()
        {
            return IndexDefinition.Clone();
        }

        public override IndexDefinitionCompareDifferences Compare(IndexDefinitionBase indexDefinition)
        {
            return IndexDefinitionCompareDifferences.All;
        }

        public override IndexDefinitionCompareDifferences Compare(IndexDefinition indexDefinition)
        {
            return IndexDefinition.Compare(indexDefinition);
        }

        protected override int ComputeRestOfHash(int hashCode)
        {
            return hashCode * 397 ^ IndexDefinition.GetHashCode();
        }

        public static IndexDefinition Load(StorageEnvironment environment)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var tx = environment.ReadTransaction())
            {
                var tree = tx.CreateTree("Definition");
                var result = tree.Read(DefinitionSlice);
                if (result == null)
                    return null;

                using (var reader = context.ReadForDisk(result.Reader.AsStream(), string.Empty))
                {
                    var definition = ReadIndexDefinition(reader);
                    definition.Name = ReadName(reader);
                    definition.LockMode = ReadLockMode(reader);
                    definition.Priority = ReadPriority(reader);

                    return definition;
                }
            }
        }

        private static IndexDefinition ReadIndexDefinition(BlittableJsonReaderObject reader)
        {
            BlittableJsonReaderObject jsonObject;
            if (reader.TryGet(nameof(IndexDefinition), out jsonObject) == false || jsonObject == null)
                throw new InvalidOperationException("No persisted definition");

            return JsonDeserializationServer.IndexDefinition(jsonObject);
        }
    }
}
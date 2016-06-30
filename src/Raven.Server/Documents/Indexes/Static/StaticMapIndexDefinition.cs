using System;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Client.Indexing;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Indexes.Static
{
    public class StaticMapIndexDefinition : IndexDefinitionBase
    {
        public readonly IndexDefinition IndexDefinition;

        public StaticMapIndexDefinition(IndexDefinition definition, string[] collections)
            : base(definition.Name, collections, definition.LockMode, GetFields(definition))
        {
            IndexDefinition = definition;
        }

        private static IndexField[] GetFields(IndexDefinition definition)
        {
            if (definition.Fields == null || definition.Fields.Count == 0)
                return new IndexField[0];

            IndexFieldOptions allFields;
            definition.Fields.TryGetValue(Constants.AllFields, out allFields);

            return definition
                .Fields
                .Select(x => IndexField.Create(x.Key, x.Value, allFields))
                .ToArray();
        }

        protected override void PersistFields(TransactionOperationContext context, BlittableJsonTextWriter writer)
        {
            var builder = IndexDefinition.ToJson();
            using (var json = context.ReadObject(builder, nameof(IndexDefinition), BlittableJsonDocumentBuilder.UsageMode.ToDisk))
            {
                writer.WritePropertyName(context.GetLazyString(nameof(IndexDefinition)));
                writer.WriteObject(json);
            }
        }

        protected override IndexDefinition CreateIndexDefinition()
        {
            return IndexDefinition;
        }

        public override bool Equals(IndexDefinitionBase indexDefinition, bool ignoreFormatting, bool ignoreMaxIndexOutputs)
        {
            return false;
        }

        public override bool Equals(IndexDefinition indexDefinition, bool ignoreFormatting, bool ignoreMaxIndexOutputs)
        {
            return IndexDefinition.Equals(indexDefinition, compareIndexIds: false, ignoreFormatting: ignoreFormatting, ignoreMaxIndexOutput: ignoreMaxIndexOutputs);
        }

        public static StaticMapIndexDefinition Load(StorageEnvironment environment)
        {
            using (var pool = new UnmanagedBuffersPool(nameof(StaticMapIndexDefinition)))
            using (var context = new JsonOperationContext(pool))
            using (var tx = environment.ReadTransaction())
            {
                var tree = tx.CreateTree("Definition");
                var result = tree.Read(DefinitionSlice);
                if (result == null)
                    return null;

                using (var reader = context.ReadForDisk(result.Reader.AsStream(), string.Empty))
                {
                    var lockMode = ReadLockMode(reader);
                    var collections = ReadCollections(reader);

                    var definition = ReadIndexDefinition(reader);
                    definition.Name = ReadName(reader);

                    return new StaticMapIndexDefinition(definition, collections)
                    {
                        LockMode = lockMode
                    };
                }
            }
        }

        private static IndexDefinition ReadIndexDefinition(BlittableJsonReaderObject reader)
        {
            BlittableJsonReaderObject jsonObject;
            if (reader.TryGet(nameof(IndexDefinition), out jsonObject) == false || jsonObject == null)
                throw new InvalidOperationException("No persisted definition");

            return JsonDeserialization.IndexDefinition(jsonObject);
        }
    }
}
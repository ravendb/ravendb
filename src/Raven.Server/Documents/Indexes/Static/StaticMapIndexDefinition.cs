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

        public StaticMapIndexDefinition(IndexDefinition definition, string[] collections, string[] outputFields)
            : base(definition.Name, collections, definition.LockMode, GetFields(definition, outputFields))
        {
            IndexDefinition = definition;
        }

        private static IndexField[] GetFields(IndexDefinition definition, string[] outputFields)
        {
            IndexFieldOptions allFields;
            definition.Fields.TryGetValue(Constants.AllFields, out allFields);

            var result = definition.Fields
                .Where(x => x.Key != Constants.AllFields)
                .Select(x => IndexField.Create(x.Key, x.Value, allFields)).ToList();

            if (definition.Fields.Count < outputFields.Length)
            {
                foreach (var outputField in outputFields)
                {
                    if (definition.Fields.ContainsKey(outputField))
                        continue;

                    result.Add(IndexField.Create(outputField, new IndexFieldOptions(), allFields));
                }
            }

            return result.ToArray();
        }

        protected override void PersistFields(TransactionOperationContext context, BlittableJsonTextWriter writer)
        {
            var builder = IndexDefinition.ToJson();
            using (var json = context.ReadObject(builder, nameof(IndexDefinition), BlittableJsonDocumentBuilder.UsageMode.ToDisk))
            {
                writer.WritePropertyName(nameof(IndexDefinition));
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

        public static IndexDefinition Load(StorageEnvironment environment)
        {
            using (var context = new JsonOperationContext())
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
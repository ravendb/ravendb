using System;
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexing;
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
            PersistMapFields(context, writer);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(Client.Indexing.IndexDefinition.Maps)));
            writer.WriteStartArray();
            var isFirst = true;
            foreach (var map in IndexDefinition.Maps)
            {
                if (isFirst == false)
                    writer.WriteComma();

                isFirst = false;
                writer.WriteString(context.GetLazyString(map));
            }
            writer.WriteEndArray();

            // TODO [ppekrol] persist more from _definition
        }

        protected override void FillIndexDefinition(IndexDefinition indexDefinition)
        {
            indexDefinition.Maps = IndexDefinition.Maps;

            // TODO [ppekrol] fill more from _definition
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
                    var fields = ReadMapFields(reader).ToDictionary(x => x.Name, x => x, StringComparer.OrdinalIgnoreCase);

                    var definition = new IndexDefinition();
                    definition.Name = ReadName(reader);
                    definition.Maps = ReadMaps(reader);
                    definition.Fields = fields.ToDictionary(
                        x => x.Key,
                        x => new IndexFieldOptions
                        {
                            Sort = x.Value.SortOption,
                            TermVector = x.Value.Highlighted ? FieldTermVector.WithPositionsAndOffsets : (FieldTermVector?)null,
                            Analyzer = x.Value.Analyzer,
                            Indexing = x.Value.Indexing,
                            Storage = x.Value.Storage
                        });

                    return new StaticMapIndexDefinition(definition, collections)
                    {
                        LockMode = lockMode
                    };
                }
            }
        }

        private static HashSet<string> ReadMaps(BlittableJsonReaderObject reader)
        {
            BlittableJsonReaderArray jsonArray;
            if (reader.TryGet(nameof(Client.Indexing.IndexDefinition.Maps), out jsonArray) == false || jsonArray.Length == 0)
                throw new InvalidOperationException("No persisted maps");

            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < jsonArray.Length; i++)
                result.Add(jsonArray.GetStringByIndex(i));

            return result;
        }
    }
}
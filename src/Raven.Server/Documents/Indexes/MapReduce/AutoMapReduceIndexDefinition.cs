using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexing;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class AutoMapReduceIndexDefinition : IndexDefinitionBase
    {
        public readonly Dictionary<string, IndexField> GroupByFields;

        public AutoMapReduceIndexDefinition(string[] collections, IndexField[] mapFields, IndexField[] groupByFields)
            : base(IndexNameFinder.FindMapReduceIndexName(collections, mapFields, groupByFields), collections, IndexLockMode.Unlock, mapFields)
        {
            foreach (var field in mapFields)
            {
                if (field.Storage != FieldStorage.Yes)
                    throw new ArgumentException($"Map-reduce field has to be stored. Field name: {field.Name}");
            }

            foreach (var field in groupByFields)
            {
                if (field.Storage != FieldStorage.Yes)
                    throw new ArgumentException($"GroupBy field has to be stored. Field name: {field.Name}");
            }

            GroupByFields = groupByFields.ToDictionary(x => x.Name, x => x, StringComparer.OrdinalIgnoreCase); ;
        }

        protected override void PersistFields(TransactionOperationContext context, BlittableJsonTextWriter writer)
        {
            PersistMapFields(context, writer);

            writer.WriteComma();

            PersistGroupByFields(context, writer);
        }

        protected void PersistGroupByFields(TransactionOperationContext context, BlittableJsonTextWriter writer)
        {
            writer.WritePropertyName(context.GetLazyString(nameof(GroupByFields)));
            writer.WriteStartArray();
            var first = true;
            foreach (var field in GroupByFields.Values)
            {
                if (first == false)
                    writer.WriteComma();

                writer.WriteStartObject();

                writer.WritePropertyName(context.GetLazyString(nameof(field.Name)));
                writer.WriteString(context.GetLazyString(field.Name));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(field.Highlighted)));
                writer.WriteBool(field.Highlighted);
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(field.SortOption)));
                writer.WriteInteger((int)(field.SortOption ?? SortOptions.None));

                writer.WriteEndObject();

                first = false;
            }
            writer.WriteEndArray();
        }

        protected override void FillIndexDefinition(IndexDefinition indexDefinition)
        {
        }

        public override bool Equals(IndexDefinitionBase indexDefinition, bool ignoreFormatting, bool ignoreMaxIndexOutputs)
        {
            return false;
        }

        public static AutoMapReduceIndexDefinition Load(StorageEnvironment environment)
        {
            using (var pool = new UnmanagedBuffersPool(nameof(AutoMapReduceIndexDefinition)))
            using (var context = new JsonOperationContext(pool))
            using (var tx = environment.ReadTransaction())
            {
                var tree = tx.CreateTree("Definition");
                var result = tree.Read(DefinitionSlice);
                if (result == null)
                    return null;

                using (var reader = context.ReadForDisk(result.Reader.AsStream(), string.Empty))
                {
                    int lockModeAsInt;
                    reader.TryGet(nameof(LockMode), out lockModeAsInt);

                    BlittableJsonReaderArray jsonArray;
                    reader.TryGet(nameof(Collections), out jsonArray);

                    var collection = jsonArray.GetStringByIndex(0);

                    reader.TryGet(nameof(MapFields), out jsonArray);

                    var mapFields = new IndexField[jsonArray.Length];

                    for (var i = 0; i < jsonArray.Length; i++)
                    {
                        var json = jsonArray.GetByIndex<BlittableJsonReaderObject>(i);

                        string name;
                        json.TryGet(nameof(IndexField.Name), out name);

                        bool highlighted;
                        json.TryGet(nameof(IndexField.Highlighted), out highlighted);

                        int sortOptionAsInt;
                        json.TryGet(nameof(IndexField.SortOption), out sortOptionAsInt);

                        int mapReduceOperationAsInt;
                        json.TryGet(nameof(IndexField.MapReduceOperation), out mapReduceOperationAsInt);

                        var field = new IndexField
                        {
                            Name = name,
                            Highlighted = highlighted,
                            Storage = FieldStorage.Yes,
                            SortOption = (SortOptions?)sortOptionAsInt,
                            Indexing = FieldIndexing.Default,
                            MapReduceOperation = (FieldMapReduceOperation)mapReduceOperationAsInt
                        };

                        mapFields[i] = field;
                    }

                    reader.TryGet(nameof(GroupByFields), out jsonArray);

                    var groupByFields = new IndexField[jsonArray.Length];

                    for (var i = 0; i < jsonArray.Length; i++)
                    {
                        var json = jsonArray.GetByIndex<BlittableJsonReaderObject>(i);

                        string name;
                        json.TryGet(nameof(IndexField.Name), out name);

                        bool highlighted;
                        json.TryGet(nameof(IndexField.Highlighted), out highlighted);

                        int sortOptionAsInt;
                        json.TryGet(nameof(IndexField.SortOption), out sortOptionAsInt);

                        var field = new IndexField
                        {
                            Name = name,
                            Highlighted = highlighted,
                            Storage = FieldStorage.Yes,
                            SortOption = (SortOptions?)sortOptionAsInt,
                            Indexing = FieldIndexing.Default
                        };

                        groupByFields[i] = field;
                    }

                    return new AutoMapReduceIndexDefinition(new []{ collection }, mapFields,  groupByFields)
                    {
                        LockMode = (IndexLockMode)lockModeAsInt
                    };
                }
            }
        }
    }
}
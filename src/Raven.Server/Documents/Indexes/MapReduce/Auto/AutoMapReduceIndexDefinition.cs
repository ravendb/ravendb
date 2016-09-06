using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexing;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Indexes.MapReduce.Auto
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

        public bool ContainsGroupByField(string field)
        {
            return GroupByFields.ContainsKey(field);
        }

        protected override void PersistFields(JsonOperationContext context, BlittableJsonTextWriter writer)
        {
            PersistMapFields(context, writer);

            writer.WriteComma();

            PersistGroupByFields(context, writer);
        }

        protected override IndexDefinition CreateIndexDefinition()
        {
            var map = $"{Collections.First()}:[{string.Join(";", MapFields.Select(x => $"<Name:{x.Value.Name},Sort:{x.Value.SortOption},Highlight:{x.Value.Highlighted}>"))}]";
            var reduce = $"{Collections.First()}:[{string.Join(";", GroupByFields.Select(x => $"<Name:{x.Value.Name},Sort:{x.Value.SortOption},Highlight:{x.Value.Highlighted},Operation:{x.Value.MapReduceOperation}>"))}]";

            var indexDefinition = new IndexDefinition();
            indexDefinition.Maps.Add(map);
            indexDefinition.Reduce = reduce;

            foreach (var kvp in ConvertFields(MapFields))
                indexDefinition.Fields[kvp.Key] = kvp.Value;

            foreach (var kvp in ConvertFields(GroupByFields))
                indexDefinition.Fields[kvp.Key] = kvp.Value;

            return indexDefinition;
        }

        protected void PersistGroupByFields(JsonOperationContext context, BlittableJsonTextWriter writer)
        {
            writer.WritePropertyName((nameof(GroupByFields)));
            writer.WriteStartArray();
            var first = true;
            foreach (var field in GroupByFields.Values)
            {
                if (first == false)
                    writer.WriteComma();

                writer.WriteStartObject();

                writer.WritePropertyName((nameof(field.Name)));
                writer.WriteString((field.Name));
                writer.WriteComma();

                writer.WritePropertyName((nameof(field.Highlighted)));
                writer.WriteBool(field.Highlighted);
                writer.WriteComma();

                writer.WritePropertyName((nameof(field.SortOption)));
                writer.WriteInteger((int)(field.SortOption ?? SortOptions.None));

                writer.WriteEndObject();

                first = false;
            }
            writer.WriteEndArray();
        }

        public override bool Equals(IndexDefinitionBase indexDefinition, bool ignoreFormatting, bool ignoreMaxIndexOutputs)
        {
            // TODO arek
            return false;
        }

        public override bool Equals(IndexDefinition indexDefinition, bool ignoreFormatting, bool ignoreMaxIndexOutputs)
        {
            return false;
        }

        public static AutoMapReduceIndexDefinition Load(StorageEnvironment environment)
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
                    return LoadFromJson(reader);
                }
            }
        }

        public static AutoMapReduceIndexDefinition LoadFromJson(BlittableJsonReaderObject reader)
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

            return new AutoMapReduceIndexDefinition(new[] { collection }, mapFields, groupByFields)
            {
                LockMode = (IndexLockMode)lockModeAsInt
            };
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Extensions;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Indexes.MapReduce.Auto
{
    public class AutoMapReduceIndexDefinition : IndexDefinitionBase
    {
        public readonly Dictionary<string, IndexField> GroupByFields;

        public AutoMapReduceIndexDefinition(string collection, IndexField[] mapFields, IndexField[] groupByFields)
            : base(IndexNameFinder.FindMapReduceIndexName(collection, mapFields, groupByFields), new HashSet<string> { collection }, IndexLockMode.Unlock, IndexPriority.Normal, mapFields)
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

        protected internal override IndexDefinition GetOrCreateIndexDefinitionInternal()
        {
            var map = $"{Collections.First()}:[{string.Join(";", MapFields.Select(x => $"<Name:{x.Value.Name},Sort:{x.Value.Sort},Operation:{x.Value.MapReduceOperation}>"))}]";
            var reduce = $"{Collections.First()}:[{string.Join(";", GroupByFields.Select(x => $"<Name:{x.Value.Name},Sort:{x.Value.Sort}>"))}]";

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

                writer.WritePropertyName(nameof(field.Name));
                writer.WriteString(field.Name);
                writer.WriteComma();

                writer.WritePropertyName((nameof(field.Sort)));
                writer.WriteInteger((int)(field.Sort ?? SortOptions.None));

                writer.WriteEndObject();

                first = false;
            }
            writer.WriteEndArray();
        }

        protected override int ComputeRestOfHash(int hashCode)
        {
            return (hashCode * 397) ^ GroupByFields.GetDictionaryHashCode();
        }

        public override IndexDefinitionCompareDifferences Compare(IndexDefinitionBase other)
        {
            var otherDefinition = other as AutoMapReduceIndexDefinition;
            if (otherDefinition == null)
                return IndexDefinitionCompareDifferences.All;

            if (ReferenceEquals(this, other))
                return IndexDefinitionCompareDifferences.None;

            var result = IndexDefinitionCompareDifferences.None;
            if (Collections.SequenceEqual(otherDefinition.Collections) == false || MapFields.SequenceEqual(otherDefinition.MapFields) == false)
                result |= IndexDefinitionCompareDifferences.Maps;

            if (GroupByFields.SequenceEqual(otherDefinition.GroupByFields) == false)
                result |= IndexDefinitionCompareDifferences.Reduce;

            if (LockMode != other.LockMode)
                result |= IndexDefinitionCompareDifferences.LockMode;

            if (Priority != other.Priority)
                result |= IndexDefinitionCompareDifferences.Priority;

            return result;
        }

        public override IndexDefinitionCompareDifferences Compare(IndexDefinition indexDefinition)
        {
            return IndexDefinitionCompareDifferences.All;
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
            var lockMode = ReadLockMode(reader);
            var priority = ReadPriority(reader);
            BlittableJsonReaderArray jsonArray;

            if(reader.TryGet(nameof(Collections), out jsonArray) == false)
                throw new InvalidOperationException("No persisted collections");

            var collection = jsonArray.GetStringByIndex(0);

            if (reader.TryGet(nameof(MapFields), out jsonArray) == false)
                throw new InvalidOperationException("No persisted map fields");

            var mapFields = new IndexField[jsonArray.Length];

            for (var i = 0; i < jsonArray.Length; i++)
            {
                var json = jsonArray.GetByIndex<BlittableJsonReaderObject>(i);

                string name;
                json.TryGet(nameof(IndexField.Name), out name);

                int sortOptionAsInt;
                json.TryGet(nameof(IndexField.Sort), out sortOptionAsInt);

                int mapReduceOperationAsInt;
                json.TryGet(nameof(IndexField.MapReduceOperation), out mapReduceOperationAsInt);

                var field = new IndexField
                {
                    Name = name,
                    Storage = FieldStorage.Yes,
                    Sort = (SortOptions?)sortOptionAsInt,
                    Indexing = FieldIndexing.Default,
                    MapReduceOperation = (FieldMapReduceOperation)mapReduceOperationAsInt
                };

                mapFields[i] = field;
            }

            if (reader.TryGet(nameof(GroupByFields), out jsonArray) == false)
                throw new InvalidOperationException("No persisted group by fields");

            var groupByFields = new IndexField[jsonArray.Length];

            for (var i = 0; i < jsonArray.Length; i++)
            {
                var json = jsonArray.GetByIndex<BlittableJsonReaderObject>(i);

                string name;
                json.TryGet(nameof(IndexField.Name), out name);

                int sortOptionAsInt;
                json.TryGet(nameof(IndexField.Sort), out sortOptionAsInt);

                var field = new IndexField
                {
                    Name = name,
                    Storage = FieldStorage.Yes,
                    Sort = (SortOptions?)sortOptionAsInt,
                    Indexing = FieldIndexing.Default
                };

                groupByFields[i] = field;
            }

            return new AutoMapReduceIndexDefinition(collection, mapFields, groupByFields)
            {
                LockMode = lockMode,
                Priority = priority
            };
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Server.Documents.Indexes.Auto;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Voron;

namespace Raven.Server.Documents.Indexes.MapReduce.Auto
{
    public class AutoMapReduceIndexDefinition : AutoIndexDefinitionBase
    {
        public readonly Dictionary<string, AutoIndexField> GroupByFields;

        public readonly Dictionary<string, AutoIndexField> MapAndGroupByFields;

        public readonly AutoIndexField[] OrderedGroupByFields;

        public AutoMapReduceIndexDefinition(string collection, AutoIndexField[] mapFields, AutoIndexField[] groupByFields, long? indexVersion = null)
            : base(AutoIndexNameFinder.FindMapReduceIndexName(collection, mapFields, groupByFields), collection, mapFields, indexVersion)
        {
            OrderedGroupByFields = groupByFields.OrderBy(x => x.Name, StringComparer.Ordinal).ToArray();

            GroupByFields = groupByFields.ToDictionary(x => x.Name, x => x, StringComparer.Ordinal);

            MapAndGroupByFields = new Dictionary<string, AutoIndexField>(MapFields.Count + GroupByFields.Count);

            foreach (var field in MapFields)
            {
                MapAndGroupByFields[field.Key] = field.Value.As<AutoIndexField>();
            }

            foreach (var field in GroupByFields)
            {
                MapAndGroupByFields[field.Key] = field.Value;

                foreach (var indexField in field.Value.ToIndexFields())
                {
                    IndexFields.Add(indexField.Name, indexField);
                }
            }
        }

        public override bool TryGetField(string field, out AutoIndexField value)
        {
            if (base.TryGetField(field, out value))
                return true;

            return GroupByFields.TryGetValue(field, out value);
        }

        protected override void PersistFields(JsonOperationContext context, AbstractBlittableJsonTextWriter writer)
        {
            PersistMapFields(context, writer);

            writer.WriteComma();

            PersistGroupByFields(context, writer);
        }

        protected internal override IndexDefinition GetOrCreateIndexDefinitionInternal()
        {
            var indexDefinition = new IndexDefinition
            {
                Name = Name,
                Type = IndexType.AutoMapReduce,
                LockMode = LockMode,
                Priority = Priority,
                State = State,
            };

            var map = $"{Collections.First()}:[{string.Join(";", MapFields.Select(x => x.Value.As<AutoIndexField>()).Select(x => $"<Name:{x.Name}#Operation:{x.Aggregation}>"))}]";
            var reduce = $"{Collections.First()}:[{string.Join(";", GroupByFields.Select(x => $"<Name:{x.Value.Name}>"))}]";
            indexDefinition.Maps.Add(map);
            indexDefinition.Reduce = reduce;

            foreach (var kvp in IndexFields)
                indexDefinition.Fields[kvp.Key] = kvp.Value.ToIndexFieldOptions();

            return indexDefinition;
        }

        protected void PersistGroupByFields(JsonOperationContext context, AbstractBlittableJsonTextWriter writer)
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

                writer.WritePropertyName(nameof(field.Indexing));
                writer.WriteString(field.Indexing.ToString());

                writer.WriteComma();

                writer.WritePropertyName(nameof(field.GroupByArrayBehavior));
                writer.WriteString(field.GroupByArrayBehavior.ToString());

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
            if (Collections.SetEquals(otherDefinition.Collections) == false || DictionaryExtensions.ContentEquals(MapFields, otherDefinition.MapFields) == false)
                result |= IndexDefinitionCompareDifferences.Maps;

            if (DictionaryExtensions.ContentEquals(GroupByFields, otherDefinition.GroupByFields) == false)
                result |= IndexDefinitionCompareDifferences.Reduce;

            if (LockMode != other.LockMode)
                result |= IndexDefinitionCompareDifferences.LockMode;

            if (Priority != other.Priority)
                result |= IndexDefinitionCompareDifferences.Priority;

            if (State != otherDefinition.State)
                result |= IndexDefinitionCompareDifferences.State;

            return result;
        }

        public override IndexDefinitionCompareDifferences Compare(IndexDefinition indexDefinition)
        {
            return GetOrCreateIndexDefinitionInternal().Compare(indexDefinition);
        }

        public static AutoMapReduceIndexDefinition Load(StorageEnvironment environment)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var tx = environment.ReadTransaction())
            {
                using (var stream = GetIndexDefinitionStream(environment, tx))
                {
                    if (stream == null)
                        return null;
                    using (var reader = context.Sync.ReadForDisk(stream, string.Empty))
                    {
                        return LoadFromJson(reader);
                    }
                }
            }
        }

        public static AutoMapReduceIndexDefinition LoadFromJson(BlittableJsonReaderObject reader)
        {
            var lockMode = ReadLockMode(reader);
            var priority = ReadPriority(reader);
            var state = ReadState(reader);
            var version = ReadVersion(reader);

            if (reader.TryGet(nameof(Collections), out BlittableJsonReaderArray jsonArray) == false)
                throw new InvalidOperationException("No persisted collections");

            var collection = jsonArray.GetStringByIndex(0);

            if (reader.TryGet(nameof(MapFields), out jsonArray) == false)
                throw new InvalidOperationException("No persisted map fields");

            var mapFields = new AutoIndexField[jsonArray.Length];

            for (var i = 0; i < jsonArray.Length; i++)
            {
                var json = jsonArray.GetByIndex<BlittableJsonReaderObject>(i);

                json.TryGet(nameof(AutoIndexField.Name), out string name);
                json.TryGet(nameof(AutoIndexField.Aggregation), out int aggregationAsInt);

                var field = new AutoIndexField
                {
                    Name = name,
                    Storage = FieldStorage.No,
                    Indexing = AutoFieldIndexing.Default,
                    Aggregation = (AggregationOperation)aggregationAsInt
                };

                mapFields[i] = field;
            }

            if (reader.TryGet(nameof(GroupByFields), out jsonArray) == false)
                throw new InvalidOperationException("No persisted group by fields");

            var groupByFields = new AutoIndexField[jsonArray.Length];

            for (var i = 0; i < jsonArray.Length; i++)
            {
                var json = jsonArray.GetByIndex<BlittableJsonReaderObject>(i);

                json.TryGet(nameof(IndexField.Name), out string name);
                json.TryGet(nameof(IndexField.Indexing), out string indexing);
                json.TryGet(nameof(AutoIndexField.GroupByArrayBehavior), out string groupByArray);

                var field = new AutoIndexField
                {
                    Name = name,
                    Storage = FieldStorage.No,
                    Indexing = (AutoFieldIndexing)Enum.Parse(typeof(AutoFieldIndexing), indexing),
                    GroupByArrayBehavior = (GroupByArrayBehavior)Enum.Parse(typeof(GroupByArrayBehavior), groupByArray),
                };

                groupByFields[i] = field;
            }

            return new AutoMapReduceIndexDefinition(collection, mapFields, groupByFields, version)
            {
                LockMode = lockMode,
                Priority = priority,
                State = state
            };
        }
    }
}

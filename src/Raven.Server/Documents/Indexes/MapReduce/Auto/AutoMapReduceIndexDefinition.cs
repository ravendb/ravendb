using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Server.Documents.Indexes.Auto; 
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Voron;

namespace Raven.Server.Documents.Indexes.MapReduce.Auto
{
    internal sealed class AutoMapReduceIndexDefinition : AutoIndexDefinitionBaseServerSide
    {
        public readonly Dictionary<string, AutoIndexField> GroupByFields;

        public readonly Dictionary<string, AutoIndexField> MapAndGroupByFields;

        public readonly AutoIndexField[] OrderedGroupByFields;

        public List<string> GroupByFieldNames { get; }

        internal AutoMapReduceIndexDefinition(string indexName, string collection, AutoIndexField[] mapFields, AutoIndexField[] groupByFields, List<string> groupByFieldNames, IndexDeploymentMode? deploymentMode, IndexDefinitionClusterState clusterState = null, long? indexVersion = null)
            : base(indexName, collection, mapFields, deploymentMode, clusterState, indexVersion)
        {
            OrderedGroupByFields = groupByFields.OrderBy(x => x.Name, StringComparer.Ordinal).ToArray();

            GroupByFields = groupByFields.ToDictionary(x => x.Name, x => x, StringComparer.Ordinal);

            GroupByFieldNames = groupByFieldNames;

            MapAndGroupByFields = new Dictionary<string, AutoIndexField>(MapFields.Count + GroupByFields.Count);

            var lastUsedId = new Reference<int>() { Value = MapFields.Count + GroupByFields.Count };
            
            foreach (var field in MapFields)
            {
                MapAndGroupByFields[field.Key] = field.Value.As<AutoIndexField>();
            }

            foreach (var field in GroupByFields)
            {
                MapAndGroupByFields[field.Key] = field.Value;

                foreach (var indexField in field.Value.ToIndexFields(lastUsedId))
                {
                    IndexFields.Add(indexField.Name, indexField);
                }
            }
        }

        // For Legacy test
        public AutoMapReduceIndexDefinition(string collection, AutoIndexField[] mapFields, AutoIndexField[] groupByFields, long? indexVersion = null)
            : this(AutoIndexNameFinder.FindMapReduceIndexName(collection, mapFields, groupByFields), collection, mapFields, groupByFields, groupByFieldNames: groupByFields.Select(x => x.Name).ToList(), deploymentMode: null, clusterState: null, indexVersion)
        {

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

        private void PersistGroupByFields(JsonOperationContext context, AbstractBlittableJsonTextWriter writer)
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

                writer.WriteComma();

                writer.WritePropertyName(nameof(field.HasQuotedName));
                writer.WriteBool(field.HasQuotedName);

                writer.WriteComma();
                
                writer.WriteEndObject();

                first = false;
            }
            writer.WriteEndArray();

            writer.WriteComma();

            writer.WritePropertyName((nameof(GroupByFieldNames)));
            writer.WriteStartArray();
            first = true;
            foreach (var field in GroupByFieldNames)
            {
                if (first == false)
                    writer.WriteComma();

                writer.WriteString(field);
                first = false;
            }
            writer.WriteEndArray();
        }

        protected override int ComputeRestOfHash(int hashCode)
        {
            return (hashCode * 397) ^ GroupByFields.GetDictionaryHashCode();
        }

        public override IndexDefinitionCompareDifferences Compare(IndexDefinitionBaseServerSide other)
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
            var indexName = ReadName(reader);

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
                json.TryGet(nameof(AutoIndexField.HasQuotedName), out bool hasQuotedName);
                
                var field = new AutoIndexField
                {
                    Name = name,
                    Storage = FieldStorage.No,
                    Indexing = AutoFieldIndexing.Default,
                    Aggregation = (AggregationOperation)aggregationAsInt,
                    HasQuotedName = hasQuotedName,
                };

                mapFields[i] = field;
            }

            int fieldId = 1;
            foreach (var field in mapFields.OrderBy(x => x.Name, StringComparer.Ordinal))
            {
                field.Id = fieldId++;
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
                json.TryGet(nameof(AutoIndexField.HasQuotedName), out bool hasQuotedName);
                
                var field = new AutoIndexField
                {
                    Name = name,
                    Storage = FieldStorage.No,
                    Indexing = (AutoFieldIndexing)Enum.Parse(typeof(AutoFieldIndexing), indexing),
                    GroupByArrayBehavior = (GroupByArrayBehavior)Enum.Parse(typeof(GroupByArrayBehavior), groupByArray),
                    HasQuotedName = hasQuotedName,
                };

                groupByFields[i] = field;
            }

            foreach (var field in groupByFields.OrderBy(x => x.Name, StringComparer.Ordinal))
            {
                field.Id = fieldId++;
            }

            List<string> groupByFieldNames;

            if (reader.TryGet(nameof(GroupByFieldNames), out jsonArray) == false)
            {
                // the fields don't exist for indexes that were imported from a dump prior to 6.0
                groupByFieldNames = groupByFields.Select(x => x.Name).ToList();
            }
            else
            {
                groupByFieldNames = new List<string>();

                foreach (var groupByField in jsonArray)
                {
                    groupByFieldNames.Add(groupByField.ToString());
                }
            }

            return new AutoMapReduceIndexDefinition(indexName, collection, mapFields, groupByFields, groupByFieldNames, deploymentMode: null, clusterState: null, version)
            {
                LockMode = lockMode,
                Priority = priority,
                State = state
            };
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Server.Extensions;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoMapIndexDefinition : AutoIndexDefinitionBase
    {
        public AutoMapIndexDefinition(string collection, AutoIndexField[] fields, long? indexVersion = null)
            : base(AutoIndexNameFinder.FindMapIndexName(collection, fields), collection, fields, indexVersion)
        {
        }

        protected override void PersistFields(JsonOperationContext context, BlittableJsonTextWriter writer)
        {
            PersistMapFields(context, writer);
        }

        protected internal override IndexDefinition GetOrCreateIndexDefinitionInternal()
        {
            var indexDefinition = new IndexDefinition
            {
                Name = Name,
                Type = IndexType.AutoMap,
                LockMode = LockMode,
                Priority = Priority,
                State = State,
            };

            void AddFields(IEnumerable<string> mapFields, IEnumerable<(string Name, IndexFieldOptions Options)> indexFields)
            {
                var map = $"{Collections.First()}:[{string.Join(";", mapFields.Select(x => $"<Name:{x}>"))}]";
                indexDefinition.Maps.Add(map);

                foreach (var field in indexFields)
                    indexDefinition.Fields[field.Name] = field.Options;
            }

            if (MapFields.Count > 0)
                AddFields(MapFields.Select(x => x.Value.Name), IndexFields.Select(x => (x.Key, x.Value.ToIndexFieldOptions())));
            else
            {
                // auto index was created to handle queries like startsWith(id(), 'users/')

                AddFields(new[] { Constants.Documents.Indexing.Fields.DocumentIdFieldName }, new[]
                {
                    (Constants.Documents.Indexing.Fields.DocumentIdFieldName, new IndexFieldOptions())
                });
            }

            return indexDefinition;
        }

        public override IndexDefinitionCompareDifferences Compare(IndexDefinitionBase other)
        {
            var otherDefinition = other as AutoMapIndexDefinition;
            if (otherDefinition == null)
                return IndexDefinitionCompareDifferences.All;

            if (ReferenceEquals(this, other))
                return IndexDefinitionCompareDifferences.None;

            var result = IndexDefinitionCompareDifferences.None;
            if (Collections.SetEquals(otherDefinition.Collections) == false || DictionaryExtensions.ContentEquals(MapFields, otherDefinition.MapFields) == false)
                result |= IndexDefinitionCompareDifferences.Maps;

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
            return IndexDefinitionCompareDifferences.All;
        }

        protected override int ComputeRestOfHash(int hashCode)
        {
            return hashCode;// nothing else here
        }

        public static AutoMapIndexDefinition Load(StorageEnvironment environment)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var tx = environment.ReadTransaction())
            {
                using (var stream = GetIndexDefinitionStream(environment, tx))
                {
                    if (stream == null)
                        return null;

                    using (var reader = context.ReadForDisk(stream, string.Empty))
                    {
                        return LoadFromJson(reader);
                    }
                }
            }
        }

        public static AutoMapIndexDefinition LoadFromJson(BlittableJsonReaderObject reader)
        {
            var lockMode = ReadLockMode(reader);
            var priority = ReadPriority(reader);
            var version = ReadVersion(reader);
            var collections = ReadCollections(reader);

            if (reader.TryGet(nameof(MapFields), out BlittableJsonReaderArray jsonArray) == false)
                throw new InvalidOperationException("No persisted lock mode");

            var fields = new AutoIndexField[jsonArray.Length];

            for (var i = 0; i < jsonArray.Length; i++)
            {
                var json = jsonArray.GetByIndex<BlittableJsonReaderObject>(i);

                json.TryGet(nameof(AutoIndexField.Name), out string name);
                json.TryGet(nameof(AutoIndexField.Indexing), out string indexing);
                json.TryGet(nameof(AutoIndexField.HasSuggestions), out bool hasSuggestions);

                var field = new AutoIndexField
                {
                    Name = name,
                    Storage = FieldStorage.No,
                    Indexing = (AutoFieldIndexing)Enum.Parse(typeof(AutoFieldIndexing), indexing),
                    HasSuggestions = hasSuggestions
                };

                fields[i] = field;
            }

            return new AutoMapIndexDefinition(collections[0], fields, version)
            {
                LockMode = lockMode,
                Priority = priority
            };
        }
    }
}

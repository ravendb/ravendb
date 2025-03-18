using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Voron;

namespace Raven.Server.Documents.Indexes.Auto
{
    internal sealed class AutoMapIndexDefinition : AutoIndexDefinitionBaseServerSide
    {
        internal AutoMapIndexDefinition(string indexName, string collection, AutoIndexField[] fields, IndexDeploymentMode? deploymentMode,
            IndexDefinitionClusterState clusterState, long? indexVersion = null)
            : base(indexName, collection, fields, deploymentMode, clusterState, indexVersion)
        {
        }

        // For legacy tests
        public AutoMapIndexDefinition(string collection, AutoIndexField[] fields, long? indexVersion = null)
            : this(AutoIndexNameFinder.FindMapIndexName(collection, fields), collection, fields, deploymentMode: null, clusterState: null, indexVersion: indexVersion)
        {
        }

        protected override void PersistFields(JsonOperationContext context, AbstractBlittableJsonTextWriter writer)
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
                
                foreach (var field in indexFields.OrderBy(x => x.Name, StringComparer.Ordinal))
                {
                    indexDefinition.Fields[field.Name] = field.Options;
                }
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

        public override IndexDefinitionCompareDifferences Compare(IndexDefinitionBaseServerSide other)
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

                    using (var reader = context.Sync.ReadForDisk(stream, string.Empty))
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
            var indexName = ReadName(reader);

            if (reader.TryGet(nameof(MapFields), out BlittableJsonReaderArray jsonArray) == false)
                throw new InvalidOperationException("No persisted lock mode");

            var fields = new AutoIndexField[jsonArray.Length];
            for (var i = 0; i < jsonArray.Length; i++)
            {
                var json = jsonArray.GetByIndex<BlittableJsonReaderObject>(i);

                json.TryGet(nameof(AutoIndexField.Name), out string name);
                json.TryGet(nameof(AutoIndexField.Indexing), out string indexing);
                json.TryGet(nameof(AutoIndexField.HasSuggestions), out bool hasSuggestions);
                json.TryGet(nameof(AutoIndexField.HasQuotedName), out bool hasQuotedName);
                json.TryGet(nameof(AutoIndexField.Spatial), out BlittableJsonReaderObject spatialBlittable);
                json.TryGet(nameof(AutoIndexField.Vector), out BlittableJsonReaderObject vectorBlittable);

                AutoSpatialOptions spatial = null;
                
                if (spatialBlittable != null)
                    spatial = JsonDeserializationServer.AutoSpatialOptions(spatialBlittable);

                AutoVectorOptions vector = null;
                if (vectorBlittable != null)
                    vector = JsonDeserializationServer.AutoVectorOptions(vectorBlittable);
                
                var field = new AutoIndexField
                {
                    Name = name,
                    Storage = FieldStorage.No,
                    Indexing = (AutoFieldIndexing)Enum.Parse(typeof(AutoFieldIndexing), indexing),
                    HasSuggestions = hasSuggestions,
                    HasQuotedName = hasQuotedName,
                    Spatial = spatial,
                    Vector = vector
                };

                fields[i] = field;
            }

            int idX = 1;
            foreach (var field in fields.OrderBy(x => x.Name, StringComparer.Ordinal))
            {
                field.Id = idX++;
            }
            
            return new AutoMapIndexDefinition(indexName, collections[0], fields, deploymentMode: null, indexVersion: version, clusterState: null)
            {
                LockMode = lockMode,
                Priority = priority
            };
        }
    }
}

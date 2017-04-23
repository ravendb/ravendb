using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Client.Util.Encryption;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;

using Voron;
using Sparrow;

namespace Raven.Server.Documents.Indexes
{
    public abstract class IndexDefinitionBase
    {
        protected const string MetadataFileName = "metadata";

        protected static readonly Slice DefinitionSlice;

        private int? _cachedHashCode;

        protected IndexDefinitionBase(string name, HashSet<string> collections, IndexLockMode lockMode, IndexPriority priority, IndexField[] mapFields)
        {
            Name = name;
            Collections = collections;
            MapFields = mapFields.ToDictionary(x => x.Name, x => x, StringComparer.Ordinal);
            LockMode = lockMode;
            Priority = priority;
        }

        static IndexDefinitionBase()
        {
            Slice.From(StorageEnvironment.LabelsContext, "Definition", ByteStringType.Immutable, out DefinitionSlice);
        }

        public string Name { get; private set; }

        public HashSet<string> Collections { get; }

        public Dictionary<string, IndexField> MapFields { get; }

        public IndexLockMode LockMode { get; set; }

        public IndexPriority Priority { get; set; }

        public virtual bool HasDynamicFields => false;

        public void Persist(TransactionOperationContext context, StorageEnvironmentOptions options)
        {
            var tree = context.Transaction.InnerTransaction.CreateTree("Definition");
            using (var stream = new MemoryStream())
            using (var writer = new BlittableJsonTextWriter(context, stream))
            {
                Persist(context, writer);

                writer.Flush();

                stream.Position = 0;

                if (options is StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)
                {
                    using (var metadata = File.Open(Path.Combine(options.BasePath, MetadataFileName), FileMode.Create))
                    using (var metadataWriter = new StreamWriter(metadata, Encoding.UTF8))
                    {
                        metadataWriter.WriteLine(Name);
                        metadataWriter.Flush();

                        stream.CopyTo(metadata);
                        stream.Position = 0;
                    }
                }


                Slice val;
                using (Slice.From(context.Allocator, stream.ToArray(), out val))
                {
                    tree.Add(DefinitionSlice, val);
                }
            }
        }

        public void Persist(JsonOperationContext context, BlittableJsonTextWriter writer)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(Name));
            writer.WriteString(Name);
            writer.WriteComma();

            writer.WritePropertyName(nameof(Collections));
            writer.WriteStartArray();
            var isFirst = true;
            foreach (var collection in Collections)
            {
                if (isFirst == false)
                    writer.WriteComma();

                isFirst = false;
                writer.WriteString((collection));
            }

            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName(nameof(LockMode));
            writer.WriteInteger((int)LockMode);
            writer.WriteComma();

            writer.WritePropertyName(nameof(Priority));
            writer.WriteInteger((int)Priority);
            writer.WriteComma();

            PersistFields(context, writer);

            writer.WriteEndObject();
        }

        protected abstract void PersistFields(JsonOperationContext context, BlittableJsonTextWriter writer);

        protected void PersistMapFields(JsonOperationContext context, BlittableJsonTextWriter writer)
        {
            writer.WritePropertyName((nameof(MapFields)));
            writer.WriteStartArray();
            var first = true;
            foreach (var field in MapFields.Values)
            {
                if (first == false)
                    writer.WriteComma();

                writer.WriteStartObject();

                writer.WritePropertyName((nameof(field.Name)));
                writer.WriteString((field.Name));
                writer.WriteComma();

                writer.WritePropertyName((nameof(field.Sort)));
                writer.WriteInteger((int)(field.Sort ?? SortOptions.None));
                writer.WriteComma();

                writer.WritePropertyName((nameof(field.MapReduceOperation)));
                writer.WriteInteger((int)(field.MapReduceOperation));

                writer.WriteEndObject();

                first = false;
            }
            writer.WriteEndArray();
        }

        public void Rename(string name, TransactionOperationContext context, StorageEnvironmentOptions options)
        {
            Name = name;

            Persist(context, options);
        }

        public IndexDefinition ConvertToIndexDefinition(Index index)
        {
            var indexDefinition = GetOrCreateIndexDefinitionInternal() ?? new IndexDefinition();
            indexDefinition.Name = index.Name;
            indexDefinition.Etag = index.Etag;
            indexDefinition.Type = index.Type;
            indexDefinition.LockMode = LockMode;
            indexDefinition.Priority = Priority;

            return indexDefinition;
        }

        protected internal abstract IndexDefinition GetOrCreateIndexDefinitionInternal();

        public bool ContainsField(string field)
        {
            field = FieldUtil.RemoveRangeSuffixIfNecessary(field);

            return MapFields.ContainsKey(field);
        }

        public IndexField GetField(string field)
        {
            field = FieldUtil.RemoveRangeSuffixIfNecessary(field);

            return MapFields[field];
        }

        public bool TryGetField(string field, out IndexField value)
        {
            field = FieldUtil.RemoveRangeSuffixIfNecessary(field);

            return MapFields.TryGetValue(field, out value);
        }

        public abstract IndexDefinitionCompareDifferences Compare(IndexDefinitionBase indexDefinition);

        public abstract IndexDefinitionCompareDifferences Compare(IndexDefinition indexDefinition);

        public override int GetHashCode()
        {
            if (_cachedHashCode != null)
                return _cachedHashCode.Value;

            unchecked
            {
                var hashCode = MapFields?.GetDictionaryHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ (Name?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (Collections?.GetEnumerableHashCode() ?? 0);

                hashCode = ComputeRestOfHash(hashCode);

                _cachedHashCode = hashCode;

                return hashCode;
            }
        }

        protected abstract int ComputeRestOfHash(int hashCode);

        public static string TryReadNameFromMetadataFile(string directory)
        {
            var metadataFile = Path.Combine(directory, MetadataFileName);
            if (File.Exists(metadataFile) == false)
                return null;

            var name = File.ReadLines(metadataFile, Encoding.UTF8).FirstOrDefault();
            if (string.IsNullOrWhiteSpace(name))
                return null;

            return name;
        }

        public static bool TryReadIdFromDirectory(DirectoryInfo directory, out int indexId, out string indexName)
        {
            var index = directory.Name.IndexOf('-');
            var maybeId = index >= 0
                ? directory.Name.Substring(0, index)
                : directory.Name;

            if (int.TryParse(maybeId, out indexId) == false)
            {
                indexId = -1;
                indexName = null;
                return false;
            }

            indexName = directory.Name.Substring(index + 1);
            return true;
        }

        public static string GetIndexNameSafeForFileSystem(string name)
        {
            foreach (var invalidPathChar in Path.GetInvalidFileNameChars())
            {
                name = name.Replace(invalidPathChar, '_');
            }

            var hash = MD5Core.GetHashString(name.ToLowerInvariant()).Substring(0, 8);
            if (name.Length < 64)
                return $"{hash}-{name}";
            return $"{hash}-{name.Substring(0, 64)}";
        }

        protected static string ReadName(BlittableJsonReaderObject reader)
        {
            string name;
            if (reader.TryGet(nameof(Name), out name) == false || String.IsNullOrWhiteSpace(name))
                throw new InvalidOperationException("No persisted name");

            return name;
        }

        protected static string[] ReadCollections(BlittableJsonReaderObject reader)
        {
            BlittableJsonReaderArray jsonArray;
            if (reader.TryGet(nameof(Collections), out jsonArray) == false || jsonArray.Length == 0)
                throw new InvalidOperationException("No persisted collections");

            var result = new string[jsonArray.Length];
            for (var i = 0; i < jsonArray.Length; i++)
                result[i] = jsonArray.GetStringByIndex(i);

            return result;
        }

        protected static IndexLockMode ReadLockMode(BlittableJsonReaderObject reader)
        {
            int lockModeAsInt;
            if (reader.TryGet(nameof(LockMode), out lockModeAsInt) == false)
                throw new InvalidOperationException("No persisted lock mode");

            return (IndexLockMode)lockModeAsInt;
        }

        protected static IndexPriority ReadPriority(BlittableJsonReaderObject reader)
        {
            int priorityAsInt;
            if (reader.TryGet(nameof(Priority), out priorityAsInt) == false)
                throw new InvalidOperationException("No persisted priority");

            return (IndexPriority)priorityAsInt;
        }

        protected static IndexField[] ReadMapFields(BlittableJsonReaderObject reader)
        {
            BlittableJsonReaderArray jsonArray;
            if (reader.TryGet(nameof(MapFields), out jsonArray) == false)
                throw new InvalidOperationException("No persisted lock mode");

            var fields = new IndexField[jsonArray.Length];
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
                    Storage = FieldStorage.No,
                    Sort = (SortOptions?)sortOptionAsInt,
                    Indexing = FieldIndexing.Default
                };

                fields[i] = field;
            }

            return fields;
        }

        protected Dictionary<string, IndexFieldOptions> ConvertFields(Dictionary<string, IndexField> fields)
        {
            return fields.ToDictionary(
                x => x.Key,
                x => x.Value.ToIndexFieldOptions());
        }
    }
}
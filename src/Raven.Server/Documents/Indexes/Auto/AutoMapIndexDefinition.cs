using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoMapIndexDefinition : AutoIndexDefinitionBase
    {
        public AutoMapIndexDefinition(string collection, AutoIndexField[] fields)
            : base(AutoIndexNameFinder.FindMapIndexName(collection, fields), collection, fields)
        {
            if (fields.Length == 0)
                throw new ArgumentException("You must specify at least one field.", nameof(fields));
        }

        protected override void PersistFields(JsonOperationContext context, BlittableJsonTextWriter writer)
        {
            PersistMapFields(context, writer);
        }

        protected internal override IndexDefinition GetOrCreateIndexDefinitionInternal()
        {
            var map = $"{Collections.First()}:[{string.Join(";", MapFields.Select(x => $"<Name:{x.Value.Name}>"))}]";

            var indexDefinition = new IndexDefinition();
            indexDefinition.Maps.Add(map);

            foreach (var kvp in IndexFields)
                indexDefinition.Fields[kvp.Key] = kvp.Value.ToIndexFieldOptions();

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
            if (Collections.SequenceEqual(otherDefinition.Collections) == false || MapFields.SequenceEqual(otherDefinition.MapFields) == false)
                result |= IndexDefinitionCompareDifferences.Maps;

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

        protected override int ComputeRestOfHash(int hashCode)
        {
            return hashCode;// nothing else here
        }

        public static AutoMapIndexDefinition Load(StorageEnvironment environment)
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

        public static AutoMapIndexDefinition LoadFromJson(BlittableJsonReaderObject reader)
        {
            var lockMode = ReadLockMode(reader);
            var priority = ReadPriority(reader);
            var collections = ReadCollections(reader);

            if (reader.TryGet(nameof(MapFields), out BlittableJsonReaderArray jsonArray) == false)
                throw new InvalidOperationException("No persisted lock mode");

            var fields = new AutoIndexField[jsonArray.Length];

            for (var i = 0; i < jsonArray.Length; i++)
            {
                var json = jsonArray.GetByIndex<BlittableJsonReaderObject>(i);

                json.TryGet(nameof(IndexField.Name), out string name);
                json.TryGet(nameof(AutoIndexField.Indexing), out string indexing);

                var field = new AutoIndexField
                {
                    Name = name,
                    Storage = FieldStorage.No,
                    Indexing = (AutoFieldIndexing)Enum.Parse(typeof(AutoFieldIndexing), indexing)
                };

                fields[i] = field;
            }

            return new AutoMapIndexDefinition(collections[0], fields)
            {
                LockMode = lockMode,
                Priority = priority
            };
        }
    }
}

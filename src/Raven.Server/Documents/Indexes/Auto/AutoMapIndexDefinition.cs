using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexing;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoMapIndexDefinition : IndexDefinitionBase
    {
        public AutoMapIndexDefinition(string collection, IndexField[] fields)
            : base(IndexNameFinder.FindMapIndexName(new[] { collection }, fields), new[] { collection }, IndexLockMode.Unlock, fields)
        {
            if (string.IsNullOrEmpty(collection))
                throw new ArgumentNullException(nameof(collection));

            if (fields.Length == 0)
                throw new ArgumentException("You must specify at least one field.", nameof(fields));
        }

        protected override void PersistFields(JsonOperationContext context, BlittableJsonTextWriter writer)
        {
            PersistMapFields(context, writer);
        }

        protected override IndexDefinition CreateIndexDefinition()
        {
            var map = $"{Collections.First()}:[{string.Join(";", MapFields.Select(x => $"<Name:{x.Value.Name},Sort:{x.Value.SortOption},Highlight:{x.Value.Highlighted}>"))}]";

            var indexDefinition = new IndexDefinition();
            indexDefinition.Maps.Add(map);
            indexDefinition.Fields = ConvertFields(MapFields);

            return indexDefinition;
        }

        public override bool Equals(IndexDefinitionBase other, bool ignoreFormatting, bool ignoreMaxIndexOutputs)
        {
            var otherDefinition = other as AutoMapIndexDefinition;
            if (otherDefinition == null)
                return false;

            if (ReferenceEquals(this, other))
                return true;

            if (string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase) == false)
                return false;

            if (Collections.SequenceEqual(otherDefinition.Collections) == false)
                return false;

            if (MapFields.SequenceEqual(otherDefinition.MapFields) == false)
                return false;

            return true;
        }

        public override bool Equals(IndexDefinition indexDefinition, bool ignoreFormatting, bool ignoreMaxIndexOutputs)
        {
            return false;
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
            var collections = ReadCollections(reader);
            var fields = ReadMapFields(reader);

            return new AutoMapIndexDefinition(collections[0], fields)
            {
                LockMode = lockMode
            };
        }
    }
}
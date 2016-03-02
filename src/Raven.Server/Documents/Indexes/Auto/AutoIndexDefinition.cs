using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes.Auto
{
    public class AutoIndexDefinition : IndexDefinitionBase
    {
        private readonly IndexField[] _fields;
        private readonly Dictionary<string, IndexField> _fieldsByName;

        public AutoIndexDefinition(string collection, IndexField[] fields)
            : base(FindIndexName(collection, fields), new[] { collection })
        {
            if (string.IsNullOrEmpty(collection))
                throw new ArgumentNullException(nameof(collection));

            if (fields == null)
                throw new ArgumentNullException(nameof(fields));

            if (fields.Length == 0)
                throw new ArgumentException("You must specify at least one field.", nameof(fields));

            _fields = fields;

            _fieldsByName = _fields.ToDictionary(x => x.Name, x => x);
        }

        public int CountOfMapFields => _fields.Length;

        public override IndexField[] MapFields => _fields; // TODO arek

        public bool ContainsField(string field)
        {
            if (field.EndsWith("_Range"))
                field = field.Substring(0, field.Length - 6);

            return _fieldsByName.ContainsKey(field);
        }

        public IndexField GetField(string field)
        {
            if (field.EndsWith("_Range"))
                field = field.Substring(0, field.Length - 6);

            return _fieldsByName[field];
        }

        private static string FindIndexName(string collection, IReadOnlyCollection<IndexField> fields)
        {
            var combinedFields = string.Join("And", fields.Select(x => IndexField.ReplaceInvalidCharactersInFieldName(x.Name)).OrderBy(x => x));

            var sortOptions = fields.Where(x => x.SortOption != null).Select(x => x.Name).ToArray();
            if (sortOptions.Length > 0)
            {
                combinedFields = $"{combinedFields}SortBy{string.Join(string.Empty, sortOptions.OrderBy(x => x))}";
            }

            var highlighted = fields.Where(x => x.Highlighted).Select(x => x.Name).ToArray();
            if (highlighted.Length > 0)
            {
                combinedFields = $"{combinedFields}Highlight{string.Join("", highlighted.OrderBy(x => x))}";
            }

            return fields.Count == 0 ? $"Auto/{collection}" : $"Auto/{collection}/By{combinedFields}";
        }

        public override void Persist(TransactionOperationContext context)
        {
            var tree = context.Transaction.InnerTransaction.CreateTree("Definition");
            using (var stream = new MemoryStream())
            using (var writer = new BlittableJsonTextWriter(context, stream))
            {
                writer.WriteStartObject();

                var collection = Collections.First();

                writer.WritePropertyName(context.GetLazyString(nameof(Collections)));
                writer.WriteStartArray();
                writer.WriteString(context.GetLazyString(collection));
                writer.WriteEndArray();
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(MapFields)));
                writer.WriteStartArray();
                var first = true;
                foreach (var field in _fields)
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

                writer.WriteEndObject();

                writer.Flush();

                stream.Position = 0;
                tree.Add(DefinitionSlice, stream.ToArray());
            }
        }

        public static AutoIndexDefinition Load(StorageEnvironment environment)
        {
            using (var pool = new UnmanagedBuffersPool(nameof(AutoIndexDefinition)))
            using (var context = new MemoryOperationContext(pool))
            using (var tx = environment.ReadTransaction())
            {
                var tree = tx.CreateTree("Definition");
                var result = tree.Read(DefinitionSlice);
                if (result == null)
                    return null;

                using (var reader = context.ReadForDisk(result.Reader.AsStream(), string.Empty))
                {
                    BlittableJsonReaderArray jsonArray;
                    reader.TryGet(nameof(Collections), out jsonArray);

                    var collection = jsonArray.GetStringByIndex(0);

                    reader.TryGet(nameof(MapFields), out jsonArray);

                    var fields = new IndexField[jsonArray.Length];
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
                            Highlighted = false,
                            Storage = FieldStorage.No
                        };

                        fields[i] = field;
                    }

                    return new AutoIndexDefinition(collection, fields);
                }
            }
        }
    }
}
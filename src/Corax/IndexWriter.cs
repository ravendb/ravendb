using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Impl;

namespace Corax
{
    public class IndexWriter : IDisposable // single threaded, controlled by caller
    {
        private readonly StorageEnvironment _environment;
        private readonly TransactionPersistentContext _transactionPersistentContext;

        private readonly Transaction _transaction;

        public static readonly Slice IndexEntriesSlice;
        private static readonly Slice IndexEntriesByNameSlice;
        private static readonly Slice TagsByNameSlice;

        private static readonly Slice NullSlice;

        public static readonly TableSchema IndexEntriesSchema;

        private readonly Table _entries;


        public enum IndexEntriesTable
        {
            DocumentId = 0,
            Entry = 1,
        }

        static IndexWriter()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "IndexEntries", ByteStringType.Immutable, out IndexEntriesSlice);
                Slice.From(ctx, "IndexEntriesByName", ByteStringType.Immutable, out IndexEntriesByNameSlice);
                Slice.From(ctx, "Tags", ByteStringType.Immutable, out TagsByNameSlice);
                Slice.From(ctx, "<<RDB__NULL>>", ByteStringType.Immutable, out NullSlice);

                IndexEntriesSchema = new()
                {
                    TableType = (byte)IndexingTableType.IndexEntries,
                    Compressed = true,
                    CompressedEtagSourceIndex = new TableSchema.FixedSizeSchemaIndexDef
                    {
                        Name = TagsByNameSlice,
                        IsGlobal = true,
                        StartIndex = 1,
                    }
                };

                IndexEntriesSchema.DefineIndex(new TableSchema.SchemaIndexDef
                {
                    StartIndex = (int)IndexEntriesTable.DocumentId,
                    Count = 1,
                    Name = IndexEntriesByNameSlice,
                    Type = TableIndexType.BTree,
                    IsGlobal = false,
                });
            }
        }

        // The reason why we want to have the transaction open for us is so that we avoid having
        // to explicitly provide the index writer with opening semantics and also every new
        // writer becomes essentially a unit of work which makes reusing assets tracking more explicit.
        public IndexWriter([NotNull] StorageEnvironment environment)
        {
            _environment = environment;
            _transactionPersistentContext = new TransactionPersistentContext(true);

            _transaction = _environment.WriteTransaction(_transactionPersistentContext);
            _entries = _transaction.OpenTable(IndexEntriesSchema, IndexEntriesSlice);
            if (_entries != null) return;
            IndexEntriesSchema.Create(_transaction, IndexEntriesSlice, 8);

            _entries = _transaction.OpenTable(IndexEntriesSchema, IndexEntriesSlice);
            _entries.CORAX_DEBUG_MOVE_DATA += () =>
                throw new NotSupportedException("Data move are not supported for index entries");
        }

        //public long Index(string id, BlittableJsonReaderObject item)
        //{
        //    using var _ = Slice.From(_transaction.Allocator, id, out var idSlice);
        //    return Index(idSlice, item);
        //}

        private readonly SortedList<Slice, SortedList<Slice, SortedList<long, long>>> _buffer =
            new SortedList<Slice, SortedList<Slice, SortedList<long, long>>>(SliceComparer.Instance);


        public long Index(string id, Span<byte> data, Dictionary<Slice, int> knownFields)
        {
            using var _ = Slice.From(_transaction.Allocator, id, out var idSlice);
            return Index(idSlice, data, knownFields);
        }

        public long Index(Slice id, Span<byte> data, Dictionary<Slice, int> knownFields)
        {
            using (_entries.Allocate(out var builder))
            {
                builder.Add(id);
                builder.Add(data);
                long entryId = _entries.Insert(builder);

                var context = _transaction.Allocator;
                var entryReader = new IndexEntryReader(data);
                entryReader.DebugDump(knownFields);

                foreach (var (key, tokenField) in knownFields)
                {
                    if (_buffer.TryGetValue(key, out var field) == false)
                    {
                        _buffer[key] = field = new SortedList<Slice, SortedList<long, long>>(SliceComparer.Instance);
                    }

                    InsertToken(context, ref entryReader, tokenField, field, entryId );
                }

                //BlittableJsonReaderObject.PropertyDetails prop = default;
                //for (int i = 0; i < item.Count; i++)
                //{
                //    item.GetPropertyByIndex(i, ref prop, addObjectToCache: false);

                //    var key = prop.Name.ToString();
                //    if (_buffer.TryGetValue(key, out var field) == false)
                //    {
                //        _buffer[key] = field = new SortedList<string, SortedList<long, long>>();
                //    }
                //    InsertToken(field, prop.Value, prop.Token, entryId);
                //}

                return entryId;
            }

        }

        private void InsertToken(ByteStringContext context, ref IndexEntryReader entryReader, int tokenField, SortedList<Slice, SortedList<long, long>> field, long entryId)
        {
            var fieldType = entryReader.GetFieldType(tokenField);
            if (fieldType.HasFlag(IndexEntryFieldType.List) && fieldType.HasFlag(IndexEntryFieldType.Tuple))
            {
                var iterator = entryReader.ReadMany(tokenField);
                while (iterator.ReadNext())
                {
                    var value = iterator.Sequence;

                    using var _ = Slice.From(context, value, ByteStringType.Mutable, out var slice);
                    if (field.TryGetValue(slice, out var term) == false)
                    {
                        var fieldName = slice.Clone(context);
                        field[fieldName] = term = new SortedList<long, long>();
                    }
                        
                    term[entryId] = entryId;
                }
            }
            else if (fieldType.HasFlag(IndexEntryFieldType.List))
            {
                var iterator = entryReader.ReadMany(tokenField);
                while (iterator.ReadNext())
                {
                    var value = iterator.Sequence;

                    using var _ = Slice.From(context, value, ByteStringType.Mutable, out var slice);
                    if (field.TryGetValue(slice, out var term) == false)
                    {
                        var fieldName = slice.Clone(context);
                        field[fieldName] = term = new SortedList<long, long>();
                    }

                    term[entryId] = entryId;
                }
            }
            else if (fieldType.HasFlag(IndexEntryFieldType.Tuple))
            {
                entryReader.Read(tokenField, out var value);

                using var _ = Slice.From(context, value, ByteStringType.Mutable, out var slice);
                if (field.TryGetValue(slice, out var term) == false)
                {
                    var fieldName = slice.Clone(context);
                    field[fieldName] = term = new SortedList<long, long>();
                }
                term[entryId] = entryId;
            }
            else if (fieldType.HasFlag(IndexEntryFieldType.Invalid))
            {
                if (field.TryGetValue(NullSlice, out var term) == false)
                    field[NullSlice] = term = new SortedList<long, long>();
                term[entryId] = entryId;
            }
            else
            {
                entryReader.Read(tokenField, out var value);

                using var _ = Slice.From(context, value, ByteStringType.Mutable, out var slice);
                if (field.TryGetValue(slice, out var term) == false)
                {
                    var fieldName = slice.Clone(context);
                    field[fieldName] = term = new SortedList<long, long>();
                }
                term[entryId] = entryId;
            }
        }

        //public unsafe long Index(Slice id, BlittableJsonReaderObject item)
        //{
        //    using (_entries.Allocate(out var builder))
        //    {
        //        builder.Add(id);
        //        builder.Add(item.BasePointer, item.Size);
        //        long entryId = _entries.Insert(builder);
        //        BlittableJsonReaderObject.PropertyDetails prop = default;
        //        for (int i = 0; i < item.Count; i++)
        //        {
        //            item.GetPropertyByIndex(i, ref prop, addObjectToCache: false);
        //            var key = prop.Name.ToString();
        //            if (_buffer.TryGetValue(key, out var field) == false)
        //            {
        //                _buffer[key] = field = new SortedList<string, SortedList<long, long>>();
        //            }
        //            InsertToken(field, prop.Value, prop.Token, entryId);
        //        }
        //        return entryId;
        //    }
        //}

        private static void InsertToken(SortedList<string, SortedList<long, long>> field, object val, BlittableJsonToken token, long entryId)
        {
            switch (token & BlittableJsonReaderBase.TypesMask)
            {
                case BlittableJsonToken.StartArray:
                    var array = (BlittableJsonReaderArray)val;
                    for (int i = 0; i < array.Length; i++)
                    {
                        (object nested, BlittableJsonToken nestedToken) = array.GetValueTokenTupleByIndex(i);
                        InsertToken(field, nested, nestedToken, entryId);
                    }
                    break;
                case BlittableJsonToken.Integer:
                case BlittableJsonToken.LazyNumber:
                case BlittableJsonToken.String:
                case BlittableJsonToken.CompressedString:
                case BlittableJsonToken.Boolean:
                {
                    var str = val.ToString(); // TODO: fixme
                    if (str.Length > 512)
                        break;

                    if (field.TryGetValue(str, out var term) == false)
                    {
                        field[str] = term = new SortedList<long, long>();
                    }

                    term[entryId] = entryId;
                    break;
                }
                case BlittableJsonToken.Null:
                {
                    var str = "<<NULL_VALUE>>"; // todo: fixme
                    if (field.TryGetValue(str, out var term) == false)
                    {
                        field[str] = term = new SortedList<long, long>();
                    }
                    term[entryId] = entryId;

                    break;
                }
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public void Commit()
        {
            foreach (var (field, terms) in _buffer)
            {
                var tree = _transaction.CreateTree(field);
                foreach (var (term, entries) in terms)
                {
                    var fixedSizeTree = tree.FixedTreeFor(term);
                    foreach (long entry in entries.Keys)
                    {
                        fixedSizeTree.Add(entry);
                    }
                }
            }
            _transaction.Commit();
        }

        public void Dispose()
        {
            _transaction?.Dispose();
        }
    }
}

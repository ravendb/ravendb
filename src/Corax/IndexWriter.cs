using System;
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

        public static readonly TableSchema IndexEntriesSchema = new TableSchema
        {
            TableType = (byte)IndexingTableType.IndexEntries
        };

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
                IndexEntriesSchema.DefineIndex(new TableSchema.SchemaIndexDef
                {
                    StartIndex = (int)IndexEntriesTable.DocumentId,
                    Count = 1,
                    Name = IndexEntriesByNameSlice,
                    Type = TableIndexType.BTree,
                    IsGlobal = false
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

        public unsafe long Index(string id, BlittableJsonReaderObject item)
        {
            using var _ = Slice.From(_transaction.Allocator, id, out var idSlice);
            return Index(idSlice, item);
        }

        public unsafe long Index(Slice id, BlittableJsonReaderObject item)
        {
            using (_entries.Allocate(out var builder))
            {
                builder.Add(id);
                builder.Add(item.BasePointer, item.Size);
                long entryId = _entries.Insert(builder);

                BlittableJsonReaderObject.PropertyDetails prop = default;
                for (int i = 0; i < item.Count; i++)
                {
                    item.GetPropertyByIndex(i, ref prop, addObjectToCache: false);
                  
                    Tree field = _transaction.CreateTree(prop.Name.ToString());
                    InsertToken(field, prop.Value, prop.Token, entryId);
                }
                
                return entryId;
            }

        }

        private static void InsertToken(Tree field, object val, BlittableJsonToken token, long entryId)
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

                    var fst = field.FixedTreeFor(str);
                    fst.Add(entryId);
                    break;
                }
                case BlittableJsonToken.Null:
                {
                    var fst = field.FixedTreeFor("<<NULL_VALUE>>"); // TODO: fixme
                    fst.Add(entryId);
                    break;
                }
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public void Commit()
        {
            _transaction.Commit();
        }

        public void Dispose()
        {
            _transaction?.Dispose();
        }
    }
}

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Utils;
using static Raven.Server.Documents.DocumentsStorage;

namespace Raven.Server.Documents
{
    public unsafe class CountersStorage
    {
        private static readonly Guid TombstoneMarker = new Guid("DEAD0000-5A81-4CB1-9E4D-E60B8EBDCE64");

        private readonly DocumentDatabase _documentDatabase;
        private readonly DocumentsStorage _documentsStorage;

        private static readonly Slice CountersSlice;

        public static readonly Slice CountersEtagSlice;

        private static readonly TableSchema CountersSchema = new TableSchema()
        {
            TableType = (byte)TableType.Counters
        };

        private enum CountersTable
        {
            // Format of this is:
            // lower document id, record separator, lower counter name, record separator, 16 bytes dbid
            CounterKey = 0,
            Name = 1, // format of lazy string key is detailed in GetLowerIdSliceAndStorageKey
            Etag = 2,
            Value = 3,
            SourceEtag = 4,
            TransactionMarker = 5
        }

        static CountersStorage()
        {
            Slice.From(StorageEnvironment.LabelsContext, "Counters", ByteStringType.Immutable, out CountersSlice);
            Slice.From(StorageEnvironment.LabelsContext, "CountersEtag", ByteStringType.Immutable, out CountersEtagSlice);

            CountersSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)CountersTable.CounterKey,
                Count = 1
            });
            CountersSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)CountersTable.Etag,
                Name = CountersEtagSlice
            });
        }

        public CountersStorage(DocumentDatabase documentDatabase, Transaction tx)
        {
            _documentDatabase = documentDatabase;
            _documentsStorage = documentDatabase.DocumentsStorage;

            CountersSchema.Create(tx, CountersSlice, 44);
        }

        public IEnumerable<ReplicationBatchItem> GetCountersFrom(DocumentsOperationContext context, long etag)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(CountersSchema.FixedSizeIndexes[CountersEtagSlice], etag, 0))
            {
                yield return ReplicationBatchItem.From(TableValueToCounter(context, ref result.Reader));
            }
        }

        public void ResetCounter(DocumentsOperationContext context, string documentId, string name)
        {
            PutCounter(context, documentId, name, TombstoneMarker, _documentsStorage.GenerateNextEtag(), 0);
        }

        public void PutCounter(DocumentsOperationContext context, string documentId, string name, Guid dbId, long sourceEtag, long value)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false);// never hit
            }

            var table = context.Transaction.InnerTransaction.OpenTable(CountersSchema, CountersSlice);
            using (GetCounterKey(context, documentId, name, dbId, out var counterKey))
            {
                using (DocumentIdWorker.GetSliceFromId(context, name, out Slice nameSlice))
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    if (table.ReadByKey(counterKey, out var existing))
                    {
                        var existingSourceEtag = *(long*)existing.Read((int)CountersTable.SourceEtag, out var size);
                        Debug.Assert(size == sizeof(long));
                        if (existingSourceEtag >= sourceEtag)
                            return;
                    }

                    // TODO: if delete marker, remove all other items
                    // TODO: if exists delete marker, remove it

                    var etag = _documentsStorage.GenerateNextEtag();
                    tvb.Add(counterKey);
                    tvb.Add(nameSlice);
                    tvb.Add(Bits.SwapBytes(etag));
                    tvb.Add(value); 
                    tvb.Add(sourceEtag);
                    tvb.Add(context.TransactionMarkerOffset);

                    table.Set(tvb);
                }
            }
        }

        public void IncrementCounter(DocumentsOperationContext context, string documentId, string name, long value)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false);// never hit
            }
            
            var table = context.Transaction.InnerTransaction.OpenTable(CountersSchema, CountersSlice);
            using (GetCounterKey(context, documentId, name, context.Environment.DbId, out var counterKey))
            {
                using (DocumentIdWorker.GetSliceFromId(context, name, out Slice nameSlice))
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    long prev = 0;
                    if (table.ReadByKey(counterKey, out var existing))
                    {
                        prev = *(long*)existing.Read((int)CountersTable.Value, out var size);
                        Debug.Assert(size == sizeof(long));
                    }

                    // todo: remove delete marker 

                    var etag = _documentsStorage.GenerateNextEtag();
                    tvb.Add(counterKey);
                    tvb.Add(nameSlice);
                    tvb.Add(Bits.SwapBytes(etag));
                    tvb.Add(prev + value); //inc
                    tvb.Add(etag); // source etag
                    tvb.Add(context.TransactionMarkerOffset);

                    table.Set(tvb);
                }
            }
        }

        public IEnumerable<string> GetCountersForDocument(DocumentsOperationContext context, string docId, int skip, int take)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(CountersSchema, CountersSlice);
            using (GetCounterPartialKey(context, docId, out var key))
            {
                ByteStringContext<ByteStringMemoryCache>.ExternalScope scope = default;
                ByteString prev = default;
                foreach (var result in table.SeekByPrimaryKeyPrefix(key, Slices.Empty, skip))
                {
                    if (take-- <= 0)
                        break;

                    var currentScope = ExtractCounterName(context, result.Key, key, out var current, out var dbId);

                    if (dbId == TombstoneMarker)
                        continue;

                    if (prev.HasValue && prev.Match(current))
                    {
                        // already seen this one, skip it 
                        currentScope.Dispose();
                        continue;
                    }

                    yield return current.ToString(new UTF8Encoding());

                    prev = current;
                    scope = currentScope;
                }

                scope.Dispose();
            }
        }

        public long GetCounterValue(DocumentsOperationContext context, string docId, string name)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(CountersSchema, CountersSlice);
            using (GetCounterPartialKey(context, docId, name, out var key))
            {
                long value = 0;
                foreach (var result in table.SeekByPrimaryKeyPrefix(key, Slices.Empty, 0))
                {
                    var pCounterDbValue = result.Value.Reader.Read((int)CountersTable.Value, out var size);
                    Debug.Assert(size == sizeof(long));
                    value += *(long*)pCounterDbValue;
                }

                return value;
            }
        }

        public Dictionary<Guid, long> GetCounterValues(DocumentsOperationContext context, string docId, string name)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(CountersSchema, CountersSlice);
            using (GetCounterPartialKey(context, docId, name, out var keyPerfix))
            {
                var dic = new Dictionary<Guid, long>();
                foreach (var result in table.SeekByPrimaryKeyPrefix(keyPerfix, Slices.Empty, 0))
                {
                    var counterKey = result.Value.Reader.Read((int)CountersTable.CounterKey, out var size);
                    Debug.Assert(size > sizeof(Guid));
                    Guid* pDbId = (Guid*)(counterKey + size - sizeof(Guid));
                    var pCounterDbValue = result.Value.Reader.Read((int)CountersTable.Value, out size);
                    Debug.Assert(size == sizeof(long));
                    dic[*pDbId] = *(long*)pCounterDbValue;
                }

                return dic;
            }
        }

        private static ByteStringContext<ByteStringMemoryCache>.ExternalScope ExtractCounterName(DocumentsOperationContext context, Slice counterKey, Slice documentIdPrefix, out ByteString current, out Guid dbId)
        {
            var scope = context.Allocator.FromPtr(counterKey.Content.Ptr + documentIdPrefix.Size,
                counterKey.Size - documentIdPrefix.Size - sizeof(Guid) - 1, /* record separator*/
                ByteStringType.Immutable,
                out current
            );

            dbId = *(Guid*)(counterKey.Content.Ptr + counterKey.Size - sizeof(Guid));

            return scope;
        }

        public ByteStringContext.InternalScope GetCounterKey(DocumentsOperationContext context, string documentId, string name, Guid dbId, out Slice partialKeySlice)
        {
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out var docIdLower, out _))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, name, out var nameLower, out _))
            {
                var scope = context.Allocator.Allocate(docIdLower.Size
                                                       + 1 // record separator
                                                       + nameLower.Size
                                                       + 1 // record separator
                                                       + sizeof(Guid),
                                                       out ByteString buffer);

                docIdLower.CopyTo(buffer.Ptr);
                buffer.Ptr[docIdLower.Size] = SpecialChars.RecordSeparator;
                byte* dest = buffer.Ptr + docIdLower.Size + 1;
                nameLower.CopyTo(dest);
                dest[nameLower.Size] = SpecialChars.RecordSeparator;
                Memory.Copy(dest + nameLower.Size + 1, (byte*)&dbId, sizeof(Guid));

                partialKeySlice = new Slice(buffer);

                return scope;
            }
        }

        public ByteStringContext.InternalScope GetCounterPartialKey(DocumentsOperationContext context, string documentId, string name, out Slice partialKeySlice)
        {
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out var docIdLower, out _))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, name, out var nameLower, out _))
            {
                var scope = context.Allocator.Allocate(docIdLower.Size
                                                       + 1 // record separator
                                                       + nameLower.Size
                                                       + 1 // record separator
                                                       , out ByteString buffer);

                docIdLower.CopyTo(buffer.Ptr);
                buffer.Ptr[docIdLower.Size] = SpecialChars.RecordSeparator;

                byte* dest = buffer.Ptr + docIdLower.Size + 1;
                nameLower.CopyTo(dest);
                dest[nameLower.Size] = SpecialChars.RecordSeparator;

                partialKeySlice = new Slice(buffer);

                return scope;
            }
        }

        public ByteStringContext.InternalScope GetCounterPartialKey(DocumentsOperationContext context, string documentId,  out Slice partialKeySlice)
        {
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out var docIdLower, out _))
            {
                var scope = context.Allocator.Allocate(docIdLower.Size
                                                       + 1 // record separator
                                                       , out ByteString buffer);

                docIdLower.CopyTo(buffer.Ptr);
                buffer.Ptr[docIdLower.Size] = SpecialChars.RecordSeparator;

                partialKeySlice = new Slice(buffer);

                return scope;
            }
        }

        private static Counter TableValueToCounter(DocumentsOperationContext context, ref TableValueReader tvr)
        {
            var result = new Counter
            {
                StorageId = tvr.Id,
                Key = TableValueToString(context, (int)CountersTable.CounterKey, ref tvr),
                Name = TableValueToId(context, (int)CountersTable.Name, ref tvr),
                Etag = TableValueToEtag((int)CountersTable.Etag, ref tvr),
                Value = TableValueToEtag((int)CountersTable.Value, ref tvr),
                SourceEtag = TableValueToEtag((int)CountersTable.SourceEtag, ref tvr),
                TransactionMarker = *(short*)tvr.Read((int)CountersTable.TransactionMarker, out int _)
            };

            return result;
        }
    }
}

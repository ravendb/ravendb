using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Raven.Client;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Utils;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.Documents.Replication.ReplicationBatchItem;
using Raven.Server.Utils;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Exceptions.Documents.Counters;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public unsafe class CountersStorage
    {
        private const int DbIdAsBase64Size = 22;

        private readonly DocumentDatabase _documentDatabase;
        private readonly DocumentsStorage _documentsStorage;

        private static readonly Slice CountersTombstonesSlice;
        public static readonly Slice AllCountersEtagSlice;
        private static readonly Slice CollectionCountersEtagsSlice;
        private static readonly Slice CounterKeysSlice;

        public static readonly string CountersTombstones = "Counters.Tombstones";

        private static readonly TableSchema CountersSchema = new TableSchema
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
            ChangeVector = 4,
            Collection = 5,
            TransactionMarker = 6
        }

        static CountersStorage()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "AllCountersEtags", ByteStringType.Immutable, out AllCountersEtagSlice);
                Slice.From(ctx, "CollectionCountersEtags", ByteStringType.Immutable, out CollectionCountersEtagsSlice);
                Slice.From(ctx, "CounterKeys", ByteStringType.Immutable, out CounterKeysSlice);
                Slice.From(ctx, CountersTombstones, ByteStringType.Immutable, out CountersTombstonesSlice);
            }
            CountersSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)CountersTable.CounterKey,
                Count = 1,
                Name = CounterKeysSlice,
                IsGlobal = true,
            });

            CountersSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)CountersTable.Etag,
                Name = AllCountersEtagSlice,
                IsGlobal = true
            });

            CountersSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)CountersTable.Etag,
                Name = CollectionCountersEtagsSlice
            });
        }

        public CountersStorage(DocumentDatabase documentDatabase, Transaction tx)
        {
            _documentDatabase = documentDatabase;
            _documentsStorage = documentDatabase.DocumentsStorage;

            tx.CreateTree(CounterKeysSlice);

            TombstonesSchema.Create(tx, CountersTombstonesSlice, 16);
        }

        public IEnumerable<ReplicationBatchItem> GetCountersFrom(DocumentsOperationContext context, long etag)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(CountersSchema.FixedSizeIndexes[AllCountersEtagSlice], etag, 0))
            {
                yield return CreateReplicationBatchItem(context, result);
            }
        }

        public IEnumerable<CounterDetail> GetCountersFrom(DocumentsOperationContext context, long etag, int skip, int take)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(CountersSchema.FixedSizeIndexes[AllCountersEtagSlice], etag, skip))
            {
                if (take-- <= 0)
                    yield break;

                yield return TableValueToCounterDetail(context, result.Reader);
            }
        }

        public IEnumerable<CounterDetail> GetCountersFrom(DocumentsOperationContext context, string collection, long etag, int skip, int take)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                yield break;

            var table = GetCountersTable(context.Transaction.InnerTransaction, collectionName);

            if (table == null)
                yield break;

            foreach (var result in table.SeekForwardFrom(CountersSchema.FixedSizeIndexes[CollectionCountersEtagsSlice], etag, skip))
            {
                if (take-- <= 0)
                    yield break;

                yield return TableValueToCounterDetail(context, result.Reader);
            }
        }

        public long GetNumberOfCountersToProcess(DocumentsOperationContext context, string collection, long afterEtag, out long totalCount)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
            {
                totalCount = 0;
                return 0;
            }

            var table = GetCountersTable(context.Transaction.InnerTransaction, collectionName);

            if (table == null)
            {
                totalCount = 0;
                return 0;
            }

            var indexDef = CountersSchema.FixedSizeIndexes[CollectionCountersEtagsSlice];

            return table.GetNumberOfEntriesAfter(indexDef, afterEtag, out totalCount);
        }

        public long GetNumberOfTombstonesToProcess(DocumentsOperationContext context, long afterEtag, out long totalCount)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, CountersTombstones);

            if (table == null)
            {
                totalCount = 0;
                return 0;
            }

            var indexDef = TombstonesSchema.FixedSizeIndexes[CollectionEtagsSlice];

            return table.GetNumberOfEntriesAfter(indexDef, afterEtag, out totalCount);
        }

        public static CounterDetail TableValueToCounterDetail(JsonOperationContext context, TableValueReader tvr)
        {
            var (doc, name) = ExtractDocIdAndName(context, tvr);

            return new CounterDetail
            {
                DocumentId = doc,
                LazyDocumentId = doc,
                CounterName = name,
                ChangeVector = TableValueToString(context, (int)CountersTable.ChangeVector, ref tvr),
                TotalValue = TableValueToLong((int)CountersTable.Value, ref tvr),
                Etag = TableValueToEtag((int)CountersTable.Etag, ref tvr),
            };
        }

        private static (LazyStringValue Doc, LazyStringValue Name) ExtractDocIdAndName(JsonOperationContext context, TableValueReader tvr)
        {
            var p = tvr.Read((int)CountersTable.CounterKey, out var size);
            Debug.Assert(size > DbIdAsBase64Size + 2 /* record separators */);
            int sizeOfDocId = 0;
            for (; sizeOfDocId < size; sizeOfDocId++)
            {
                if (p[sizeOfDocId] == SpecialChars.RecordSeparator)
                    break;
            }

            var doc = context.AllocateStringValue(null, p, sizeOfDocId);
            var name = ExtractCounterName(context, tvr);
            return (doc, name);
        }

        public static (LazyStringValue DocId, string CounterName) ExtractDocIdAndCounterNameFromTombstone(JsonOperationContext context,
            LazyStringValue counterTombstoneId)
        {
            var p = counterTombstoneId.Buffer;
            var size = counterTombstoneId.Size;

            int sizeOfDocId = 0;
            for (; sizeOfDocId < size; sizeOfDocId++)
            {
                if (p[sizeOfDocId] == SpecialChars.RecordSeparator)
                    break;
            }

            var doc = context.AllocateStringValue(null, p, sizeOfDocId);
            var name = Encoding.UTF8.GetString(p + sizeOfDocId + 1, size - (sizeOfDocId + 2));

            return (doc, name);
        }

        private static ReplicationBatchItem CreateReplicationBatchItem(DocumentsOperationContext context, Table.TableValueHolder result)
        {
            var (doc, name) = ExtractDocIdAndName(context, result.Reader);

            return new ReplicationBatchItem
            {
                Type = ReplicationItemType.Counter,
                Id = doc,
                Etag = TableValueToEtag((int)CountersTable.Etag, ref result.Reader),
                Name = name,
                ChangeVector = TableValueToString(context, (int)CountersTable.ChangeVector, ref result.Reader),
                Value = TableValueToLong((int)CountersTable.Value, ref result.Reader),
                Collection = TableValueToId(context, (int)CountersTable.Collection, ref result.Reader),
                TransactionMarker = TableValueToShort((int)CountersTable.TransactionMarker, nameof(ReplicationBatchItem.TransactionMarker), ref result.Reader),
            };
        }

        public void PutCounter(DocumentsOperationContext context, string documentId, string collection, string name, long value)
        {
            PutCounterImpl(context, documentId, collection, name, null, value);
        }

        public void PutCounter(DocumentsOperationContext context, string documentId, string collection, string name, string changeVector, long value)
        {
            PutCounterImpl(context, documentId, collection, name, changeVector, value);
        }

        private void PutCounterImpl(DocumentsOperationContext context, string documentId, string collection, string name, string changeVector, long value)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false);// never hit
            }

            var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            var table = GetCountersTable(context.Transaction.InnerTransaction, collectionName);

            using (GetCounterKey(context, documentId, name, changeVector ?? context.Environment.Base64Id, out var counterKey))
            {
                using (DocumentIdWorker.GetStringPreserveCase(context, name, out Slice nameSlice))
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    if (changeVector != null)
                    {
                        if (table.ReadByKey(counterKey, out var existing))
                        {
                            var existingChangeVector = TableValueToChangeVector(context, (int)CountersTable.ChangeVector, ref existing);

                            if (ChangeVectorUtils.GetConflictStatus(changeVector, existingChangeVector) == ConflictStatus.AlreadyMerged)
                                return;
                        }
                    }

                    RemoveTombstoneIfExists(context, documentId, name);

                    var etag = _documentsStorage.GenerateNextEtag();

                    if (changeVector == null)
                    {
                        changeVector = ChangeVectorUtils
                            .TryUpdateChangeVector(_documentDatabase.ServerStore.NodeTag, _documentsStorage.Environment.Base64Id, etag, string.Empty)
                            .ChangeVector;
                    }

                    using (Slice.From(context.Allocator, changeVector, out var cv))
                    using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
                    {
                        tvb.Add(counterKey);
                        tvb.Add(nameSlice);
                        tvb.Add(Bits.SwapBytes(etag));
                        tvb.Add(value);
                        tvb.Add(cv);
                        tvb.Add(collectionSlice);
                        tvb.Add(context.TransactionMarkerOffset);

                        table.Set(tvb);
                    }

                    UpdateMetrics(counterKey, name, changeVector, collection);

                    context.Transaction.AddAfterCommitNotification(new CounterChange
                    {
                        ChangeVector = changeVector,
                        DocumentId = documentId,
                        Name = name,
                        Value = value,
                        Type = CounterChangeTypes.Put
                    });
                }
            }
        }

        private void UpdateMetrics(Slice counterKey, string counterName, string changeVector, string collection)
        {
            _documentDatabase.Metrics.Counters.PutsPerSec.MarkSingleThreaded(1);
            var bytesPutsInBytes =
                counterKey.Size + counterName.Length
                                + sizeof(long) // etag 
                                + sizeof(long) // counter value
                                + changeVector.Length + collection.Length;

            _documentDatabase.Metrics.Counters.BytesPutsPerSec.MarkSingleThreaded(bytesPutsInBytes);
        }

        private void RemoveTombstoneIfExists(DocumentsOperationContext context, string documentId, string name)
        {
            using (GetCounterPartialKey(context, documentId, name, out var keyPrefix))
            {
                var tombstoneTable = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, CountersTombstonesSlice);

                if (tombstoneTable.ReadByKey(keyPrefix, out var existingTombstone))
                {
                    tombstoneTable.Delete(existingTombstone.Id);
                }
            }
        }

        private HashSet<string> _tableCreated = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        public Table GetCountersTable(Transaction tx, CollectionName collection)
        {
            var tableName = collection.GetTableName(CollectionTableType.Counters);

            if (tx.IsWriteTransaction && _tableCreated.Contains(collection.Name) == false)
            {
                // RavenDB-11705: It is possible that this will revert if the transaction
                // aborts, so we must record this only after the transaction has been committed
                // note that calling the Create() method multiple times is a noop
                CountersSchema.Create(tx, tableName, 16);
                tx.LowLevelTransaction.OnDispose += _ =>
                {
                    if (tx.LowLevelTransaction.Committed == false)
                        return;

                    // not sure if we can _rely_ on the tx write lock here, so let's be safe and create
                    // a new instance, just in case 
                    _tableCreated = new HashSet<string>(_tableCreated, StringComparer.OrdinalIgnoreCase)
                     {
                         collection.Name
                     };
                };
            }

            return tx.OpenTable(CountersSchema, tableName);
        }

        public string IncrementCounter(DocumentsOperationContext context, string documentId, string collection, string name, long delta, out bool exists)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false);// never hit
            }

            var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            var table = GetCountersTable(context.Transaction.InnerTransaction, collectionName);

            using (GetCounterKey(context, documentId, name, context.Environment.Base64Id, out var counterKey))
            {
                var value = delta;
                exists = table.ReadByKey(counterKey, out var existing);
                if (exists)
                {
                    var prev = *(long*)existing.Read((int)CountersTable.Value, out var size);
                    Debug.Assert(size == sizeof(long));
                    try
                    {
                        value = checked(prev + delta); //inc
                    }
                    catch (OverflowException e)
                    {
                        CounterOverflowException.ThrowFor(documentId, name, prev, delta, e);
                    }
                }

                RemoveTombstoneIfExists(context, documentId, name);

                var etag = _documentsStorage.GenerateNextEtag();
                var result = ChangeVectorUtils.TryUpdateChangeVector(_documentDatabase.ServerStore.NodeTag, _documentsStorage.Environment.Base64Id, etag, string.Empty);

                using (Slice.From(context.Allocator, result.ChangeVector, out var cv))
                using (DocumentIdWorker.GetStringPreserveCase(context, name, out Slice nameSlice))
                using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(counterKey);
                    tvb.Add(nameSlice);
                    tvb.Add(Bits.SwapBytes(etag));
                    tvb.Add(value);
                    tvb.Add(cv);
                    tvb.Add(collectionSlice);
                    tvb.Add(context.TransactionMarkerOffset);

                    table.Set(tvb);
                }

                UpdateMetrics(counterKey, name, result.ChangeVector, collection);

                context.Transaction.AddAfterCommitNotification(new CounterChange
                {
                    ChangeVector = result.ChangeVector,
                    DocumentId = documentId,
                    Name = name,
                    Type = exists ? CounterChangeTypes.Increment : CounterChangeTypes.Put,
                    Value = value
                });

                return result.ChangeVector;
            }
        }

        public IEnumerable<string> GetCountersForDocument(DocumentsOperationContext context, string docId)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            using (GetCounterPartialKey(context, docId, out var key))
            {
                LazyStringValue prev = null;
                foreach (var result in table.SeekByPrimaryKeyPrefix(key, Slices.Empty, 0))
                {
                    var current = ExtractCounterName(context, result.Value.Reader);

                    if (prev?.Equals(current) == true)
                    {
                        // already seen this one, skip it 
                        continue;
                    }

                    yield return current;

                    prev?.Dispose();
                    prev = current;
                }
            }
        }

        public long? GetCounterValue(DocumentsOperationContext context, string docId, string counterName)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            using (GetCounterPartialKey(context, docId, counterName, out var key))
            {
                long? value = null;
                foreach (var result in table.SeekByPrimaryKeyPrefix(key, Slices.Empty, 0))
                {
                    value = value ?? 0;
                    var pCounterDbValue = result.Value.Reader.Read((int)CountersTable.Value, out var size);
                    Debug.Assert(size == sizeof(long));
                    try
                    {
                        value = checked(value + *(long*)pCounterDbValue);
                    }
                    catch (OverflowException e)
                    {
                        CounterOverflowException.ThrowFor(docId, counterName, e);
                    }
                }

                return value;
            }
        }

        public IEnumerable<(string ChangeVector, long Value)> GetCounterValues(DocumentsOperationContext context, string docId, string counterName)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            using (GetCounterPartialKey(context, docId, counterName, out var keyPrefix))
            {
                foreach (var result in table.SeekByPrimaryKeyPrefix(keyPrefix, Slices.Empty, 0))
                {
                    (string, long) val = ExtractDbIdAndValue(result);
                    yield return val;
                }
            }
        }

        private static (string ChangeVector, long Value) ExtractDbIdAndValue((Slice Key, Table.TableValueHolder Value) result)
        {
            var counterKey = result.Value.Reader.Read((int)CountersTable.CounterKey, out var size);
            Debug.Assert(size > DbIdAsBase64Size);
            var pCounterDbValue = result.Value.Reader.Read((int)CountersTable.Value, out size);
            Debug.Assert(size == sizeof(long));
            var changeVector = result.Value.Reader.Read((int)CountersTable.ChangeVector, out size);

            return (Encoding.UTF8.GetString(changeVector, size), *(long*)pCounterDbValue);
        }

        private static LazyStringValue ExtractCounterName(JsonOperationContext context, TableValueReader tvr)
        {
            return TableValueToId(context, (int)CountersTable.Name, ref tvr);

        }

        public ByteStringContext.InternalScope GetCounterKey(DocumentsOperationContext context, string documentId, string name, string changeVector, out Slice partialKeySlice)
        {
            Debug.Assert(changeVector.Length >= DbIdAsBase64Size);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out var docIdLower, out _))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, name, out var nameLower, out _))
            using (Slice.From(context.Allocator, changeVector, out var cv))
            {
                var scope = context.Allocator.Allocate(docIdLower.Size
                                                       + 1 // record separator
                                                       + nameLower.Size
                                                       + 1 // record separator
                                                       + DbIdAsBase64Size, // db id
                                                       out ByteString buffer);

                docIdLower.CopyTo(buffer.Ptr);
                buffer.Ptr[docIdLower.Size] = SpecialChars.RecordSeparator;
                byte* dest = buffer.Ptr + docIdLower.Size + 1;
                nameLower.CopyTo(dest);
                dest[nameLower.Size] = SpecialChars.RecordSeparator;
                cv.CopyTo(cv.Size - DbIdAsBase64Size, dest, nameLower.Size + 1, DbIdAsBase64Size);

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

        public ByteStringContext.InternalScope GetCounterPartialKey(DocumentsOperationContext context, string documentId, out Slice partialKeySlice)
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

        public void DeleteCountersForDocument(DocumentsOperationContext context, string documentId, CollectionName collection)
        {
            // this will called as part of document's delete, so we don't bother creating
            // tombstones (existing tombstones will remain and be cleaned up by the usual
            // tombstone cleaner task

            var table = GetCountersTable(context.Transaction.InnerTransaction, collection);

            if (table.NumberOfEntries == 0)
                return;

            using (GetCounterPartialKey(context, documentId, out var keyPrefix))
            {
                table.DeleteByPrimaryKeyPrefix(keyPrefix);
            }
        }

        public string DeleteCounter(DocumentsOperationContext context, string documentId, string collection, string counterName)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false);// never hit
            }

            using (GetCounterPartialKey(context, documentId, counterName, out var keyPrefix))
            {
                var lastModifiedTicks = _documentDatabase.Time.GetUtcNow().Ticks;
                return DeleteCounter(context, keyPrefix, collection, lastModifiedTicks,
                    // let's avoid creating a tombstone for missing counter if writing locally
                    forceTombstone: false);
            }
        }

        public string DeleteCounter(DocumentsOperationContext context, Slice key, string collection, long lastModifiedTicks, bool forceTombstone)
        {
            var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            var table = GetCountersTable(context.Transaction.InnerTransaction, collectionName);

            long deletedEtag = -1;
            string documentId = null;
            string name = null;
            var deleted = table.DeleteByPrimaryKeyPrefix(key, tvh =>
            {
                var etag = *(long*)tvh.Reader.Read((int)CountersTable.Etag, out var size);
                deletedEtag = Math.Max(Bits.SwapBytes(etag), deletedEtag);
                Debug.Assert(size == sizeof(long));
                (documentId, name) = ExtractDocIdAndName(context, tvh.Reader);
            });

            if (deleted == false && forceTombstone == false)
                return null;

            if (deletedEtag == -1)
            {
                deletedEtag = -_documentsStorage.GenerateNextEtag();
            }

            var newEtag = _documentsStorage.GenerateNextEtag();
            _documentsStorage.EnsureLastEtagIsPersisted(context, newEtag);

            var newChangeVector = ChangeVectorUtils.NewChangeVector(_documentDatabase.ServerStore.NodeTag, newEtag, _documentsStorage.Environment.Base64Id);

            CreateTombstone(context, key, collection, deletedEtag, lastModifiedTicks, newEtag, newChangeVector);

            context.Transaction.AddAfterCommitNotification(new CounterChange
            {
                ChangeVector = newChangeVector,
                DocumentId = documentId,
                Name = name,
                Type = CounterChangeTypes.Delete
            });

            return newChangeVector;
        }

        private void CreateTombstone(DocumentsOperationContext context, Slice keySlice, string collectionName, long deletedEtag, long lastModifiedTicks, long newEtag, string newChangeVector)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, CountersTombstonesSlice);
            using (Slice.From(context.Allocator, newChangeVector, out var cv))
            using (DocumentIdWorker.GetStringPreserveCase(context, collectionName, out Slice collectionSlice))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(keySlice.Content.Ptr, keySlice.Size);
                tvb.Add(Bits.SwapBytes(newEtag));
                tvb.Add(Bits.SwapBytes(deletedEtag)); // etag that was deleted
                tvb.Add(context.GetTransactionMarker());
                tvb.Add((byte)Tombstone.TombstoneType.Counter);
                tvb.Add(collectionSlice);
                tvb.Add((int)DocumentFlags.None);
                tvb.Add(cv.Content.Ptr, cv.Size); // change vector
                tvb.Add(lastModifiedTicks);
                table.Insert(tvb);
            }
        }

        public static void AssertCounters(BlittableJsonReaderObject document, DocumentFlags flags)
        {
            if ((flags & DocumentFlags.HasCounters) == DocumentFlags.HasCounters)
            {
                if (document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                    metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray _) == false)
                {
                    Debug.Assert(false, $"Found {DocumentFlags.HasCounters} flag but {Constants.Documents.Metadata.Counters} is missing from metadata.");
                }
            }
            else
            {
                if (document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
                    metadata.TryGet(Constants.Documents.Metadata.Counters, out BlittableJsonReaderArray counters))
                {
                    Debug.Assert(false, $"Found {Constants.Documents.Metadata.Counters}({counters.Length}) in metadata but {DocumentFlags.HasCounters} flag is missing.");
                }
            }
        }

        public long GetNumberOfCounterEntries(DocumentsOperationContext context)
        {
            var fstIndex = CountersSchema.FixedSizeIndexes[AllCountersEtagSlice];
            var fst = context.Transaction.InnerTransaction.FixedTreeFor(fstIndex.Name, sizeof(long));
            return fst.NumberOfEntries;
        }

        public void UpdateDocumentCounters(DocumentsOperationContext context, Document doc, string docId,
            SortedSet<string> countersToAdd, HashSet<string> countersToRemove, NonPersistentDocumentFlags nonPersistentDocumentFlags)
        {
            if (countersToRemove.Count == 0 && countersToAdd.Count == 0)
                return;

            var data = doc.Data;
            BlittableJsonReaderArray metadataCounters = null;
            if (data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
            {
                metadata.TryGet(Constants.Documents.Metadata.Counters, out metadataCounters);
            }

            var counters = GetCountersForDocument(metadataCounters, countersToAdd, countersToRemove, out var hadModifications);
            if (hadModifications == false)
                return;

            var flags = doc.Flags.Strip(DocumentFlags.FromClusterTransaction);
            if (counters.Count == 0)
            {
                flags = flags.Strip(DocumentFlags.HasCounters);
                if (metadata != null)
                {
                    metadata.Modifications = new DynamicJsonValue(metadata);
                    metadata.Modifications.Remove(Constants.Documents.Metadata.Counters);
                    data.Modifications = new DynamicJsonValue(data)
                    {
                        [Constants.Documents.Metadata.Key] = metadata
                    };
                }
            }
            else
            {
                flags |= DocumentFlags.HasCounters;
                data.Modifications = new DynamicJsonValue(data);
                if (metadata == null)
                {
                    data.Modifications[Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Counters] = new DynamicJsonArray(counters)
                    };
                }
                else
                {
                    metadata.Modifications = new DynamicJsonValue(metadata)
                    {
                        [Constants.Documents.Metadata.Counters] = new DynamicJsonArray(counters)
                    };
                    data.Modifications[Constants.Documents.Metadata.Key] = metadata;
                }
            }

            var newDocumentData = context.ReadObject(doc.Data, docId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            _documentDatabase.DocumentsStorage.Put(context, docId, null, newDocumentData, flags: flags, nonPersistentFlags: nonPersistentDocumentFlags);
        }

        private static SortedSet<string> GetCountersForDocument(BlittableJsonReaderArray metadataCounters, SortedSet<string> countersToAdd, HashSet<string> countersToRemove, out bool modified)
        {
            modified = false;
            if (metadataCounters == null)
            {
                modified = true;
                return countersToAdd;
            }

            foreach (var counter in metadataCounters)
            {
                var str = counter.ToString();
                if (countersToRemove.Contains(str))
                {
                    modified = true;
                    continue;
                }

                countersToAdd.Add(str);
            }

            if (modified == false)
            {
                // if no counter was removed, we can be sure that there are no modification when the counter's count in the metadata is equal to the count of countersToAdd 
                modified = countersToAdd.Count != metadataCounters.Length;
            }

            return countersToAdd;
        }
    }
}

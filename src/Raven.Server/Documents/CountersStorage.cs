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
            // lower document id, record separator, prefix 
            DocumentId = 0,
            Etag = 1,         
            ChangeVector = 2,
            Data = 3,
            Collection = 4,
            TransactionMarker = 5
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
                StartIndex = (int)CountersTable.DocumentId,
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
                foreach (var batchItem in CreateReplicationBatchItems(context, result))
                {
                    yield return batchItem;
                }
                //yield return CreateReplicationBatchItem(context, result);
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
                foreach (var counterDetail in TableValueToCounterDetails(context, result.Reader))
                {
                    yield return counterDetail;
                }
                //yield return TableValueToCounterDetail(context, result.Reader);
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

        public static IEnumerable<CounterDetail> TableValueToCounterDetails(JsonOperationContext context, TableValueReader tvr)
        {
            var data = GetData(context, ref tvr);
            var prop = new BlittableJsonReaderObject.PropertyDetails();
            for (var i=0; i<data.Count; i++)
            {
                data.GetPropertyByIndex(i, ref prop);
                var counterValues = (BlittableJsonReaderObject)prop.Value;
                var innerProp = new BlittableJsonReaderObject.PropertyDetails();

                foreach (var j in counterValues.GetPropertiesByInsertionOrder())
                {
                    counterValues.GetPropertyByIndex(j, ref innerProp);

                    var dbId = innerProp.Name;
                    var valueAsArray = (BlittableJsonReaderArray)innerProp.Value;
                    var value = (long)valueAsArray[0];
                    var sourceEtag = (long)valueAsArray[1];

                    yield return new CounterDetail
                    {
                        DocumentId = TableValueToString(context, (int)CountersTable.DocumentId, ref tvr),
                        CounterName = prop.Name,
                        ChangeVector = TableValueToChangeVector(context, (int)CountersTable.ChangeVector, ref tvr),
                        TotalValue = value,
                        Etag = sourceEtag,
                        DbId = dbId
                    };
                }
            }
        }

        public static CounterDetail TableValueToCounterDetail(JsonOperationContext context, TableValueReader tvr)
        {
            return new CounterDetail();
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

        private static IEnumerable<ReplicationBatchItem> CreateReplicationBatchItems(DocumentsOperationContext context, Table.TableValueHolder tvh)
        {
            var data = GetData(context, ref tvh.Reader);

            var prop = new BlittableJsonReaderObject.PropertyDetails();
            for (int index = 0; index < data.Count; index++)
            {
                data.GetPropertyByIndex(index, ref prop);
                yield return new ReplicationBatchItem
                {
                    Type = ReplicationItemType.Counter,

                    Id = TableValueToString(context, (int)CountersTable.DocumentId, ref tvh.Reader),
                    Name = prop.Name,
                    ChangeVector = TableValueToChangeVector(context, (int)CountersTable.ChangeVector, ref tvh.Reader),
                    //Value = value,
                    Values = (BlittableJsonReaderObject)prop.Value,
                    Collection = TableValueToId(context, (int)CountersTable.Collection, ref tvh.Reader),
                    Etag = TableValueToEtag((int)CountersTable.Etag, ref tvh.Reader)
                };
            }
        }

        private static ReplicationBatchItem CreateReplicationBatchItem(DocumentsOperationContext context, Table.TableValueHolder result)
        {

            var data = GetData(context, ref result.Reader);

            return new ReplicationBatchItem
            {
                Type = ReplicationItemType.Counter,
                //Id = doc,

                Etag = TableValueToEtag((int)CountersTable.Etag, ref result.Reader),
                //Name = name,
                ChangeVector = TableValueToString(context, (int)CountersTable.ChangeVector, ref result.Reader),
                Data = data,
                //Value = TableValueToLong((int)CountersTable.Value, ref result.Reader),
                //Collection = TableValueToId(context, (int)CountersTable.Collection, ref result.Reader),
                TransactionMarker = TableValueToShort((int)CountersTable.TransactionMarker, nameof(ReplicationBatchItem.TransactionMarker), ref result.Reader),
            };
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

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out Slice lowerId, out _))
            {
                BlittableJsonReaderObject data = null;
                var value = delta;
                if (table.ReadByKey(lowerId, out var existing))
                {
                    data = new BlittableJsonReaderObject(existing.Read((int)CountersTable.Data, out int oldSize), oldSize, context);
                }

                exists = false;

                if (data != null)
                {
                    if (data.TryGet(name, out BlittableJsonReaderObject oldCounter))
                    {
                        if (oldCounter.TryGet(context.Environment.Base64Id, out BlittableJsonReaderArray prev))
                        {
                            try
                            {
                                value = checked((long)prev[0] + delta); //inc
                            }
                            catch (OverflowException e)
                            {
                                CounterOverflowException.ThrowFor(documentId, name, (long)prev[0], delta, e);
                            }
                        }

                        exists = true;
                        oldCounter.Modifications = new DynamicJsonValue(oldCounter)
                        {
                            [context.Environment.Base64Id] = new DynamicJsonArray { value, _documentsStorage.GenerateNextEtag() }
                        };

                        data = context.ReadObject(data, null, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                    }
                }
                if (exists == false)
                {
                    /*                    if (data.Size > 2 * Voron.Global.Constants.Size.Kilobyte)
                                        {
                                            // TODO: split the counters here
                                        }*/

                    var modified = new DynamicJsonValue(data)
                    {
                        [name] = new DynamicJsonValue
                        {
                            [context.Environment.Base64Id] = new DynamicJsonArray { value, _documentsStorage.GenerateNextEtag() }
                        }
                    };

                    data = data != null ?
                        context.ReadObject(data, null, BlittableJsonDocumentBuilder.UsageMode.ToDisk) :
                        context.ReadObject(modified, null, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                }

                using (GetCounterPartialKey(context, documentId, name, out var counterKey))
                {
                    RemoveTombstoneIfExists(context, counterKey);
                }

                var etag = _documentsStorage.GenerateNextEtag();
                var result = ChangeVectorUtils.TryUpdateChangeVector(_documentDatabase.ServerStore.NodeTag, _documentsStorage.Environment.Base64Id, etag, string.Empty);

                using (Slice.From(context.Allocator, result.ChangeVector, out var cv))
                using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(lowerId);
                    tvb.Add(Bits.SwapBytes(etag));
                    tvb.Add(cv);
                    tvb.Add(data.BasePointer, data.Size);
                    tvb.Add(collectionSlice);
                    tvb.Add(context.TransactionMarkerOffset);

                    if (existing.Pointer == null)
                    {
                        table.Insert(tvb);
                    }
                    else
                    {
                        table.Update(existing.Id, tvb);
                    }
                }

                UpdateMetrics(lowerId, name, result.ChangeVector, collection);

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

        public void PutCounter(DocumentsOperationContext context, string documentId, string collection, string name, long value)
        {
            PutCounterImpl(context, documentId, collection, name, null, value, null);
        }

        public void PutCounter(DocumentsOperationContext context, string documentId, string collection, string name, string changeVector, long value, string dbId)
        {
            PutCounterImpl(context, documentId, collection, name, changeVector, value, dbId);
        }

        public void PutCounters(DocumentsOperationContext context, string documentId, string collection, string changeVector,
            Dictionary<string, List<(string DbId, long Value, long Etag)>> counterValues)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false); // never hit
            }

            var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            var table = GetCountersTable(context.Transaction.InnerTransaction, collectionName);
            BlittableJsonReaderObject data = null;

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out Slice lowerId, out _))
            {
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    if (changeVector != null && table.ReadByKey(lowerId, out var existing))
                    {                       
                        var existingChangeVector = TableValueToChangeVector(context, (int)CountersTable.ChangeVector, ref existing);

                        if (ChangeVectorUtils.GetConflictStatus(changeVector, existingChangeVector) == ConflictStatus.AlreadyMerged)
                            return;

                        data = new BlittableJsonReaderObject(existing.Read((int)CountersTable.Data, out int oldSize), oldSize, context);
                        data.Modifications = new DynamicJsonValue(data);
                        var modified = false;

                        foreach (var kvp in counterValues)
                        {
                            if (data.TryGet(kvp.Key, out BlittableJsonReaderObject localCounterValues))
                            {
                                localCounterValues.Modifications = new DynamicJsonValue(localCounterValues);

                                foreach (var tuple in kvp.Value)
                                {
                                    if (tuple.DbId == context.Environment.Base64Id)
                                        continue;

                                    if (localCounterValues.TryGetMember(tuple.DbId, out var arr))
                                    {
                                        var localEtag = (long)((BlittableJsonReaderArray)arr)[1];
                                        if (localEtag >= tuple.Etag)
                                            continue;
                                    }

                                    modified = true;
                                    localCounterValues.Modifications[tuple.DbId] = new DynamicJsonArray
                                    {
                                        tuple.Value,
                                        tuple.Etag
                                    };
                                }
                            }
                            else
                            {
                                modified = true;
                                var djv = new DynamicJsonValue();
                                foreach (var tuple in kvp.Value)
                                {
                                    djv[tuple.DbId] = new DynamicJsonArray
                                    {
                                        tuple.Value,
                                        tuple.Etag
                                    };
                                }

                                data.Modifications[kvp.Key] = djv;

                                using (GetCounterPartialKey(context, documentId, kvp.Key, out var counterKey))
                                {
                                    RemoveTombstoneIfExists(context, counterKey);
                                }

                            }

                            if (modified == false)
                                continue;

                            context.Transaction.AddAfterCommitNotification(new CounterChange
                            {
                                ChangeVector = changeVector,
                                DocumentId = documentId,
                                Name = kvp.Key,
                                //Value = value,
                                Type = CounterChangeTypes.Put
                            });

                            UpdateMetrics(lowerId, kvp.Key, changeVector, collection);

                            modified = false;

                        }
                    }

                    if (data == null)
                    {
                        var newData = new DynamicJsonValue();

                        foreach (var kvp in counterValues)
                        {
                            var currentCounter = new DynamicJsonValue();

                            foreach (var tuple in kvp.Value)
                            {
                                currentCounter[tuple.DbId] = new DynamicJsonArray { tuple.Value, tuple.Etag };
                            }

                            newData[kvp.Key] = currentCounter;
                        }

                        data = context.ReadObject(newData, null, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                    }
                    else
                    {
                        data = context.ReadObject(data, null, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                    }

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
                        tvb.Add(lowerId);
                        tvb.Add(Bits.SwapBytes(etag));
                        tvb.Add(cv);
                        tvb.Add(data.BasePointer, data.Size);
                        tvb.Add(collectionSlice);
                        tvb.Add(context.TransactionMarkerOffset);

                        table.Set(tvb);
                    }
                }
            }
        }

        private void PutCounterImpl(DocumentsOperationContext context, string documentId, string collection, string name, string changeVector, long value, string dbId)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false);// never hit
            }

            var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            var table = GetCountersTable(context.Transaction.InnerTransaction, collectionName);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out Slice lowerId, out _))
            {
                BlittableJsonReaderObject data = null;
                TableValueReader existing = default;
                if (changeVector != null)
                {
                    if (table.ReadByKey(lowerId, out existing))
                    {
                        var existingChangeVector = TableValueToChangeVector(context, (int)CountersTable.ChangeVector, ref existing);

                        if (ChangeVectorUtils.GetConflictStatus(changeVector, existingChangeVector) == ConflictStatus.AlreadyMerged)
                            return;

                        data = GetData(context, ref existing);
                    }
                }

                dbId = dbId ?? context.Environment.Base64Id;
                var modified = new DynamicJsonValue(data)
                {
                    [name] = new DynamicJsonValue
                    {
                        [dbId] = new DynamicJsonArray { value, _documentsStorage.GenerateNextEtag() }
                    }
                };

                data = data != null ?
                    context.ReadObject(data, null, BlittableJsonDocumentBuilder.UsageMode.ToDisk) :
                    context.ReadObject(modified, null, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                using (GetCounterPartialKey(context, documentId, name, out var counterKey))
                {
                    RemoveTombstoneIfExists(context, counterKey);
                }

                var etag = _documentsStorage.GenerateNextEtag();

                if (changeVector == null)
                {
                    changeVector = ChangeVectorUtils
                        .TryUpdateChangeVector(_documentDatabase.ServerStore.NodeTag, _documentsStorage.Environment.Base64Id, etag, string.Empty)
                        .ChangeVector;
                }

                using (Slice.From(context.Allocator, changeVector, out var cv))
                using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(lowerId);
                    tvb.Add(Bits.SwapBytes(etag));
                    tvb.Add(cv);
                    tvb.Add(data.BasePointer, data.Size);
                    tvb.Add(collectionSlice);
                    tvb.Add(context.TransactionMarkerOffset);

                    if (existing.Pointer == null)
                    {
                        table.Insert(tvb);
                    }
                    else
                    {
                        table.Update(existing.Id, tvb);
                    }
                }

                UpdateMetrics(lowerId, name, changeVector, collection);

                context.Transaction.AddAfterCommitNotification(new CounterChange
                {
                    ChangeVector = changeVector,
                    DocumentId = documentId,
                    Name = name,
                    Type = CounterChangeTypes.Put,
                    Value = value
                });

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

        private void RemoveTombstoneIfExists(DocumentsOperationContext context, Slice counterKey)
        {
            var tombstoneTable = context.Transaction.InnerTransaction.OpenTable(TombstonesSchema, CountersTombstonesSlice);

            if (tombstoneTable.ReadByKey(counterKey, out var existingTombstone))
            {
                tombstoneTable.Delete(existingTombstone.Id);
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

        public IEnumerable<string> GetCountersForDocument(DocumentsOperationContext context, string docId)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);


            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, docId, out Slice key, out _))
            //using (GetCounterPartialKey(context, docId, out var key))
            {
                if (table.ReadByKey(key, out var existing) == false)
                    yield break;

                var data = GetData(context, ref existing);
                foreach (var prop in data.GetPropertyNames())
                {
                    yield return prop;
                }
            }
        }

        private static BlittableJsonReaderObject GetData(JsonOperationContext context, ref TableValueReader existing)
        {
            return new BlittableJsonReaderObject(existing.Read((int)CountersTable.Data, out int oldSize), oldSize, context);
        }

        public long? GetCounterValue(DocumentsOperationContext context, string docId, string counterName)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, docId, out Slice key, out _))
            {
                if (table.ReadByKey(key, out var tvr) == false)
                    return null;

                var data = GetData(context, ref tvr);
                if (data.TryGet(counterName, out BlittableJsonReaderObject counterValues) == false ||
                    counterValues.Count == 0)
                    return null;

                long value = 0;
                var details = new BlittableJsonReaderObject.PropertyDetails();
                foreach (var index in counterValues.GetPropertiesByInsertionOrder())
                {
                    counterValues.GetPropertyByIndex(index, ref details);
                    var val = ((BlittableJsonReaderArray)details.Value)[0];
                    value += (long)val;
                }

                return value;
            }
        }

        public IEnumerable<(string ChangeVector, long Value)> GetCounterValues(DocumentsOperationContext context, string docId, string counterName)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, docId, out Slice key, out _))
            {
                if (table.ReadByKey(key, out var tvr) == false)
                    yield break;

                var data = GetData(context, ref tvr);
                if (data.TryGet(counterName, out BlittableJsonReaderObject counterValues) == false)
                    yield break;

                var details = new BlittableJsonReaderObject.PropertyDetails();
                foreach (var index in counterValues.GetPropertiesByInsertionOrder())
                {
                    counterValues.GetPropertyByIndex(index, ref details);
                    var val = ((BlittableJsonReaderArray)details.Value)[0];
                    yield return (details.Name, (long)val);
                }

            }
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

        public void DeleteCountersForDocument(DocumentsOperationContext context, string documentId, CollectionName collection)
        {
            // this will called as part of document's delete, so we don't bother creating
            // tombstones (existing tombstones will remain and be cleaned up by the usual
            // tombstone cleaner task

            var table = GetCountersTable(context.Transaction.InnerTransaction, collection);

            if (table.NumberOfEntries == 0)
                return;

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out Slice lowerId, out _))
            {
                table.DeleteByPrimaryKeyPrefix(lowerId);
            }
        }

        public string DeleteCounter(DocumentsOperationContext context, Slice counterKey, string collection, long lastModifiedTicks, bool forceTombstone)
        {
            var (doc, name) = ExtractDocIdAndName(context, counterKey);
            return DeleteCounter(context, doc, collection, name, forceTombstone, lastModifiedTicks);
        }

        public string DeleteCounter(DocumentsOperationContext context, string documentId, string collection, string counterName, bool forceTombstone = false, long lastModifiedTicks = -1)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false);// never hit
            }

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out Slice lowerId, out _))
            {
                var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
                var table = GetCountersTable(context.Transaction.InnerTransaction, collectionName);
                if (table.ReadByKey(lowerId, out var existing) == false)
                {
                    return forceTombstone 
                        ? FindDeletedEtagAndCreateTombstone(context, documentId, collection, counterName, counterToDelete: null, forceTombstone: true, lastModifiedTicks)
                        : null;
                }

                var data = GetData(context, ref existing);
                if (data.TryGet(counterName, out BlittableJsonReaderObject counterToDelete) == false)
                {
                    if (forceTombstone == false)
                        return null;
                }

                var tombstoneChangeVector = FindDeletedEtagAndCreateTombstone(context, documentId, collection, counterName, counterToDelete, forceTombstone, lastModifiedTicks);
                if (counterToDelete == null)
                    return tombstoneChangeVector;

                if (data.Count == 1)
                {
                    table.DeleteByPrimaryKeyPrefix(lowerId);
                }
                else
                {
                    RemoveCounterFromBlittableAndUpdateTable(context, counterName, data, collectionName, table, lowerId);
                }

                return tombstoneChangeVector;
            }
        }

        private string FindDeletedEtagAndCreateTombstone(DocumentsOperationContext context, string documentId, string collection, string counterName,
            BlittableJsonReaderObject counterToDelete, bool forceTombstone, long lastModifiedTicks = -1)
        {
            long deletedEtag = -1;
            if (counterToDelete?.Count > 0)
            {
                var prop = new BlittableJsonReaderObject.PropertyDetails();
                for (int i = 0; i < counterToDelete.Count; i++)
                {
                    counterToDelete.GetPropertyByIndex(i, ref prop);
                    var etag = (long)((BlittableJsonReaderArray)prop.Value)[1];
                    deletedEtag = Math.Max(Bits.SwapBytes(etag), deletedEtag);
                }
            }

            if (deletedEtag == -1)
            {
                if (forceTombstone == false)
                    return null;

                deletedEtag = -_documentsStorage.GenerateNextEtag();
            }

            var etagForTombstone = _documentsStorage.GenerateNextEtag();
            _documentsStorage.EnsureLastEtagIsPersisted(context, etagForTombstone);

            var changeVectorForTombstone = ChangeVectorUtils.NewChangeVector(_documentDatabase.ServerStore.NodeTag, etagForTombstone, _documentsStorage.Environment.Base64Id);

            if (lastModifiedTicks == -1)
                lastModifiedTicks= _documentDatabase.Time.GetUtcNow().Ticks;

            using (GetCounterPartialKey(context, documentId, counterName, out var counterKey))
                CreateTombstone(context, counterKey, collection, deletedEtag, lastModifiedTicks, etagForTombstone, changeVectorForTombstone);

            context.Transaction.AddAfterCommitNotification(new CounterChange
            {
                ChangeVector = changeVectorForTombstone,
                DocumentId = documentId,
                Name = counterName,
                Type = CounterChangeTypes.Delete
            });

            return changeVectorForTombstone;
        }

        private void RemoveCounterFromBlittableAndUpdateTable(DocumentsOperationContext context, string counterName, BlittableJsonReaderObject data, CollectionName collectionName, Table table,
            Slice lowerId)
        {
            data.Modifications = new DynamicJsonValue(data);
            data.Modifications.Remove(counterName);

            data = context.ReadObject(data, null, BlittableJsonDocumentBuilder.UsageMode.ToDisk);

            var newEtag = _documentsStorage.GenerateNextEtag();
            var newChangeVector = ChangeVectorUtils.NewChangeVector(_documentDatabase.ServerStore.NodeTag, newEtag, _documentsStorage.Environment.Base64Id);
            using (Slice.From(context.Allocator, newChangeVector, out var cv))
            using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(lowerId);
                tvb.Add(Bits.SwapBytes(newEtag));
                tvb.Add(cv);
                tvb.Add(data.BasePointer, data.Size);
                tvb.Add(collectionSlice);
                tvb.Add(context.TransactionMarkerOffset);

                table.Set(tvb);
            }
        }

        private static void CreateTombstone(DocumentsOperationContext context, Slice keySlice, string collectionName, long deletedEtag, long lastModifiedTicks, long newEtag, string newChangeVector)
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

        private static (LazyStringValue Doc, LazyStringValue Name) ExtractDocIdAndName(JsonOperationContext context, Slice counterKey)
        {
            var p = counterKey.Content.Ptr;
            var size = counterKey.Size;
            //Debug.Assert(size > DbIdAsBase64Size + 2 /* record separators */);
            int sizeOfDocId = 0;
            for (; sizeOfDocId < size; sizeOfDocId++)
            {
                if (p[sizeOfDocId] == SpecialChars.RecordSeparator)
                    break;
            }

            var doc = context.AllocateStringValue(null, p, sizeOfDocId);

            sizeOfDocId++;
            p += sizeOfDocId;
            int sizeOfName = size - sizeOfDocId - 1;
            var name = context.AllocateStringValue(null, p, sizeOfName);
            return (doc, name);
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
            //TODO 
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

            var flags = doc.Flags.Strip(DocumentFlags.HasCounters);
            if (counters.Count == 0)
            {
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

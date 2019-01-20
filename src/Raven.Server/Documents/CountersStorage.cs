using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
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
        public const int DbIdAsBase64Size = 22;

        private readonly DocumentDatabase _documentDatabase;
        private readonly DocumentsStorage _documentsStorage;

        public static readonly Slice AllCountersEtagSlice;
        private static readonly Slice CollectionCountersEtagsSlice;
        private static readonly Slice CounterKeysSlice;

        public const string DbIds = "@dbIds";
        public const string Values = "@vals";

        private long _countersCount;

        private readonly List<ByteStringContext<ByteStringMemoryCache>.InternalScope> _counterModificationMemoryScopes = new List<ByteStringContext<ByteStringMemoryCache>.InternalScope>();

        private static readonly TableSchema CountersSchema = new TableSchema
        {
            TableType = (byte)TableType.Counters
        };

        private enum CountersTable
        {
            // Format of this is:
            // lower document id, record separator, prefix 
            CounterKey = 0,
            Etag = 1,         
            ChangeVector = 2,
            Data = 3,
            Collection = 4,
            TransactionMarker = 5
        }

        [StructLayout(LayoutKind.Explicit)]
        internal struct CounterValues
        {
            [FieldOffset(0)]
            public long Value;
            [FieldOffset(8)]
            public long Etag;
        }

        static CountersStorage()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "AllCountersEtags", ByteStringType.Immutable, out AllCountersEtagSlice);
                Slice.From(ctx, "CollectionCountersEtags", ByteStringType.Immutable, out CollectionCountersEtagsSlice);
                Slice.From(ctx, "CounterKeys", ByteStringType.Immutable, out CounterKeysSlice);
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

        public IEnumerable<CounterGroupDetail> GetCountersFrom(DocumentsOperationContext context, long etag, int skip, int take)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(CountersSchema.FixedSizeIndexes[AllCountersEtagSlice], etag, skip))
            {
                if (take-- <= 0)
                    yield break;

                yield return TableValueToCounterGroupDetail(context, result.Reader);
            }
        }

        public IEnumerable<CounterGroupDetail> GetCountersFrom(DocumentsOperationContext context, string collection, long etag, int skip, int take)
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

                yield return TableValueToCounterGroupDetail(context, result.Reader);
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

        public static CounterGroupDetail TableValueToCounterGroupDetail(JsonOperationContext context, TableValueReader tvr)
        {
            return new CounterGroupDetail
            {
                CounterKey = TableValueToString(context, (int)CountersTable.CounterKey, ref tvr),
                ChangeVector = TableValueToString(context, (int)CountersTable.ChangeVector, ref tvr),
                Etag = TableValueToEtag((int)CountersTable.Etag, ref tvr),
                Values = GetData(context, ref tvr)
            };
        }

        private static ReplicationBatchItem CreateReplicationBatchItem(DocumentsOperationContext context, Table.TableValueHolder tvh)
        {
            var data = GetData(context, ref tvh.Reader);
            return new ReplicationBatchItem
            {
                Type = ReplicationItemType.Counter,

                Id = TableValueToString(context, (int)CountersTable.CounterKey, ref tvh.Reader),
                ChangeVector = TableValueToChangeVector(context, (int)CountersTable.ChangeVector, ref tvh.Reader),
                Values = data,
                Collection = TableValueToId(context, (int)CountersTable.Collection, ref tvh.Reader),
                Etag = TableValueToEtag((int)CountersTable.Etag, ref tvh.Reader)
            };
        
        }

        public string IncrementCounter(DocumentsOperationContext context, string documentId, string collection, string name, long delta, out bool exists)
        {
            return PutOrIncrementCounter(context, documentId, collection, name, delta, out exists);
        }

        public string PutCounter(DocumentsOperationContext context, string documentId, string collection, string name, long delta)
        {
            return PutOrIncrementCounter(context, documentId, collection, name, delta, out _, overrideExisting: true);
        }

        public string PutOrIncrementCounter(DocumentsOperationContext context, string documentId, string collection, string name, long delta, out bool exists,
            bool overrideExisting = false)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false);// never hit
            }

            var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            var table = GetCountersTable(context.Transaction.InnerTransaction, collectionName);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out Slice counterKey, out _))
            {
                BlittableJsonReaderObject data = null;
                exists = false;
                var value = delta;
                if (table.ReadByKey(counterKey, out var existing))
                {
                    using (data = GetData(context, ref existing))
                    {
                        // Common case is that we modify the data IN PLACE
                        // as such, we must copy it before modification
                        data = data.Clone(context);
                    }
                }


                var newETag = _documentsStorage.GenerateNextEtag();

                if (data != null)
                {
                    var dbIdIndex = GetDbIdIndex(context, data);

                    data.TryGet(Values, out BlittableJsonReaderObject counters);


                    if (counters.TryGetMember(name, out object existingCounter) == false || 
                        existingCounter is LazyStringValue ||
                        !(existingCounter is BlittableJsonReaderObject.RawBlob blob) ||
                        overrideExisting)
                    {
                        CreateNewCounterOrOverrideExisting(context, name, dbIdIndex, value, newETag, counters, existingCounter != null);
                    }
                    else
                    {
                        IncrementExistingCounter(context, documentId, name, delta, ref exists, blob, dbIdIndex, newETag, counters, value);
                    }

                    if (counters.Modifications != null)
                    {
                        using (data)
                        {
                            data = context.ReadObject(data, null, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                        }
                    }
                }
                else
                {
                    // no counters at all
                    data = WriteNewCountersDocument(context, name, value, newETag);
                }

                var etag = _documentsStorage.GenerateNextEtag();
                var result = ChangeVectorUtils.TryUpdateChangeVector(_documentDatabase.ServerStore.NodeTag, _documentsStorage.Environment.Base64Id, etag, string.Empty);

                using (Slice.From(context.Allocator, result.ChangeVector, out var cv))
                using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(counterKey);
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

        private static int GetDbIdIndex(DocumentsOperationContext context, BlittableJsonReaderObject data)
        {
            var dbIdIndex = int.MaxValue;
            if (data.TryGet(DbIds, out BlittableJsonReaderArray dbIds))
            {
                for (dbIdIndex = 0; dbIdIndex < dbIds.Length; dbIdIndex++)
                {
                    if (dbIds[dbIdIndex].Equals(context.Environment.Base64Id) == false)
                        continue;

                    break;
                }

                if (dbIdIndex == dbIds.Length)
                {
                    dbIds.Modifications = new DynamicJsonArray {context.Environment.Base64Id};
                }
            }

            return dbIdIndex;
        }

        private void CreateNewCounterOrOverrideExisting(DocumentsOperationContext context, string name, int dbIdIndex, long value, long newETag,
            BlittableJsonReaderObject counters, bool overrideExisting = false)
        {
            using (context.Allocator.Allocate((dbIdIndex + 1) * SizeOfCounterValues, out var newVal))
            {
                if (dbIdIndex > 0) 
                {
                    Memory.Set(newVal.Ptr, 0, dbIdIndex * SizeOfCounterValues);
                }

                var newEntry = (CounterValues*)newVal.Ptr + dbIdIndex;

                newEntry->Value = value;
                newEntry->Etag = newETag;

                counters.Modifications = new DynamicJsonValue(counters)
                {
                    [name] = new BlittableJsonReaderObject.RawBlob
                    {
                        Length = newVal.Length,
                        Ptr = newVal.Ptr
                    }
                };
            }

            if (overrideExisting == false)
                _countersCount++;
        }

        private static void IncrementExistingCounter(DocumentsOperationContext context, string documentId, string name, long delta, ref bool exists, BlittableJsonReaderObject.RawBlob existingCounter,
            int dbIdIndex, long newETag, BlittableJsonReaderObject counters, long value)
        {
            var existingCount = existingCounter.Length / SizeOfCounterValues;

            if (dbIdIndex < existingCount)
            {
                exists = true;
                var counter = (CounterValues*)existingCounter.Ptr + dbIdIndex;
                try
                {
                    counter->Value = checked(counter->Value + delta); //inc
                    counter->Etag = newETag;
                }
                catch (OverflowException e)
                {
                    CounterOverflowException.ThrowFor(documentId, name, counter->Value, delta, e);
                }
            }
            else
            {
                // counter exists , but not with local DbId

                using (AddPartialValueToExistingCounter(context, existingCounter, dbIdIndex, value, newETag))
                {
                    counters.Modifications = new DynamicJsonValue(counters)
                    {
                        [name] = existingCounter
                    };
                }
            }
        }

        private BlittableJsonReaderObject WriteNewCountersDocument(DocumentsOperationContext context, string name, long value, long newETag)
        {
            _countersCount++;

            BlittableJsonReaderObject data;
            using (context.Allocator.Allocate(SizeOfCounterValues, out var newVal))
            {
                var newEntry = (CounterValues*)newVal.Ptr;
                newEntry->Value = value;
                newEntry->Etag = newETag;

                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();

                    builder.WritePropertyName(DbIds);

                    builder.StartWriteArray();
                    builder.WriteValue(context.Environment.Base64Id);
                    builder.WriteArrayEnd();

                    builder.WritePropertyName(Values);
                    builder.StartWriteObject();

                    builder.WritePropertyName(name);
                    builder.WriteRawBlob(newVal.Ptr, newVal.Length);

                    builder.WriteObjectEnd();

                    builder.WriteObjectEnd();
                    builder.FinalizeDocument();

                    data = builder.CreateReader();
                }
            }

            return data;
        }

        public void PutCounters(DocumentsOperationContext context, string documentId, string collection, string changeVector,
            BlittableJsonReaderObject sourceData)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                return;
            }

            try
            {
                var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
                var table = GetCountersTable(context.Transaction.InnerTransaction, collectionName);

                BlittableJsonReaderObject data = null;
                sourceData.TryGet(Values, out BlittableJsonReaderObject sourceCounters);

                using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, documentId, out Slice counterKey, out _))
                {
                    using (table.Allocate(out TableValueBuilder tvb))
                    {
                        if (changeVector != null && table.ReadByKey(counterKey, out var existing))
                        {
                            var existingChangeVector = TableValueToChangeVector(context, (int)CountersTable.ChangeVector, ref existing);

                            if (ChangeVectorUtils.GetConflictStatus(changeVector, existingChangeVector) == ConflictStatus.AlreadyMerged)
                                return;

                            data = GetData(context, ref existing);
                            data = data.Clone(context);
                            data.TryGet(DbIds, out BlittableJsonReaderArray dbIds);
                            data.TryGet(Values, out BlittableJsonReaderObject localCounters);

                            var localDbIdsList = DbIdsToList(dbIds);

                            sourceData.TryGet(DbIds, out BlittableJsonReaderArray sourceDbIds);


                            var prop = new BlittableJsonReaderObject.PropertyDetails();
                            for (var i = 0; i< sourceCounters.Count; i++)
                            {
                                sourceCounters.GetPropertyByIndex(i, ref prop);

                                var counterName = prop.Name;
                                var modified = false;
                                var changeType = CounterChangeTypes.Put;
                                long value = 0;

                                LazyStringValue deletedLocalCounter = null;
                                BlittableJsonReaderObject.RawBlob loaclCounterValues = null;

                                if (localCounters.TryGet(counterName, out object existingCounter))
                                {
                                    if (existingCounter is LazyStringValue lsv)
                                    {
                                        deletedLocalCounter = lsv;
                                    }
                                    else
                                    {
                                        loaclCounterValues = existingCounter as BlittableJsonReaderObject.RawBlob;
                                    }
                                }


                                if (prop.Value is LazyStringValue deletedSourceCounter)
                                {
                                    if (deletedLocalCounter != null)
                                    {
                                        // delete + delete => merger change vectors

                                        var mergedCv = ChangeVectorUtils.MergeVectors(deletedLocalCounter, deletedSourceCounter);
                                        // todo add local dbId+etag to deleted change vector

                                        localCounters.Modifications = localCounters.Modifications ?? new DynamicJsonValue(localCounters);
                                        localCounters.Modifications[counterName] = mergedCv;
                                        continue;

                                    }
                                    if (loaclCounterValues != null)
                                    {
                                        // delete + blob => resolve conflict

                                        using (var localValuesAsCv = RawBlobToChangeVector(context, data, loaclCounterValues, existingChangeVector))
                                        {
                                            var conflictStatus = ChangeVectorUtils.GetConflictStatus(deletedSourceCounter, localValuesAsCv);

                                            switch (conflictStatus)
                                            {
                                                case ConflictStatus.Update:
                                                    //delete is more up do date
                                                    modified = true;
                                                    changeType = CounterChangeTypes.Delete;
                                                    localCounters.Modifications = localCounters.Modifications ?? new DynamicJsonValue(localCounters);
                                                    localCounters.Modifications[counterName] = deletedSourceCounter;
                                                    break;
                                                case ConflictStatus.Conflict:
                                                // conflict => resolve to raw blob (no change)
                                                case ConflictStatus.AlreadyMerged:
                                                    // raw blob is more up to date (no change)
                                                    continue;
                                            }
                                        }                                       
                                    }
                                    else
                                    {
                                        // put deleted counter

                                        modified = true;
                                        changeType = CounterChangeTypes.Delete;
                                        localCounters.Modifications = localCounters.Modifications ?? new DynamicJsonValue(localCounters);
                                        // todo add local dbId + etag ? 
                                        localCounters.Modifications[counterName] = deletedSourceCounter;
                                        _countersCount--;
                                    }
                                }

                                if (prop.Value is BlittableJsonReaderObject.RawBlob sourceBlob)
                                {
                                    if (deletedLocalCounter != null)
                                    {
                                        // blob + delete => resolve conflict

                                        var sourceValuesAsCv= RawBlobToChangeVector(context, data, sourceBlob, changeVector);

                                        var conflictStatus = ChangeVectorUtils.GetConflictStatus(sourceValuesAsCv, deletedLocalCounter);

                                        sourceValuesAsCv.Dispose();

                                        switch (conflictStatus)
                                        {
                                            case ConflictStatus.Update:
                                                //delete is more up do date (no change)
                                                continue;
                                            case ConflictStatus.Conflict: 
                                                // conflict => resolve to raw blob 
                                            case ConflictStatus.AlreadyMerged:
                                                // raw blob is more up to date => put counter

                                                _countersCount++;
                                                loaclCounterValues = new BlittableJsonReaderObject.RawBlob();
                                                break;
                                        }


                                    }
                                    else if (loaclCounterValues == null)
                                    {
                                        // put new counter

                                        _countersCount++;
                                        loaclCounterValues = new BlittableJsonReaderObject.RawBlob();
                                    }

                                    // blob + blob => merge
                                    value = InternalPutCounter(context, localCounters, counterName, dbIds, sourceDbIds, localDbIdsList, loaclCounterValues, sourceBlob, out modified);

                                }


                                if (modified == false)
                                    continue;

                                context.Transaction.AddAfterCommitNotification(new CounterChange
                                {
                                    ChangeVector = changeVector,
                                    DocumentId = documentId,
                                    Name = counterName,
                                    Value = value,
                                    Type = changeType
                                });

                                UpdateMetrics(counterKey, counterName, changeVector, collection);
                            }

                            if (localCounters.Modifications != null)
                            {
                                using (data)
                                {
                                    data = context.ReadObject(data, null, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                                }
                            }
                        }

                        if (data == null)
                        {
                            data = context.ReadObject(sourceData, null, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                            _countersCount += sourceCounters?.Count ?? 0;
                        }

                        var etag = _documentsStorage.GenerateNextEtag();

                    if (changeVector == null)
                    {
                        changeVector = ChangeVectorUtils.NewChangeVector(_documentDatabase.ServerStore.NodeTag, etag, _documentsStorage.Environment.Base64Id);
                        context.LastDatabaseChangeVector =
                            ChangeVectorUtils.MergeVectors(context.LastDatabaseChangeVector ?? GetDatabaseChangeVector(context), changeVector);
                    }

                        using (Slice.From(context.Allocator, changeVector, out var cv))
                        using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
                        {
                            tvb.Add(counterKey);
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
            finally 
            {
                foreach (var s in _counterModificationMemoryScopes)
                {
                    s.Dispose();
                }
                _counterModificationMemoryScopes.Clear();              
            }
        }

        private static List<LazyStringValue> DbIdsToList(BlittableJsonReaderArray dbIds)
        {
            var localDbIdsList = new List<LazyStringValue>(dbIds.Length);
            for (int i = 0; i < dbIds.Length; i++)
            {
                localDbIdsList.Add((LazyStringValue)dbIds[i]);
            }

            return localDbIdsList;
        }

        private long InternalPutCounter(DocumentsOperationContext context, BlittableJsonReaderObject counters, string counterName,
            BlittableJsonReaderArray localDbIds, BlittableJsonReaderArray sourceDbIds, List<LazyStringValue> localDbIdsList, 
            BlittableJsonReaderObject.RawBlob existingCounter, BlittableJsonReaderObject.RawBlob source, out bool modified)
        {
            long value = 0;
            var existingCount = existingCounter.Length / SizeOfCounterValues;
            var sourceCount = source.Length / SizeOfCounterValues;
            modified = false;

            for (var index = 0; index < sourceCount; index++)
            {
                var sourceDbId = (LazyStringValue)sourceDbIds[index];
                var sourceValue = ((CounterValues*)source.Ptr)[index];

                int localDbIdIndex = GetOrAddDbIdIndex(localDbIds, localDbIdsList, sourceDbId);

                if (localDbIdIndex < existingCount)
                {
                    var localValuePtr = (CounterValues*)existingCounter.Ptr + localDbIdIndex;
                    if (localValuePtr->Etag >= sourceValue.Etag ||
                        sourceDbId.Equals(context.Environment.Base64Id))
                    {
                        value += localValuePtr->Value;
                        continue;
                    }

                    localValuePtr->Value = sourceValue.Value;
                    localValuePtr->Etag = sourceValue.Etag;

                    value += sourceValue.Value;
                    continue;
                }

                // counter doesn't have this dbId
                modified = true;
                value += sourceValue.Value;
                var scope = AddPartialValueToExistingCounter(context, existingCounter, localDbIdIndex, sourceValue.Value, sourceValue.Etag);
                _counterModificationMemoryScopes.Add(scope);

                existingCount = existingCounter.Length / SizeOfCounterValues;
            }

            if (modified)
            {
                counters.Modifications = counters.Modifications ?? new DynamicJsonValue(counters);
                counters.Modifications[counterName] = existingCounter;
            }

            return value;
        }

        private static int GetOrAddDbIdIndex(BlittableJsonReaderArray localDbIds, List<LazyStringValue> localDbIdsList, LazyStringValue dbId)
        {
            int dbIdIndex;
            for (dbIdIndex = 0; dbIdIndex < localDbIdsList.Count; dbIdIndex++)
            {
                var current = localDbIdsList[dbIdIndex];
                if (current.Equals(dbId) == false)
                    continue;
                break;
            }

            if (dbIdIndex == localDbIdsList.Count)
            {
                localDbIdsList.Add(dbId);
                localDbIds.Modifications = localDbIds.Modifications ?? new DynamicJsonArray();
                localDbIds.Modifications.Add(dbId);
            }

            return dbIdIndex;
        }

        private static ByteStringContext<ByteStringMemoryCache>.InternalScope AddPartialValueToExistingCounter(DocumentsOperationContext context, 
            BlittableJsonReaderObject.RawBlob existingCounter, int dbIdIndex, long sourceValue, long sourceEtag)
        {
            var scope = context.Allocator.Allocate((dbIdIndex + 1) * SizeOfCounterValues, out var newVal);

            Memory.Copy(newVal.Ptr, existingCounter.Ptr, existingCounter.Length);
            var empties = dbIdIndex - existingCounter.Length / SizeOfCounterValues;
            if (empties > 0)
            {
                Memory.Set(newVal.Ptr + existingCounter.Length, 0, empties * SizeOfCounterValues);
            }
;
            var newEntry = (CounterValues*)newVal.Ptr + dbIdIndex;
            newEntry->Value = sourceValue;
            newEntry->Etag = sourceEtag;

            existingCounter.Ptr = newVal.Ptr;
            existingCounter.Length = newVal.Length;

            return scope;

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

        private HashSet<string> _tableCreated = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public static int SizeOfCounterValues = sizeof(CounterValues);

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
            {
                if (table.ReadByKey(key, out var existing) == false)
                    yield break;

                var data = GetData(context, ref existing);
                data.TryGet(Values, out BlittableJsonReaderObject counters);

                var prop = new BlittableJsonReaderObject.PropertyDetails();
                for (var i=0; i < counters.Count; i++)
                {
                    counters.GetPropertyByIndex(i, ref prop);
                    if (prop.Value is LazyStringValue)
                        continue; //deleted

                    yield return prop.Name;
                }
            }
        }

        private static BlittableJsonReaderObject GetData(JsonOperationContext context, ref TableValueReader existing)
        {
            return new BlittableJsonReaderObject(existing.Read((int)CountersTable.Data, out int oldSize), oldSize, context);
        }

        public long? GetCounterValue(DocumentsOperationContext context, string docId, string counterName)
        {
            if (TryGetRawBlob(context, docId, counterName, out var blob) == false)
                return null;

            var existingCount = blob.Length / SizeOfCounterValues;

            long value = 0;
            for (var i = 0; i < existingCount; i++)
            {
                value += GetPartialValue(i, blob);
            }

            return value;
        }

        private static bool TryGetRawBlob(DocumentsOperationContext context, string docId, string counterName, out BlittableJsonReaderObject.RawBlob blob)
        {
            blob = null;
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, docId, out Slice key, out _))
            {
                if (table.ReadByKey(key, out var tvr) == false)
                    return false;

                var data = GetData(context, ref tvr);
                if (data.TryGet(Values, out BlittableJsonReaderObject counters) == false ||
                    counters.TryGetMember(counterName, out object counterValues) == false ||
                    counterValues is LazyStringValue || // deleted
                    !(counterValues is BlittableJsonReaderObject.RawBlob counterValuesAsRawBlob))
                    return false;

                blob = counterValuesAsRawBlob;
                return true;
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
                if (data.TryGet(DbIds, out BlittableJsonReaderArray dbIds) == false ||
                    data.TryGet(Values, out BlittableJsonReaderObject counters) == false ||
                    counters.TryGetMember(counterName, out object counterValues) == false ||
                    counterValues is LazyStringValue || // deleted
                    !(counterValues is BlittableJsonReaderObject.RawBlob blob))
                    yield break;

                var existingCount = blob.Length / SizeOfCounterValues;
                

                for (var dbIdIndex = 0; dbIdIndex < existingCount; dbIdIndex ++)
                {
                    var val = GetPartialValue(dbIdIndex, blob);
                    yield return (dbIds[dbIdIndex].ToString(), val);
                }
            }
        }

        internal static long GetPartialValue(int index, BlittableJsonReaderObject.RawBlob counterValues)
        {
            return ((CounterValues*)counterValues.Ptr)[index].Value;
        }

        internal CounterGroupDetail GetCounterValuesForDocument(DocumentsOperationContext context, string docId)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, docId, out Slice key, out _))
            {
                if (table.ReadByKey(key, out var existing) == false)
                    return null;

                return TableValueToCounterGroupDetail(context, existing);
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
                //todo update Count of Counters
                table.DeleteByKey(lowerId /*, holder => _countersCount -= GetData(context, ref holder.Reader).Count -1*/); 
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
                    return null;
                
                var data = GetData(context, ref existing);

                if (data.TryGet(Values, out BlittableJsonReaderObject counters) == false ||
                    counters.TryGetMember(counterName, out object counterToDelete) == false)                
                    return null;

                if (counterToDelete is LazyStringValue || // already deleted
                    !(counterToDelete is BlittableJsonReaderObject.RawBlob blob))
                    return null;
                var oldCv = TableValueToString(context, (int)CountersTable.ChangeVector, ref existing);

                RemoveCounterFromBlittableAndUpdateTable(context, counterName, data, counters, blob, collectionName, table, lowerId, oldCv, out var cv);
                _countersCount--;

                return cv;


            }
        }

        private void RemoveCounterFromBlittableAndUpdateTable(DocumentsOperationContext context, string counterName, BlittableJsonReaderObject data,
            BlittableJsonReaderObject counters, BlittableJsonReaderObject.RawBlob counterToDelete, CollectionName collectionName, Table table,
            Slice lowerId, string oldChangeVector, out string newChangeVector)
        {
            var newEtag = _documentsStorage.GenerateNextEtag();
            using (var cvLsv = RawBlobToChangeVector(context, data, counterToDelete, oldChangeVector, true))
            {
                counters.Modifications = new DynamicJsonValue(counters)
                {
                    // [counterName] = sb
                    [counterName] = cvLsv
                };


                using (data)
                {
                    data = context.ReadObject(data, null, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                }
            }

            newChangeVector = ChangeVectorUtils.NewChangeVector(_documentDatabase.ServerStore.NodeTag, newEtag, _documentsStorage.Environment.Base64Id);
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

        private static LazyStringValue RawBlobToChangeVector(DocumentsOperationContext context, BlittableJsonReaderObject data, BlittableJsonReaderObject.RawBlob counterToDelete, string changeVector,
            bool addLocal = false)
        {
            data.TryGet(DbIds, out BlittableJsonReaderArray dbIds);
            var dbIdIndex = GetDbIdIndex(context, data);
            var count = counterToDelete.Length / SizeOfCounterValues;
            var sb = new StringBuilder();

            for (int i = 0; i < count; i++)
            {
                if (i > 0)
                {
                    sb.Append(", ");
                }
                if (i != dbIdIndex)
                {
                    var etag = ((CounterValues*)counterToDelete.Ptr + i)->Etag;
                    var nodeTag = ChangeVectorUtils.GetNodeTagById(changeVector, dbIds[i].ToString());

                    sb.Append(nodeTag ?? "Z")
                        .Append(":")
                        .Append(etag)
                        .Append("-")
                        .Append(dbIds[i]);

                    continue;
                }

                AddLocalNodeToChangeVector(context, sb);

            }


            if (count < dbIdIndex && addLocal)
            {
                AddLocalNodeToChangeVector(context, sb);
            }

            /*            var size = Encoding.UTF8.GetMaxByteCount(sb.Length);
                        var cvMem = context.GetMemory(size);
                        var span = new Span<char>(cvMem.Address, sb.Length);
                        sb.CopyTo(0, span, sb.Length);

                        return new LazyStringValue(null, cvMem.Address, sb.Length, context);*/

            var mem = context.GetMemory(sb.Length);
            return new LazyStringValue(sb.ToString(), mem.Address, sb.Length, context);

        }

        private static void AddLocalNodeToChangeVector(DocumentsOperationContext context, StringBuilder sb)
        {
            var newEtag = context.DocumentDatabase.DocumentsStorage.GenerateNextEtag();
            var localNodeTag = context.DocumentDatabase.ServerStore.NodeTag;
            sb.Append(localNodeTag)
                .Append(":")
                .Append(newEtag)
                .Append("-")
                .Append(context.Environment.Base64Id);
        }

        private static (LazyStringValue Doc, LazyStringValue Name) ExtractDocIdAndName(JsonOperationContext context, Slice counterKey)
        {
            var p = counterKey.Content.Ptr;
            var size = counterKey.Size;
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
        [Conditional("DEBUG")]        public static void AssertCounters(BlittableJsonReaderObject document, DocumentFlags flags)
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

        public long GetNumberOfCounterEntries()
        {
            return _countersCount;
        }

        public string UpdateDocumentCounters(DocumentsOperationContext context, Document doc, string docId,
            SortedSet<string> countersToAdd, HashSet<string> countersToRemove, NonPersistentDocumentFlags nonPersistentDocumentFlags)
        {
            if (countersToRemove.Count == 0 && countersToAdd.Count == 0)
                return null;

            var data = doc.Data;
            BlittableJsonReaderArray metadataCounters = null;
            if (data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
            {
                metadata.TryGet(Constants.Documents.Metadata.Counters, out metadataCounters);
            }

            var counters = GetCountersForDocument(metadataCounters, countersToAdd, countersToRemove, out var hadModifications);
            if (hadModifications == false)
                return null;

            var flags = doc.Flags.Strip(DocumentFlags.FromClusterTransaction | DocumentFlags.Resolved);
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
            using (data)
			{
            	var newDocumentData = context.ReadObject(doc.Data, docId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            	var putResult = _documentDatabase.DocumentsStorage.Put(context, docId, null, newDocumentData, flags: flags, nonPersistentFlags: nonPersistentDocumentFlags);
            	return putResult.ChangeVector;
			}
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

        public static void ConvertFromBlobToNumbers(JsonOperationContext context, CounterGroupDetail counterGroupDetail)
        {
            counterGroupDetail.Values.TryGet(Values, out BlittableJsonReaderObject counters);
            counters.Modifications = new DynamicJsonValue(counters);

            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (int i = 0; i < counters.Count; i++)
            {
                counters.GetPropertyByIndex(i, ref prop);

                var dja = new DynamicJsonArray();
                var blob = (BlittableJsonReaderObject.RawBlob)prop.Value;
                var existingCount = blob.Length / SizeOfCounterValues;

                for (int dbIdIndex = 0; dbIdIndex < existingCount; dbIdIndex++)
                {
                    var current = (CounterValues*)blob.Ptr + dbIdIndex;

                    dja.Add(current->Value);
                    dja.Add(current->Etag);
                }

                counters.Modifications[prop.Name] = dja;
            }

            using (counterGroupDetail.Values)
            {
                counterGroupDetail.Values = context.ReadObject(counterGroupDetail.Values, null);
            }
        }

    }
}

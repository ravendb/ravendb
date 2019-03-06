using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using Sparrow;
using Sparrow.Binary;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.Documents.Replication.ReplicationBatchItem;
using Raven.Server.Utils;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Exceptions.Documents.Counters;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents
{
    public unsafe class CountersStorage
    {
        public const int DbIdAsBase64Size = 22;

        public const int MaxCounterDocumentSize = 128;

        private readonly DocumentDatabase _documentDatabase;
        private readonly DocumentsStorage _documentsStorage;

        public static readonly Slice AllCountersEtagSlice;
        private static readonly Slice CollectionCountersEtagsSlice;
        private static readonly Slice CounterKeysSlice;

        public const string DbIds = "@dbIds";
        public const string Values = "@vals";

        private readonly List<ByteStringContext<ByteStringMemoryCache>.InternalScope> _counterModificationMemoryScopes =
            new List<ByteStringContext<ByteStringMemoryCache>.InternalScope>();

        public static int SizeOfCounterValues = sizeof(CounterValues);

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

        internal struct DbIdsHolder
        {
            private BlittableJsonReaderArray _dbIdsBlittableArray;
            public readonly List<LazyStringValue> dbIdsList;

            public DbIdsHolder(BlittableJsonReaderArray dbIds)
            {
                _dbIdsBlittableArray = dbIds;
                dbIdsList = DbIdsToList(dbIds);
            }

            public int GetOrAddDbIdIndex(LazyStringValue dbId)
            {
                int dbIdIndex;
                for (dbIdIndex = 0; dbIdIndex < dbIdsList.Count; dbIdIndex++)
                {
                    var current = dbIdsList[dbIdIndex];
                    if (current.Equals(dbId))
                        break;
                }

                if (dbIdIndex == dbIdsList.Count)
                {
                    dbIdsList.Add(dbId);
                    _dbIdsBlittableArray.Modifications = _dbIdsBlittableArray.Modifications ?? new DynamicJsonArray();
                    _dbIdsBlittableArray.Modifications.Add(dbId);
                }

                return dbIdIndex;
            }

        }

        static CountersStorage()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "AllCounterGroupsEtags", ByteStringType.Immutable, out AllCountersEtagSlice);
                Slice.From(ctx, "CollectionCounterGroupsEtags", ByteStringType.Immutable, out CollectionCountersEtagsSlice);
                Slice.From(ctx, "CounterGroupKeys", ByteStringType.Immutable, out CounterKeysSlice);
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

                yield return TableValueToCounterGroupDetail(context, ref result.Reader);
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

                yield return TableValueToCounterGroupDetail(context, ref result.Reader);
            }
        }

        public long GetNumberOfCounterGroupsToProcess(DocumentsOperationContext context, string collection, long afterEtag, out long totalCount)
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

        public static CounterGroupDetail TableValueToCounterGroupDetail(JsonOperationContext context, ref TableValueReader tvr)
        {
            var docId = ExtractDocId(context, ref tvr);

            return new CounterGroupDetail
            {
                DocumentId = docId,
                ChangeVector = TableValueToString(context, (int)CountersTable.ChangeVector, ref tvr),
                Etag = TableValueToEtag((int)CountersTable.Etag, ref tvr),
                Values = GetCounterValuesData(context, ref tvr)
            };
        }

        private static ReplicationBatchItem CreateReplicationBatchItem(DocumentsOperationContext context, Table.TableValueHolder tvh)
        {
            var data = GetCounterValuesData(context, ref tvh.Reader);
            var docId = ExtractDocId(context, ref tvh.Reader);

            return new ReplicationBatchItem
            {
                Type = ReplicationItemType.CounterGroup,
                Id = docId,
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
                Debug.Assert(false); // never hit
            }

            try
            {
                var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
                var table = GetCountersTable(context.Transaction.InnerTransaction, collectionName);

                ByteStringContext.InternalScope countersGroupKeyScope = default;
                using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice documentKeyPrefix, separator: SpecialChars.RecordSeparator))
                using (Slice.From(context.Allocator, name, out Slice counterName))
                using (context.Allocator.Allocate(documentKeyPrefix.Size + counterName.Size, out var counterKeyBuffer))
                using (CreateCounterKeySlice(context, counterKeyBuffer, documentKeyPrefix, counterName, out var counterKeySlice))
                {

                    BlittableJsonReaderObject data;
                    exists = false;
                    var value = delta;

                    Slice countersGroupKey;
                    if (table.SeekOneBackwardByPrimaryKeyPrefix(documentKeyPrefix, counterKeySlice, out var existing))
                    {
                        countersGroupKeyScope = Slice.From(context.Allocator, existing.Read((int)CountersTable.CounterKey, out var size), size, out countersGroupKey);

                        using (data = GetCounterValuesData(context, ref existing))
                        {
                            // Common case is that we modify the data IN PLACE
                            // as such, we must copy it before modification
                            data = data.Clone(context);
                        }

                        if (data.TryGet(DbIds, out BlittableJsonReaderArray dbIds) == false)
                            throw new InvalidDataException($"Counter-Group document '{counterKeySlice}' is missing '{DbIds}' property. Shouldn't happen");

                        var dbIdIndex = GetOrAddLocalDbIdIndex(dbIds);

                        if (data.TryGet(Values, out BlittableJsonReaderObject counters) == false)
                        {
                            throw new InvalidDataException($"Counter-Group document '{counterKeySlice}' is missing '{Values}' property. Shouldn't happen");
                        }

                        var counterEtag = _documentsStorage.GenerateNextEtag();

                        counters.TryGetMember(name, out object existingCounter);

                        if (existingCounter is BlittableJsonReaderObject.RawBlob blob &&
                            overrideExisting == false)
                        {
                            exists = IncrementExistingCounter(context, documentId, name, delta,
                                blob, dbIdIndex, counterEtag, counters, ref value);
                        }
                        else
                        {
                            if (existingCounter == null &&
                                data.Size + sizeof(long) * 3 > MaxCounterDocumentSize &&
                                counters.Count > 1)
                            {
                                // we now need to add a new counter to the counters blittable
                                // and adding it will cause to grow beyond 2KB (the 24bytes is an 
                                // estimate, we don't actually depend on hard 2KB limit).

                                var existingChangeVector = TableValueToChangeVector(context, (int)CountersTable.ChangeVector, ref existing);
                                SplitCounterGroup(context, collectionName, table, documentKeyPrefix, countersGroupKey, counters, dbIds, existingChangeVector, ref existing);

                                // now we retry and know that we have enough space
                                return PutOrIncrementCounter(context, documentId, collection, name, delta, out exists, overrideExisting);
                            }

                            CreateNewCounterOrOverrideExisting(context, name, dbIdIndex, value, counterEtag, counters);
                        }

                        if (counters.Modifications != null)
                        {
                            using (var old = data)
                            {
                                data = context.ReadObject(data, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                            }
                        }
                    }
                    else
                    {
                        data = WriteNewCountersDocument(context, name, value);
                        countersGroupKey = documentKeyPrefix;
                    }

                    var groupEtag = _documentsStorage.GenerateNextEtag();
                    var databaseChangeVector = context.LastDatabaseChangeVector ?? GetDatabaseChangeVector(context);
                    var changeVector = ChangeVectorUtils.TryUpdateChangeVector(_documentDatabase, databaseChangeVector, groupEtag).ChangeVector;
                    context.LastDatabaseChangeVector = changeVector;

                    using (countersGroupKeyScope)
                    {
                        using (Slice.From(context.Allocator, changeVector, out var cv))
                        using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
                        using (table.Allocate(out TableValueBuilder tvb))
                        {
                            tvb.Add(countersGroupKey);
                            tvb.Add(Bits.SwapBytes(groupEtag));
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

                        UpdateMetrics(countersGroupKey, name, changeVector, collection);
                    }

                    context.Transaction.AddAfterCommitNotification(new CounterChange
                    {
                        ChangeVector = changeVector,
                        DocumentId = documentId,
                        Name = name,
                        Type = exists ? CounterChangeTypes.Increment : CounterChangeTypes.Put,
                        Value = value
                    });

                    return changeVector;
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

        private void SplitCounterGroup(DocumentsOperationContext context, CollectionName collectionName, Table table, Slice documentKeyPrefix, Slice countersGroupKey, BlittableJsonReaderObject values, BlittableJsonReaderArray dbIds, string changeVector, ref TableValueReader existing)
        {
            var (fst, snd) = SplitCounterDocument(context, values, dbIds);

            using (Slice.From(context.Allocator, changeVector, out var cv))
            using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
            {
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(countersGroupKey);
                    tvb.Add(Bits.SwapBytes(_documentsStorage.GenerateNextEtag()));
                    tvb.Add(cv);
                    tvb.Add(fst.BasePointer, fst.Size);
                    tvb.Add(collectionSlice);
                    tvb.Add(context.TransactionMarkerOffset);
                    table.Update(existing.Id, tvb);
                }

                fst.TryGet(Values, out BlittableJsonReaderObject fstValues);
                snd.TryGet(Values, out BlittableJsonReaderObject sndValues);
                BlittableJsonReaderObject.PropertyDetails firstPropertySnd = default, lastPropertyFirst = default;
                sndValues.GetPropertyByIndex(0, ref firstPropertySnd);
                fstValues.GetPropertyByIndex(fstValues.Count - 1, ref lastPropertyFirst);

                var firstChange = 0;
                for (; firstChange < lastPropertyFirst.Name.Length; firstChange++)
                {
                    if (firstPropertySnd.Name[firstChange] != lastPropertyFirst.Name[firstChange])
                        break;
                }

                using (context.Allocator.Allocate(documentKeyPrefix.Size + firstChange + 1, out ByteString newCounterKey))
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    documentKeyPrefix.CopyTo(newCounterKey.Ptr);
                    Memory.Copy(newCounterKey.Ptr + documentKeyPrefix.Size, firstPropertySnd.Name.Buffer, firstChange + 1);

                    tvb.Add(newCounterKey);
                    tvb.Add(Bits.SwapBytes(_documentsStorage.GenerateNextEtag()));
                    tvb.Add(cv);
                    tvb.Add(snd.BasePointer, snd.Size);
                    tvb.Add(collectionSlice);
                    tvb.Add(context.TransactionMarkerOffset);
                    table.Insert(tvb);
                }
            }
        }

        private int GetOrAddLocalDbIdIndex(BlittableJsonReaderArray dbIds)
        {
            int dbIdIndex;
            for (dbIdIndex = 0; dbIdIndex < dbIds.Length; dbIdIndex++)
            {
                if (dbIds[dbIdIndex].Equals(_documentDatabase.DbBase64Id))
                    break;
            }

            if (dbIdIndex == dbIds.Length)
            {
                dbIds.Modifications = new DynamicJsonArray
                {
                    _documentDatabase.DbBase64Id
                };
            }

            return dbIdIndex;
        }

        private void CreateNewCounterOrOverrideExisting(DocumentsOperationContext context, string name, int dbIdIndex, long value, long newETag,
            BlittableJsonReaderObject counters)
        {
            var scope = context.Allocator.Allocate((dbIdIndex + 1) * SizeOfCounterValues, out var newVal);
            _counterModificationMemoryScopes.Add(scope);

            Memory.Set(newVal.Ptr, 0, dbIdIndex * SizeOfCounterValues);
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

        private bool IncrementExistingCounter(DocumentsOperationContext context, string documentId, string name, long delta,
            BlittableJsonReaderObject.RawBlob existingCounter,
            int dbIdIndex, long newETag, BlittableJsonReaderObject counters, ref long value)
        {
            var existingCount = existingCounter.Length / SizeOfCounterValues;

            if (dbIdIndex < existingCount)
            {
                var counter = (CounterValues*)existingCounter.Ptr + dbIdIndex;
                try
                {
                    value = checked(counter->Value + delta); //inc
                    counter->Value = value;
                    counter->Etag = newETag;
                }
                catch (OverflowException e)
                {
                    CounterOverflowException.ThrowFor(documentId, name, counter->Value, delta, e);
                }

                return true;
            }

            // counter exists , but not with local DbId

            existingCounter = AddPartialValueToExistingCounter(context, existingCounter, dbIdIndex, value, newETag);

            counters.Modifications = new DynamicJsonValue(counters)
            {
                [name] = existingCounter
            };

            return false;
        }

        private BlittableJsonReaderObject WriteNewCountersDocument(DocumentsOperationContext context, string name, long value)
        {
            var counterEtag = _documentsStorage.GenerateNextEtag();
            BlittableJsonReaderObject data;

            using (context.Allocator.Allocate(SizeOfCounterValues, out var newVal))
            {
                var newEntry = (CounterValues*)newVal.Ptr;
                newEntry->Value = value;
                newEntry->Etag = counterEtag;

                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    context.CachedProperties.NewDocument();
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();

                    builder.WritePropertyName(DbIds);

                    builder.StartWriteArray();
                    builder.WriteValue(_documentDatabase.DbBase64Id);
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

        private (BlittableJsonReaderObject First, BlittableJsonReaderObject Second) SplitCounterDocument(DocumentsOperationContext context, BlittableJsonReaderObject values, BlittableJsonReaderArray dbIds)
        {
            // here we rely on the internal sort order of the blittables, because we go through them
            // in lexical order
            var fst = CreateHalfDocument(context, values, 0, values.Count / 2, dbIds);
            var snd = CreateHalfDocument(context, values, values.Count / 2, values.Count, dbIds);

            return (fst, snd);
        }

        private static BlittableJsonReaderObject CreateHalfDocument(DocumentsOperationContext context, BlittableJsonReaderObject values, int start, int end, BlittableJsonReaderArray dbIds)
        {
            BlittableJsonReaderObject.PropertyDetails prop = default;
            using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
            {
                context.CachedProperties.NewDocument();
                builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                builder.StartWriteObjectDocument();

                builder.StartWriteObject();

                builder.WritePropertyName(DbIds);

                builder.StartWriteArray();

                foreach (LazyStringValue item in dbIds)
                {
                    builder.WriteValue(item);
                }

                builder.WriteArrayEnd();

                builder.WritePropertyName(Values);
                builder.StartWriteObject();

                for (int i = start; i < end; i++)
                {
                    values.GetPropertyByIndex(i, ref prop);

                    builder.WritePropertyName(prop.Name);
                    if (prop.Value is BlittableJsonReaderObject.RawBlob blob)
                        builder.WriteRawBlob(blob.Ptr, blob.Length);
                    else if (prop.Value is LazyStringValue lsv)
                        builder.WriteValue(lsv);
                    else
                        throw new InvalidDataException("Unknown type: " + prop.Token + " when trying to split counter doc");
                }

                builder.WriteObjectEnd();

                builder.WriteObjectEnd();
                builder.FinalizeDocument();

                return builder.CreateReader();
            }
        }

        public void PutCounters(DocumentsOperationContext context, string documentId, string collection, string changeVector,
            BlittableJsonReaderObject sourceData)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                return;
            }

            var canSkip = new Dictionary<string, bool>();

            try
            {
                var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
                var table = GetCountersTable(context.Transaction.InnerTransaction, collectionName);

                using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice documentKeyPrefix, separator: SpecialChars.RecordSeparator))
                {
                    if (sourceData.TryGet(Values, out BlittableJsonReaderObject sourceCounters) == false)
                    {
                        throw new InvalidDataException($"Remote Counter-Group document '{documentId}' is missing '{Values}' property. Shouldn't happen");
                    }

                    {
                        BlittableJsonReaderObject data;

                        if (table.SeekOnePrimaryKeyPrefix(documentKeyPrefix, out _) == false)
                        {
                            data = context.ReadObject(sourceData, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                            using (Slice.From(context.Allocator, changeVector, out var cv))
                            using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
                            using (table.Allocate(out TableValueBuilder tvb))
                            {
                                tvb.Add(documentKeyPrefix);
                                tvb.Add(Bits.SwapBytes(_documentsStorage.GenerateNextEtag()));
                                tvb.Add(cv);
                                tvb.Add(data.BasePointer, data.Size);
                                tvb.Add(collectionSlice);
                                tvb.Add(context.TransactionMarkerOffset);

                                table.Set(tvb);
                            }
                        }

                        else
                        {
                            if (sourceData.TryGet(DbIds, out BlittableJsonReaderArray sourceDbIds) == false)
                            {
                                throw new InvalidDataException($"Remote Counter-Group document is missing '{DbIds}' property. Shouldn't happen");
                            }

                            var prop = new BlittableJsonReaderObject.PropertyDetails();
                            for (var i = 0; i < sourceCounters.Count; i++)
                            {
                                sourceCounters.GetPropertyByIndex(i, ref prop);

                                using (Slice.From(context.Allocator, prop.Name, out Slice counterNameSlice))
                                using (context.Allocator.Allocate(documentKeyPrefix.Size + counterNameSlice.Size, out var counterKeyBuffer))
                                using (CreateCounterKeySlice(context, counterKeyBuffer, documentKeyPrefix, counterNameSlice, out var counterKeySlice))
                                {
                                    if (table.SeekOneBackwardByPrimaryKeyPrefix(documentKeyPrefix, counterKeySlice, out var tvr) == false)
                                        continue;

                                    var existingChangeVector = TableValueToChangeVector(context, (int)CountersTable.ChangeVector, ref tvr);

                                    var firstOccurance = false;
                                    if (canSkip.TryGetValue(existingChangeVector, out var alreadyMerged))
                                    {
                                        if (alreadyMerged)
                                            continue;
                                    }
                                    else
                                    {
                                        if (ChangeVectorUtils.GetConflictStatus(changeVector, existingChangeVector) == ConflictStatus.AlreadyMerged)
                                        {
                                            canSkip.Add(existingChangeVector, true);
                                            continue;
                                        }

                                        firstOccurance = true;
                                    }

                                    using (data = GetCounterValuesData(context, ref tvr))
                                    {
                                        data = data.Clone(context);
                                    }

                                    if (data.TryGet(DbIds, out BlittableJsonReaderArray dbIds) == false)
                                    {
                                        throw new InvalidDataException($"Local Counter-Group document '{counterKeySlice}' is missing '{DbIds}' property. Shouldn't happen");
                                    }
                                    var dbIdsHolder = new DbIdsHolder(dbIds);

                                    if (data.TryGet(Values, out BlittableJsonReaderObject localCounters) == false)
                                    {
                                        throw new InvalidDataException($"Local Counter-Group document '{counterKeySlice}' is missing '{Values}' property. Shouldn't happen");
                                    }

                                    if (MergeCounterIfNeeded(context, localCounters, ref prop, dbIdsHolder, sourceDbIds, 
                                        out var localCounterValues, out var modified, out var changeType) == false)
                                        continue;

                                    using (Slice.From(context.Allocator, tvr.Read((int)CountersTable.CounterKey, out var size), size, out var countersGroupKey))
                                    {
                                        string changeVectorToSave;

                                        if (modified)
                                        {
                                            using (var old = data)
                                            {
                                                data = context.ReadObject(data, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                                            }

                                            var counterName = prop.Name;
                                            var value = InternalGetCounterValue(localCounterValues, documentId, counterName);
                                            context.Transaction.AddAfterCommitNotification(new CounterChange
                                            {
                                                ChangeVector = changeVector,
                                                DocumentId = documentId,
                                                Name = counterName,
                                                Value = value,
                                                Type = changeType
                                            });

                                            UpdateMetrics(counterKeySlice, counterName, changeVector, collection);

                                            if (data.Size + sizeof(long) * 3 > 2048)
                                            {
                                                // after adding a new counter to the counters blittable
                                                // we caused the blittable to grow beyond 2KB (the 24bytes is an 
                                                // estimate, we don't actually depend on hard 2KB limit).

                                                changeVectorToSave = GetChangeVectorToSave(changeVector, firstOccurance, existingChangeVector, canSkip);

                                                SplitCounterGroup(context, collectionName, table, documentKeyPrefix, countersGroupKey, localCounters, dbIds, changeVectorToSave, ref tvr);

                                                continue;
                                            }

                                        }

                                        changeVectorToSave = GetChangeVectorToSave(changeVector, firstOccurance, existingChangeVector, canSkip);

                                        var etag = _documentsStorage.GenerateNextEtag();
                                        using (Slice.From(context.Allocator, changeVectorToSave, out var cv))
                                        using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
                                        using (table.Allocate(out TableValueBuilder tvb))
                                        {
                                            tvb.Add(countersGroupKey);
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

        private static string GetChangeVectorToSave(string changeVector, bool firstOccurance, string existingChangeVector, Dictionary<string, bool> canSkip)
        {
            if (firstOccurance == false)
                return existingChangeVector;

            var changeVectorToSave = ChangeVectorUtils.MergeVectors(existingChangeVector, changeVector);
            canSkip[changeVectorToSave] = false;

            return changeVectorToSave;
        }

        private bool MergeCounterIfNeeded(DocumentsOperationContext context, BlittableJsonReaderObject localCounters, ref BlittableJsonReaderObject.PropertyDetails prop, 
            DbIdsHolder dbIdsHolder, BlittableJsonReaderArray sourceDbIds, out BlittableJsonReaderObject.RawBlob localCounterValues, out bool modified,
            out CounterChangeTypes changeType)
        {
            LazyStringValue deletedLocalCounter = null;
            modified = false;
            localCounterValues = null;
            changeType = CounterChangeTypes.Put;
            var counterName = prop.Name;

            if (localCounters.TryGetMember(prop.Name, out object localVal))
            {
                if (localVal is LazyStringValue lsv)
                {
                    deletedLocalCounter = lsv;
                }
                else
                {
                    localCounterValues = localVal as BlittableJsonReaderObject.RawBlob;
                }
            }

            switch (prop.Value)
            {
                case BlittableJsonReaderObject.RawBlob sourceBlob:
                {
                    if (deletedLocalCounter != null)
                    {
                        // blob + delete => resolve conflict

                        var conflictStatus = CompareCounterValuesAndDeletedCounter(sourceBlob, deletedLocalCounter, dbIdsHolder.dbIdsList, false);

                        switch (conflictStatus)
                        {
                            case ConflictStatus.Update:
                            // raw blob is more up to date => put counter
                            case ConflictStatus.Conflict:
                                // conflict => resolve to raw blob 
                                localCounterValues = new BlittableJsonReaderObject.RawBlob();
                                break;
                            case ConflictStatus.AlreadyMerged:
                                //delete is more up do date (no change)
                                return false;
                        }
                    }
                    else if (localCounterValues == null)
                    {
                        // put new counter
                        localCounterValues = new BlittableJsonReaderObject.RawBlob();
                    }

                    // blob + blob => put counter
                    modified = InternalPutCounter(context, localCounters, counterName, dbIdsHolder, sourceDbIds, localCounterValues, sourceBlob);
                    return true;
                }

                case LazyStringValue deletedSourceCounter:
                {
                    if (deletedLocalCounter != null)
                    {
                        // delete + delete => merge change vectors if needd

                        if (deletedLocalCounter == deletedSourceCounter)
                            return false;

                        modified = true;
                        var mergedCv = MergeDeletedCounterVectors(deletedLocalCounter, deletedSourceCounter);

                        localCounters.Modifications = localCounters.Modifications ?? new DynamicJsonValue(localCounters);
                        localCounters.Modifications[counterName] = mergedCv;
                        return true;
                    }

                    if (localCounterValues != null)
                    {
                        // delete + blob => resolve conflict

                        var conflictStatus =
                            CompareCounterValuesAndDeletedCounter(localCounterValues, deletedSourceCounter, dbIdsHolder.dbIdsList, true);

                        switch (conflictStatus)
                        {
                            case ConflictStatus.Update:
                                //delete is more up do date
                                modified = true;
                                changeType = CounterChangeTypes.Delete;
                                localCounters.Modifications = localCounters.Modifications ?? new DynamicJsonValue(localCounters);
                                localCounters.Modifications[counterName] = deletedSourceCounter;
                                return true;
                            case ConflictStatus.Conflict:
                            // conflict => resolve to raw blob (no change)
                            case ConflictStatus.AlreadyMerged:
                                // raw blob is more up to date (no change)
                                return false;
                            default:
                                return false;
                        }
                    }

                    // put deleted counter

                    modified = true;
                    changeType = CounterChangeTypes.Delete;
                    localCounters.Modifications = localCounters.Modifications ?? new DynamicJsonValue(localCounters);
                    localCounters.Modifications[counterName] = deletedSourceCounter;
                    return true;
                }

                default:
                    return false;
            }
        }

        internal static long InternalGetCounterValue(BlittableJsonReaderObject.RawBlob localCounterValues, string docId, string counterName)
        {
            Debug.Assert(localCounterValues != null);
            var count = localCounterValues.Length / SizeOfCounterValues;
            long value = 0;

            try
            {
                for (int index = 0; index < count; index++)
                {
                    value = checked(value + ((CounterValues*)localCounterValues.Ptr)[index].Value);
                }
            }
            catch (OverflowException e)
            {
                CounterOverflowException.ThrowFor(docId, counterName, e);
            }


            return value;
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

        private bool InternalPutCounter(DocumentsOperationContext context, BlittableJsonReaderObject counters, string counterName,
            DbIdsHolder localDbIds, BlittableJsonReaderArray sourceDbIds,
            BlittableJsonReaderObject.RawBlob existingCounter, BlittableJsonReaderObject.RawBlob source)
        {
            var existingCount = existingCounter.Length / SizeOfCounterValues;
            var sourceCount = source.Length / SizeOfCounterValues;
            var modified = false;

            for (var index = 0; index < sourceCount; index++)
            {
                var sourceDbId = (LazyStringValue)sourceDbIds[index];
                var sourceValue = &((CounterValues*)source.Ptr)[index];

                int localDbIdIndex = localDbIds.GetOrAddDbIdIndex(sourceDbId);

                if (localDbIdIndex < existingCount)
                {
                    var localValuePtr = (CounterValues*)existingCounter.Ptr + localDbIdIndex;
                    if (localValuePtr->Etag >= sourceValue->Etag)
                        continue;

                    localValuePtr->Value = sourceValue->Value;
                    localValuePtr->Etag = sourceValue->Etag;

                    continue;
                }

                // counter doesn't have this dbId
                modified = true;
                existingCounter = AddPartialValueToExistingCounter(context, existingCounter, localDbIdIndex, sourceValue->Value, sourceValue->Etag);

                existingCount = existingCounter.Length / SizeOfCounterValues;
            }

            if (modified)
            {
                counters.Modifications = counters.Modifications ?? new DynamicJsonValue(counters);
                counters.Modifications[counterName] = existingCounter;
            }

            return modified;
        }

        private BlittableJsonReaderObject.RawBlob AddPartialValueToExistingCounter(DocumentsOperationContext context,
            BlittableJsonReaderObject.RawBlob existingCounter, int dbIdIndex, long sourceValue, long sourceEtag)
        {
            var scope = context.Allocator.Allocate((dbIdIndex + 1) * SizeOfCounterValues, out var newVal);
            _counterModificationMemoryScopes.Add(scope);

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

            return existingCounter;
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

        public Table GetCountersTable(Transaction tx, CollectionName collection)
        {
            var tableName = collection.GetTableName(CollectionTableType.CounterGroups);

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

            using (DocumentIdWorker.GetSliceFromId(context, docId, out Slice key, separator: SpecialChars.RecordSeparator))
            {
                foreach (var counterGroup in table.SeekByPrimaryKeyPrefix(key, Slices.Empty, 0))
                {
                    var data = GetCounterValuesData(context, ref counterGroup.Value.Reader);
                    if (data.TryGet(Values, out BlittableJsonReaderObject counters) == false)
                    {
                        throw new InvalidDataException($"Counter-Group document ({docId}) '{counterGroup.Key}' is missing '{Values}' property. Shouldn't happen");
                    }

                    var prop = new BlittableJsonReaderObject.PropertyDetails();
                    for (var i = 0; i < counters.Count; i++)
                    {
                        counters.GetPropertyByIndex(i, ref prop);
                        if (prop.Value is LazyStringValue)
                            continue; //deleted

                        yield return prop.Name;
                    }
                }
            }
        }

        private static BlittableJsonReaderObject GetCounterValuesData(JsonOperationContext context, ref TableValueReader existing)
        {
            return new BlittableJsonReaderObject(existing.Read((int)CountersTable.Data, out int oldSize), oldSize, context);
        }

        public long? GetCounterValue(DocumentsOperationContext context, string docId, string counterName)
        {
            if (TryGetRawBlob(context, docId, counterName, out var blob) == false)
                return null;

            return InternalGetCounterValue(blob, docId, counterName);
        }

        private static bool TryGetRawBlob(DocumentsOperationContext context, string docId, string counterName, out BlittableJsonReaderObject.RawBlob blob)
        {
            blob = null;
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            using (DocumentIdWorker.GetSliceFromId(context, docId, out Slice documentIdPrefix, separator: SpecialChars.RecordSeparator))
            using (Slice.From(context.Allocator, counterName, out Slice counterNameSlice))
            using (context.Allocator.Allocate(counterNameSlice.Size + documentIdPrefix.Size, out var buffer))
            using (CreateCounterKeySlice(context, buffer, documentIdPrefix, counterNameSlice, out var counterKeySlice))
            {
                if (table.SeekOneBackwardByPrimaryKeyPrefix(documentIdPrefix, counterKeySlice, out var tvr) == false)
                    return false;

                var data = GetCounterValuesData(context, ref tvr);
                if (data.TryGet(Values, out BlittableJsonReaderObject counters) == false ||
                    counters.TryGetMember(counterName, out object counterValues) == false ||
                    counterValues is LazyStringValue)
                    return false;

                blob = counterValues as BlittableJsonReaderObject.RawBlob;
                return true;
            }
        }

        private static ByteStringContext.ExternalScope CreateCounterKeySlice(DocumentsOperationContext context, ByteString buffer, Slice documentIdPrefix, Slice counterName, out Slice counterKeySlice)
        {
            var scope = Slice.External(context.Allocator, buffer.Ptr, buffer.Length, out counterKeySlice);
            documentIdPrefix.CopyTo(buffer.Ptr);
            counterName.CopyTo(buffer.Ptr + documentIdPrefix.Size);
            return scope;
        }

        public IEnumerable<(string ChangeVector, long Value)> GetCounterValues(DocumentsOperationContext context, string docId, string counterName)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            using (DocumentIdWorker.GetSliceFromId(context, docId, out Slice documentIdPrefix, separator: SpecialChars.RecordSeparator))
            using (Slice.From(context.Allocator, counterName, out Slice counterNameSlice))
            using (context.Allocator.Allocate(counterNameSlice.Size + documentIdPrefix.Size, out var buffer))
            using (CreateCounterKeySlice(context, buffer, documentIdPrefix, counterNameSlice, out var counterKeySlice))
            {
                if (table.SeekOneBackwardByPrimaryKeyPrefix(documentIdPrefix, counterKeySlice, out var tvr) == false)
                    yield break;

                var data = GetCounterValuesData(context, ref tvr);
                if (data.TryGet(DbIds, out BlittableJsonReaderArray dbIds) == false ||
                    data.TryGet(Values, out BlittableJsonReaderObject counters) == false ||
                    counters.TryGetMember(counterName, out object counterValues) == false ||
                    counterValues is LazyStringValue)
                    yield break;

                var dbCv = context.LastDatabaseChangeVector ?? GetDatabaseChangeVector(context);

                var blob = counterValues as BlittableJsonReaderObject.RawBlob;
                var existingCount = blob?.Length / SizeOfCounterValues ?? 0;

                for (var dbIdIndex = 0; dbIdIndex < existingCount; dbIdIndex++)
                {
                    var dbId = dbIds[dbIdIndex].ToString();
                    var val = GetPartialValue(dbIdIndex, blob);
                    var nodeTag = ChangeVectorUtils.GetNodeTagById(dbCv, dbId);
                    yield return ($"{nodeTag ?? "?"}-{dbId}", val);
                }
                
            }
        }

        internal static long GetPartialValue(int index, BlittableJsonReaderObject.RawBlob counterValues)
        {
            return ((CounterValues*)counterValues.Ptr)[index].Value;
        }

        internal IEnumerable<CounterGroupDetail> GetCounterValuesForDocument(DocumentsOperationContext context, string docId)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            using (DocumentIdWorker.GetSliceFromId(context, docId, out Slice key, separator: SpecialChars.RecordSeparator))
            {
                foreach (var result in table.SeekByPrimaryKeyPrefix(key, Slices.Empty, 0))
                {
                    yield return TableValueToCounterGroupDetail(context, ref result.Value.Reader);
                }
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

            using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice lowerIdPrefix, separator: 30))
            {
                table.DeleteByPrimaryKeyPrefix(lowerIdPrefix);
            }
        }

        public string DeleteCounter(DocumentsOperationContext context, string documentId, string collection, string counterName, bool forceTombstone = false,
            long lastModifiedTicks = -1)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false); // never hit
            }

            using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice documentKeyPrefix, separator: SpecialChars.RecordSeparator))
            using (Slice.From(context.Allocator, counterName, out Slice counterNameSlice))
            using (context.Allocator.Allocate(documentKeyPrefix.Size + counterNameSlice.Size, out var counterKeyBuffer))
            using (CreateCounterKeySlice(context, counterKeyBuffer, documentKeyPrefix, counterNameSlice, out var counterKeySlice))
            {
                var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
                var table = GetCountersTable(context.Transaction.InnerTransaction, collectionName);
                if (table.SeekOneBackwardByPrimaryKeyPrefix(documentKeyPrefix, counterKeySlice, out var existing) == false)
                    return null;

                var data = GetCounterValuesData(context, ref existing);

                if (data.TryGet(Values, out BlittableJsonReaderObject counters) == false)
                    return null;

                var propIndex = counters.GetPropertyIndex(counterName);
                if (propIndex == -1)
                    return null;

                var prop = new BlittableJsonReaderObject.PropertyDetails();
                counters.GetPropertyByIndex(propIndex, ref prop);

                if (prop.Value is LazyStringValue) // already deleted
                    return null;

                counterName = prop.Name; // use original casing
                var deleteCv = GenerateDeleteChangeVectorFromRawBlob(data, prop.Value as BlittableJsonReaderObject.RawBlob);
                counters.Modifications = new DynamicJsonValue(counters)
                {
                    [counterName] = deleteCv
                };

                using (var old = data)
                {
                    data = context.ReadObject(data, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                }

                var newEtag = _documentsStorage.GenerateNextEtag();

                var databaseChangeVector = context.LastDatabaseChangeVector ?? GetDatabaseChangeVector(context);
                var newChangeVector = ChangeVectorUtils.TryUpdateChangeVector(_documentDatabase, databaseChangeVector, newEtag).ChangeVector;
                context.LastDatabaseChangeVector = newChangeVector;

                using (Slice.From(context.Allocator, existing.Read((int)CountersTable.CounterKey, out var size), size, out var counterGroupKey))
                using (Slice.From(context.Allocator, newChangeVector, out var cv))
                using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(counterGroupKey);
                    tvb.Add(Bits.SwapBytes(newEtag));
                    tvb.Add(cv);
                    tvb.Add(data.BasePointer, data.Size);
                    tvb.Add(collectionSlice);
                    tvb.Add(context.TransactionMarkerOffset);

                    table.Set(tvb);
                }

                context.Transaction.AddAfterCommitNotification(new CounterChange
                {
                    ChangeVector = newChangeVector,
                    DocumentId = documentId,
                    Name = counterName,
                    Type = CounterChangeTypes.Delete
                });

                return newChangeVector;
            }
        }

        internal string GenerateDeleteChangeVectorFromRawBlob(BlittableJsonReaderObject data,
            BlittableJsonReaderObject.RawBlob counterToDelete)
        {
            if (data.TryGet(DbIds, out BlittableJsonReaderArray dbIds) == false)
                throw new InvalidDataException($"Counter-Group document is missing '{DbIds}' property. Shouldn't happen");

            var dbIdIndex = GetOrAddLocalDbIdIndex(dbIds);
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
                    var dbId = dbIds[i].ToString();

                    sb.Append(dbId)
                        .Append(":")
                        .Append(etag);

                    continue;
                }

                var newEtag = _documentDatabase.DocumentsStorage.GenerateNextEtag();
                sb.Append(_documentDatabase.DbBase64Id)
                    .Append(":")
                    .Append(newEtag);
            }

            if (count < dbIdIndex)
            {
                var newEtag = _documentDatabase.DocumentsStorage.GenerateNextEtag();
                sb.Append(_documentDatabase.DbBase64Id)
                    .Append(":")
                    .Append(newEtag);
            }

            return sb.ToString();
        }

        public static LazyStringValue ExtractDocId(JsonOperationContext context, ref TableValueReader tvr)
        {
            var p = tvr.Read((int)CountersTable.CounterKey, out var size);
            int sizeOfDocId = 0;
            for (; sizeOfDocId < size; sizeOfDocId++)
            {
                if (p[sizeOfDocId] == SpecialChars.RecordSeparator)
                    break;
            }

            return context.AllocateStringValue(null, p, sizeOfDocId);
        }

        [Conditional("DEBUG")]
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

        private static SortedSet<string> GetCountersForDocument(BlittableJsonReaderArray metadataCounters, SortedSet<string> countersToAdd,
            HashSet<string> countersToRemove, out bool modified)
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

                if (!(prop.Value is BlittableJsonReaderObject.RawBlob blob))
                    continue; // deleted counter, noop

                var dja = new DynamicJsonArray();
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
                counterGroupDetail.Values = context.ReadObject(counterGroupDetail.Values, counterGroupDetail.DocumentId);
            }
        }

        private static ConflictStatus CompareCounterValuesAndDeletedCounter(BlittableJsonReaderObject.RawBlob counterValues, string deletedCounter,
            List<LazyStringValue> dbIds, bool remoteDelete)
        {
            //any missing entries from a change vector are assumed to have zero value
            var blobHasLargerEntries = false;
            var deleteHasLargerEntries = false;

            var deletedCv = DeletedCounterToChangeVectorEntries(deletedCounter);

            var sizeOfValues = counterValues.Length / SizeOfCounterValues;

            int numOfMatches = 0;
            for (int i = 0; i < sizeOfValues; i++)
            {
                var found = false;

                var current = (CounterValues*)counterValues.Ptr + i;
                var etag = current->Etag;
                if (etag == 0)
                    continue;

                for (int j = 0; j < deletedCv.Length; j++)
                {
                    if (dbIds[i] == deletedCv[j].DbId)
                    {
                        found = true;
                        numOfMatches++;

                        if (etag > deletedCv[j].Etag)
                        {
                            blobHasLargerEntries = true;
                        }
                        else if (etag < deletedCv[j].Etag)
                        {
                            deleteHasLargerEntries = true;
                        }

                        break;
                    }
                }

                if (found == false)
                {
                    blobHasLargerEntries = true;
                }

                if (blobHasLargerEntries && deleteHasLargerEntries)
                {
                    return ConflictStatus.Conflict;
                }
            }

            if (numOfMatches < deletedCv.Length)
            {
                deleteHasLargerEntries = true;
            }

            if (blobHasLargerEntries && deleteHasLargerEntries)
                return ConflictStatus.Conflict;

            if (blobHasLargerEntries == false && deleteHasLargerEntries == false)
                return ConflictStatus.AlreadyMerged; // change vectors identical

            return remoteDelete
                ? blobHasLargerEntries ? ConflictStatus.AlreadyMerged : ConflictStatus.Update
                : blobHasLargerEntries
                    ? ConflictStatus.Update
                    : ConflictStatus.AlreadyMerged;
        }

        private static ChangeVectorEntry[] DeletedCounterToChangeVectorEntries(string changeVector)
        {
            if (string.IsNullOrEmpty(changeVector))
                return Array.Empty<ChangeVectorEntry>();

            var span = changeVector.AsSpan();

            var list = new List<ChangeVectorEntry>();
            var currentPos = 0;
            while (currentPos < changeVector.Length)
            {
                var dbId = span.Slice(currentPos, DbIdAsBase64Size);
                currentPos += DbIdAsBase64Size + 1;

                var next = changeVector.IndexOf(',', currentPos);
                var etagStr = next > 0
                    ? span.Slice(currentPos, next - currentPos)
                    : span.Slice(currentPos);

                if (long.TryParse(etagStr, out var etag) == false)
                    throw new InvalidDataException("Invalid deleted counter string: " + changeVector);

                list.Add(new ChangeVectorEntry
                {
                    Etag = etag,
                    DbId = dbId.ToString()
                });

                if (next == -1)
                    break;

                currentPos = next + 2;
            }

            return list.ToArray();
        }

        private static string MergeDeletedCounterVectors(string deletedA, string deletedB)
        {
            var mergeVectorBuffer = new List<ChangeVectorEntry>();

            MergeDeletedCounterVector(deletedA, mergeVectorBuffer);
            MergeDeletedCounterVector(deletedB, mergeVectorBuffer);

            var sb = new StringBuilder();
            for (int i = 0; i < mergeVectorBuffer.Count; i++)
            {
                if (i > 0)
                    sb.Append(", ");

                sb.Append(mergeVectorBuffer[i].DbId)
                    .Append(":")
                    .Append(mergeVectorBuffer[i].Etag);
            }

            return sb.ToString();
        }

        private static void MergeDeletedCounterVector(string deletedCounterVector, List<ChangeVectorEntry> entries)
        {
            if (string.IsNullOrEmpty(deletedCounterVector))
                return;

            var span = deletedCounterVector.AsSpan();
            var currentPos = 0;
            while (currentPos < deletedCounterVector.Length)
            {
                var dbId = span.Slice(currentPos, DbIdAsBase64Size).ToString();
                currentPos += DbIdAsBase64Size + 1;

                var next = deletedCounterVector.IndexOf(',', currentPos);
                var etagStr = next > 0
                    ? span.Slice(currentPos, next - currentPos)
                    : span.Slice(currentPos);

                if (long.TryParse(etagStr, out var etag) == false)
                    throw new InvalidDataException("Invalid deleted counter string: " + deletedCounterVector);

                var found = false;
                for (int i = 0; i < entries.Count; i++)
                {
                    if (entries[i].DbId == dbId)
                    {
                        if (entries[i].Etag < etag)
                        {
                            entries[i] = new ChangeVectorEntry
                            {
                                Etag = etag,
                                DbId = dbId
                            };
                        }
                        found = true;
                        break;
                    }
                }
                if (found == false)
                {
                    entries.Add(new ChangeVectorEntry
                    {
                        Etag = etag,
                        DbId = dbId
                    });
                }

                if (next == -1)
                    break;

                currentPos = next + 2;
            }
        }

        public long GetNumberOfCounterEntries(DocumentsOperationContext context)
        {
            var fstIndex = CountersSchema.FixedSizeIndexes[AllCountersEtagSlice];
            var fst = context.Transaction.InnerTransaction.FixedTreeFor(fstIndex.Name, sizeof(long));
            return fst.NumberOfEntries;
        }
    }
}

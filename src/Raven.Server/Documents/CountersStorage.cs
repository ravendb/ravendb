using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Exceptions.Documents.Counters;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using static Raven.Server.Documents.DocumentsStorage;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents
{
    public unsafe class CountersStorage
    {
        public const int DbIdAsBase64Size = 22;

        public const int MaxCounterDocumentSize = 2048;

        private readonly DocumentDatabase _documentDatabase;
        private readonly DocumentsStorage _documentsStorage;

        public static readonly Slice AllCountersEtagSlice;
        internal static readonly Slice CollectionCountersEtagsSlice;
        internal static readonly Slice CounterKeysSlice;

        public const string DbIds = "@dbIds";
        public const string Values = "@vals";
        public const string CounterNames = "@names";

        internal readonly List<ByteStringContext<ByteStringMemoryCache>.InternalScope> _counterModificationMemoryScopes =
            new List<ByteStringContext<ByteStringMemoryCache>.InternalScope>();

        private readonly ObjectPool<Dictionary<LazyStringValue, PutCountersData>> _dictionariesPool
            = new ObjectPool<Dictionary<LazyStringValue, PutCountersData>>(() => new Dictionary<LazyStringValue, PutCountersData>(LazyStringValueComparer.Instance));

        public static int SizeOfCounterValues = sizeof(CounterValues);

        internal static readonly TableSchema CountersSchema = new TableSchema
        {
            TableType = (byte)TableType.Counters
        };

        internal enum CountersTable
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

        internal class DbIdsHolder
        {
            private readonly BlittableJsonReaderArray _dbIdsBlittableArray;
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
                    _dbIdsBlittableArray.Modifications ??= new DynamicJsonArray();
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

        public readonly IndexingMethods Indexing;

        public CountersStorage(DocumentDatabase documentDatabase, Transaction tx)
        {
            _documentDatabase = documentDatabase;
            _documentsStorage = documentDatabase.DocumentsStorage;

            tx.CreateTree(CounterKeysSlice);

            Indexing = new IndexingMethods(this);
        }

        public IEnumerable<ReplicationBatchItem> GetCountersFrom(DocumentsOperationContext context, long etag, bool caseInsensitiveNames = true)
        {
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(CountersSchema.FixedSizeIndexes[AllCountersEtagSlice], etag, 0))
            {
                var countersItem = CreateReplicationBatchItem(context, ref result.Reader);

                if (caseInsensitiveNames)
                {
                    yield return countersItem;
                    continue;
                }

                // 4.2 replication destination
                // need to change the CounterGroup document to match 4.2 format
                var converted = ConvertToCaseSensitiveFormat(context, countersItem);
                yield return converted;
            }
        }

        private static CounterReplicationItem ConvertToCaseSensitiveFormat(DocumentsOperationContext context, CounterReplicationItem countersItem)
        {
            if (countersItem.Values.TryGet(Values, out BlittableJsonReaderObject counterValues) == false)
                throw new InvalidDataException($"CounterGroup document {countersItem.Id} is missing '{Values}' property");

            if (countersItem.Values.TryGet(CounterNames, out BlittableJsonReaderObject originalNames) == false)
                throw new InvalidDataException($"CounterGroup document {countersItem.Id} is missing '{CounterNames}' property");

            countersItem.Values.Modifications = new DynamicJsonValue(countersItem.Values);
            counterValues.Modifications = new DynamicJsonValue(counterValues);

            // remove @names property
            countersItem.Values.Modifications.Remove(CounterNames);

            var prop = new BlittableJsonReaderObject.PropertyDetails();
            for (int i = 0; i < counterValues.Count; i++)
            {
                counterValues.GetPropertyByIndex(i, ref prop);
                var originalName = GetOriginalName(context, originalNames, prop.Name);
                if (LazyStringValueComparer.Instance.Equals(prop.Name, originalName))
                    continue;

                // remove the lower-cased property, use original casing instead
                counterValues.Modifications.Remove(prop.Name);
                counterValues.Modifications[originalName] = prop.Value;
            }

            using (var old = countersItem.Values)
            {
                countersItem.Values = context.ReadObject(countersItem.Values, countersItem.Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            }

            return countersItem;
        }

        public IEnumerable<CounterGroupDetail> GetCountersFrom(DocumentsOperationContext context, long etag, long skip, long take)
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

        public IEnumerable<CounterGroupDetail> GetCountersFrom(DocumentsOperationContext context, string collection, long etag, long skip, long take)
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

        internal static CounterReplicationItem CreateReplicationBatchItem(DocumentsOperationContext context, ref TableValueReader reader)
        {
            var data = GetCounterValuesData(context, ref reader);
            var docId = ExtractDocId(context, ref reader);

            return new CounterReplicationItem
            {
                Type = ReplicationBatchItem.ReplicationItemType.CounterGroup,
                Id = docId,
                ChangeVector = TableValueToChangeVector(context, (int)CountersTable.ChangeVector, ref reader),
                Values = data,
                Collection = TableValueToId(context, (int)CountersTable.Collection, ref reader),
                Etag = TableValueToEtag((int)CountersTable.Etag, ref reader),
                TransactionMarker = TableValueToShort((int)CountersTable.TransactionMarker, nameof(CountersTable.TransactionMarker), ref reader)
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

            if (name.Length > DocumentIdWorker.MaxIdSize)
            {
                ThrowCounterNameTooBig(name);
            }

            try
            {
                var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
                var table = GetCountersTable(context.Transaction.InnerTransaction, collectionName);

                ByteStringContext.InternalScope countersGroupKeyScope = default;
                using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice documentKeyPrefix, separator: SpecialChars.RecordSeparator))
                using (DocumentIdWorker.GetLower(context.Allocator, name, out Slice counterName))
                using (context.Allocator.Allocate(documentKeyPrefix.Size + counterName.Size, out var counterKeyBuffer))
                using (CreateCounterKeySlice(context, counterKeyBuffer, documentKeyPrefix, counterName, out var counterKeySlice))
                {
                    BlittableJsonReaderObject data;
                    exists = false;
                    var value = delta;
                    var lowerName = Encodings.Utf8.GetString(counterName.Content.Ptr, counterName.Content.Length);

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
                            ThrowMissingProperty(counterKeySlice, DbIds);

                        var dbIdIndex = GetOrAddLocalDbIdIndex(dbIds);

                        if (data.TryGet(Values, out BlittableJsonReaderObject counters) == false)
                            ThrowMissingProperty(counterKeySlice, Values);

                        if (data.TryGet(CounterNames, out BlittableJsonReaderObject originalNames) == false)
                            ThrowMissingProperty(counterKeySlice, CounterNames);

                        var counterEtag = _documentsStorage.GenerateNextEtag();

                        counters.TryGetMember(lowerName, out object existingCounter);

                        if (existingCounter is BlittableJsonReaderObject.RawBlob blob &&
                            overrideExisting == false)
                        {
                            exists = IncrementExistingCounter(context, documentId, lowerName, name, delta,
                                blob, dbIdIndex, counterEtag, counters, ref value);
                        }
                        else
                        {
                            if (existingCounter == null &&
                                data.Size + sizeof(long) * 3 > MaxCounterDocumentSize)
                            {
                                // we now need to add a new counter to the counters blittable
                                // and adding it will cause to grow beyond 2KB (the 24bytes is an
                                // estimate, we don't actually depend on hard 2KB limit).

                                var existingChangeVector = TableValueToChangeVector(context, (int)CountersTable.ChangeVector, ref existing);
                                using (data)
                                {
                                    SplitCounterGroup(context, collectionName, table, documentKeyPrefix, countersGroupKey, counters, dbIds, originalNames, existingChangeVector);
                                }

                                // now we retry and know that we have enough space
                                return PutOrIncrementCounter(context, documentId, collection, name, delta, out exists, overrideExisting);
                            }

                            CreateNewCounterOrOverrideExisting(context, lowerName, dbIdIndex, value, counterEtag, counters);

                            if (existingCounter == null)
                            {
                                // new counter
                                originalNames.Modifications = new DynamicJsonValue
                                {
                                    [lowerName] = name
                                };
                            }
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
                        data = WriteNewCountersDocument(context, originalName: name, lowerName, value);
                        countersGroupKey = documentKeyPrefix;
                    }

                    var groupEtag = _documentsStorage.GenerateNextEtag();
                    var changeVector = _documentsStorage.GetNewChangeVector(context, groupEtag);

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
                            tvb.Add(context.GetTransactionMarker());

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
                        CollectionName = collectionName.Name,
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

        internal static void ThrowMissingProperty(Slice counterKeySlice, string property)
        {
            throw new InvalidDataException($"Counter-Group document '{counterKeySlice}' is missing '{property}' property. Shouldn't happen");
        }

        private static void ThrowCounterNameTooBig(string name)
        {
            throw new ArgumentException(
                $"Counter name cannot exceed {DocumentIdWorker.MaxIdSize} bytes, but counter name has {name.Length} characters. " +
                $"The invalid counter name is '{name}'.", nameof(name));
        }

        private static void SplitCounterGroup(DocumentsOperationContext context, CollectionName collectionName, Table table, Slice documentKeyPrefix,
            Slice countersGroupKey, BlittableJsonReaderObject values, BlittableJsonReaderArray dbIds, BlittableJsonReaderObject originalNames, string changeVector)
        {
            var (fst, snd) = SplitCounterDocument(context, values, dbIds, originalNames);

            using (Slice.From(context.Allocator, changeVector, out var cv))
            using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
            {
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(countersGroupKey);
                    tvb.Add(Bits.SwapBytes(context.DocumentDatabase.DocumentsStorage.GenerateNextEtag()));
                    tvb.Add(cv);
                    tvb.Add(fst.BasePointer, fst.Size);
                    tvb.Add(collectionSlice);
                    tvb.Add(context.GetTransactionMarker());
                    table.Set(tvb);
                }

                fst.TryGet(Values, out BlittableJsonReaderObject fstValues);
                snd.TryGet(Values, out BlittableJsonReaderObject sndValues);
                BlittableJsonReaderObject.PropertyDetails firstPropertySnd = default, lastPropertyFirst = default;
                sndValues.GetPropertyByIndex(0, ref firstPropertySnd);
                fstValues.GetPropertyByIndex(fstValues.Count - 1, ref lastPropertyFirst);

                var firstChange = 0;
                for (; firstChange < lastPropertyFirst.Name.Size; firstChange++)
                {
                    if (firstPropertySnd.Name[firstChange] != lastPropertyFirst.Name[firstChange])
                        break;
                }

                var etag = context.DocumentDatabase.DocumentsStorage.GenerateNextEtag();
                var cv2 = context.DocumentDatabase.DocumentsStorage.GetNewChangeVector(context, etag);

                using (context.Allocator.Allocate(documentKeyPrefix.Size + firstChange + 1, out ByteString newCounterKey))
                using (Slice.From(context.Allocator, cv2, out cv))
                {
                    documentKeyPrefix.CopyTo(newCounterKey.Ptr);
                    Memory.Copy(newCounterKey.Ptr + documentKeyPrefix.Size, firstPropertySnd.Name.Buffer, firstChange + 1);
                    using (table.Allocate(out TableValueBuilder tvb))
                    {
                        tvb.Add(newCounterKey);
                        tvb.Add(Bits.SwapBytes(etag));
                        tvb.Add(cv);
                        tvb.Add(snd.BasePointer, snd.Size);
                        tvb.Add(collectionSlice);
                        tvb.Add(context.GetTransactionMarker());
                        table.Insert(tvb);
                    }
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

        private void CreateNewCounterOrOverrideExisting(DocumentsOperationContext context, string lowerName, int dbIdIndex, long value, long newETag,
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
                [lowerName] = new BlittableJsonReaderObject.RawBlob
                {
                    Length = newVal.Length,
                    Ptr = newVal.Ptr
                }
            };
        }

        private bool IncrementExistingCounter(DocumentsOperationContext context, string documentId, string lowerName, string originalName, long delta,
            BlittableJsonReaderObject.RawBlob existingCounter,
            int dbIdIndex, long newETag, BlittableJsonReaderObject counters, ref long value)
        {
            var existingCount = existingCounter.Length / SizeOfCounterValues;

            if (dbIdIndex < existingCount)
            {
                var counter = (CounterValues*)existingCounter.Ptr + dbIdIndex; // existingCounter.Ptr + sizeof(CounterValues) * dbIdIndex
                try
                {
                    value = checked(counter->Value + delta); //inc
                    counter->Value = value;
                    counter->Etag = newETag;
                }
                catch (OverflowException e)
                {
                    CounterOverflowException.ThrowFor(documentId, originalName, counter->Value, delta, e);
                }

                return true;
            }

            // counter exists , but not with local DbId

            existingCounter = AddPartialValueToExistingCounter(context, existingCounter, dbIdIndex, value, newETag);

            counters.Modifications = new DynamicJsonValue(counters)
            {
                [lowerName] = existingCounter
            };

            return false;
        }

        private BlittableJsonReaderObject WriteNewCountersDocument(DocumentsOperationContext context, string originalName, string lowerName, long value)
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

                    builder.WritePropertyName(CounterNames);
                    builder.StartWriteObject();

                    builder.WritePropertyName(lowerName);
                    builder.WriteValue(originalName);

                    builder.WriteObjectEnd();

                    builder.WritePropertyName(Values);
                    builder.StartWriteObject();

                    builder.WritePropertyName(lowerName);
                    builder.WriteRawBlob(newVal.Ptr, newVal.Length);

                    builder.WriteObjectEnd();

                    builder.WriteObjectEnd();
                    builder.FinalizeDocument();

                    data = builder.CreateReader();
                }
            }

            return data;
        }

        internal static (BlittableJsonReaderObject First, BlittableJsonReaderObject Second) SplitCounterDocument(DocumentsOperationContext context,
            BlittableJsonReaderObject values, BlittableJsonReaderArray dbIds, BlittableJsonReaderObject originalNames)
        {
            // here we rely on the internal sort order of the blittables, because we go through them
            // in lexical order
            var fst = CreateHalfDocument(context, values, 0, values.Count / 2, dbIds, originalNames);
            var snd = CreateHalfDocument(context, values, values.Count / 2, values.Count, dbIds, originalNames);

            return (fst, snd);
        }

        private static ByteStringContext<ByteStringMemoryCache>.InternalScope ReWriteBlob(
            DocumentsOperationContext context,
            BlittableJsonReaderObject.RawBlob blob,
            HashSet<int> usedDbIndexes,
            out BlittableJsonReaderObject.RawBlob newBlob)
        {
            var partialValues = blob.Length / SizeOfCounterValues;

            Span<CounterValues> newValues = stackalloc CounterValues[usedDbIndexes.Count];
            var currentIndex = 0;
            var newSize = 0;

            foreach (int oldIndex in usedDbIndexes)
            {
                if (oldIndex < partialValues)
                {
                    newValues[currentIndex] = GetPartialValue(oldIndex, blob);
                }

                if (newValues[currentIndex].Value != 0)
                    newSize = currentIndex + 1;

                currentIndex++;
            }

            var scope = context.Allocator.Allocate(newSize * SizeOfCounterValues, out var newVal);
            fixed (byte* ptr = MemoryMarshal.Cast<CounterValues, byte>(newValues.Slice(0, newSize)))
            {
                Memory.Copy(newVal.Ptr, ptr, newSize * SizeOfCounterValues);
            }

            newBlob = new BlittableJsonReaderObject.RawBlob
            {
                Ptr = newVal.Ptr,
                Length = newVal.Length
            };
            return scope;
        }

        private static BlittableJsonReaderObject CreateHalfDocument(DocumentsOperationContext context, BlittableJsonReaderObject values, int start, int end,
            BlittableJsonReaderArray dbIds, BlittableJsonReaderObject originalNames)
        {
            BlittableJsonReaderObject.PropertyDetails prop = default;

            // find used indexes
            var usedDbIdsIndexes = new HashSet<int>();
            for (int i = start; i < end; i++)
            {
                values.GetPropertyByIndex(i, ref prop);
                if (prop.Value is BlittableJsonReaderObject.RawBlob blob)
                {
                    var numberOfPartialValues = blob.Length / SizeOfCounterValues;
                    Debug.Assert(numberOfPartialValues <= dbIds.Length);

                    for (int index = 0; index < numberOfPartialValues; index++)
                    {
                        var val = GetPartialValue(index, blob);
                        if (val.Value != 0)
                            usedDbIdsIndexes.Add(index);
                    }
                }
            }
            var reWriteBlob = usedDbIdsIndexes.Count < dbIds.Length;

            using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
            {
                context.CachedProperties.NewDocument();
                builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                builder.StartWriteObjectDocument();

                builder.StartWriteObject();

                builder.WritePropertyName(Values);
                builder.StartWriteObject();

                for (int i = start; i < end; i++)
                {
                    values.GetPropertyByIndex(i, ref prop);
                    builder.WritePropertyName(prop.Name);
                    if (prop.Value is BlittableJsonReaderObject.RawBlob blob)
                    {
                        if (reWriteBlob)
                        {
                            using (ReWriteBlob(context, blob, usedDbIdsIndexes, out var newBlob))
                            {
                                builder.WriteRawBlob(newBlob.Ptr, newBlob.Length);
                            }
                        }
                        else
                        {
                            builder.WriteRawBlob(blob.Ptr, blob.Length);
                        }
                    }
                    else if (prop.Value is LazyStringValue lsv)
                        builder.WriteValue(lsv); // delete counter
                    else
                        throw new InvalidDataException("Unknown type: " + prop.Token + " when trying to split counter doc");
                }

                builder.WriteObjectEnd();

                builder.WritePropertyName(DbIds);

                builder.StartWriteArray();

                if (reWriteBlob)
                {
                    // it is important to keep those in the same order as the new blob
                    foreach (var oldIndex in usedDbIdsIndexes)
                    {
                        var item = (LazyStringValue)dbIds[oldIndex];
                        builder.WriteValue(item);
                    }
                }
                else
                {
                    for (var index = 0; index < dbIds.Length; index++)
                    {
                        var item = (LazyStringValue)dbIds[index];
                        builder.WriteValue(item);
                    }
                }

                builder.WriteArrayEnd();

                builder.WritePropertyName(CounterNames);
                builder.StartWriteObject();

                for (int i = start; i < end; i++)
                {
                    originalNames.GetPropertyByIndex(i, ref prop);

                    builder.WritePropertyName(prop.Name);
                    if (prop.Value is LazyStringValue lsv)
                        builder.WriteValue(lsv);
                    else if (prop.Value is LazyCompressedStringValue compressed)
                        builder.WriteValue(compressed);
                    else
                        throw new InvalidDataException("Unknown type: " + prop.Token + " when trying to split counter doc");
                }

                builder.WriteObjectEnd();

                builder.WriteObjectEnd();
                builder.FinalizeDocument();

                return builder.CreateReader();
            }
        }

        internal class PutCountersData
        {
            public BlittableJsonReaderObject Data;
            public DbIdsHolder DbIdsHolder;
            public string ChangeVector;
            public bool Modified;
            public ByteStringContext.InternalScope KeyScope;
        }

        public bool PutCounters(DocumentsOperationContext context, string documentId, string collection, string changeVector,
            BlittableJsonReaderObject sourceData)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                return false;
            }

            var entriesToUpdate = _dictionariesPool.Allocate();
            var documentCountersHaveChanged = false;
            try
            {
                var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
                var table = GetCountersTable(context.Transaction.InnerTransaction, collectionName);

                using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice documentKeyPrefix, separator: SpecialChars.RecordSeparator))
                {
                    if (sourceData.TryGet(Values, out BlittableJsonReaderObject sourceCounters) == false)
                    {
                        throw new InvalidDataException($"Incoming Counter-Group document '{documentId}' is missing '{Values}' property. Shouldn't happen");
                    }

                    if (sourceData.TryGet(DbIds, out BlittableJsonReaderArray sourceDbIds) == false)
                    {
                        throw new InvalidDataException($"Remote Counter-Group document '{documentId}' is missing '{DbIds}' property. Shouldn't happen");
                    }

                    sourceData.TryGet(CounterNames, out BlittableJsonReaderObject sourceCounterNames);
                    BlittableJsonReaderObject data;

                    if (table.SeekOnePrimaryKeyPrefix(documentKeyPrefix, out _) == false)
                    {
                        // simplest case of having no counters for this document, can use the raw data from the source as-is

                        if (sourceCounterNames == null)
                        {
                            // 4.2 source
                            // need to create @names array and to lower the property names in 'sourceCounters'

                            var propDetails = new BlittableJsonReaderObject.PropertyDetails();
                            sourceCounters.Modifications = new DynamicJsonValue(sourceCounters);

                            var originalNames = new DynamicJsonValue();

                            for (int i = 0; i < sourceCounters.Count; i++)
                            {
                                sourceCounters.GetPropertyByIndex(i, ref propDetails);

                                var lowered = propDetails.Name.ToLower();
                                originalNames[lowered] = propDetails.Name;

                                if (string.Equals(propDetails.Name, lowered))
                                    continue;

                                sourceCounters.Modifications.Remove(propDetails.Name);
                                sourceCounters.Modifications[lowered] = propDetails.Value;
                            }

                            sourceData.Modifications = new DynamicJsonValue(sourceData)
                            {
                                [CounterNames] = originalNames
                            };
                        }

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
                            tvb.Add(context.GetTransactionMarker());

                            table.Set(tvb);
                        }

                        UpdateMetricsForNewCounterGroup(data);

                        return true;
                    }

                    var prop = new BlittableJsonReaderObject.PropertyDetails();
                    for (var i = 0; i < sourceCounters.Count; i++)
                    {
                        sourceCounters.GetPropertyByIndex(i, ref prop);

                        using (DocumentIdWorker.GetLower(context.Allocator, prop.Name, out Slice counterNameSlice))
                        using (context.Allocator.Allocate(documentKeyPrefix.Size + counterNameSlice.Size, out var counterKeyBuffer))
                        using (CreateCounterKeySlice(context, counterKeyBuffer, documentKeyPrefix, counterNameSlice, out var counterKeySlice))
                        {
                            if (table.SeekOneBackwardByPrimaryKeyPrefix(documentKeyPrefix, counterKeySlice, out var tvr) == false)
                                continue;

                            using (var counterGroupKey = TableValueToString(context, (int)CountersTable.CounterKey, ref tvr))
                            {
                                if (entriesToUpdate.TryGetValue(counterGroupKey, out var putCountersData))
                                {
                                    data = putCountersData.Data;
                                }
                                else
                                {
                                    var existingChangeVector = TableValueToChangeVector(context, (int)CountersTable.ChangeVector, ref tvr);
                                    if (sourceCounterNames != null)
                                    {
                                        // 5.0 source
                                        if (ChangeVectorUtils.GetConflictStatus(changeVector, existingChangeVector) == ConflictStatus.AlreadyMerged)
                                            continue;
                                    }

                                    using (data = GetCounterValuesData(context, ref tvr))
                                    {
                                        data = data.Clone(context);
                                    }

                                    if (data.TryGet(DbIds, out BlittableJsonReaderArray dbIds) == false)
                                    {
                                        ThrowMissingProperty(counterKeySlice, DbIds);
                                    }

                                    putCountersData = new PutCountersData
                                    {
                                        Data = data,
                                        DbIdsHolder = new DbIdsHolder(dbIds),
                                        ChangeVector = existingChangeVector,
                                        Modified = false
                                    };
                                }

                                if (data.TryGet(Values, out BlittableJsonReaderObject localCounters) == false)
                                {
                                    ThrowMissingProperty(counterKeySlice, Values);
                                }

                                if (data.TryGet(CounterNames, out BlittableJsonReaderObject originalNames) == false)
                                {
                                    ThrowMissingProperty(counterKeySlice, CounterNames);
                                }

                                if (MergeCounterIfNeeded(context, localCounters, ref prop, putCountersData.DbIdsHolder, sourceDbIds, sourceCounterNames, originalNames,
                                        out var localCounterValues, out var changeType) == false)
                                {
                                    continue;
                                }

                                Debug.Assert(changeType != CounterChangeTypes.None || localCounters.Modifications != null,
                                    "We asked to update counters, but don't have any change.");

                                if (entriesToUpdate.ContainsKey(counterGroupKey) == false)
                                {
                                    // clone counter group key
                                    var scope = context.Allocator.Allocate(counterGroupKey.Size, out var output);
                                    counterGroupKey.CopyTo(output.Ptr);
                                    var clonedKey = context.AllocateStringValue(null, output.Ptr, output.Length);
                                    putCountersData.KeyScope = scope;
                                    entriesToUpdate[clonedKey] = putCountersData;
                                }

                                if (localCounters.Modifications != null)
                                {
                                    putCountersData.Modified = true;
                                }

                                if (changeType == CounterChangeTypes.None)
                                    continue;

                                if (changeType != CounterChangeTypes.Increment)
                                    documentCountersHaveChanged = true;

                                var counterName = prop.Name;
                                var value = 0L;
                                if (localCounterValues != null)
                                    value = InternalGetCounterValue(localCounterValues, documentId, counterName, capOnOverflow: true);

                                context.Transaction.AddAfterCommitNotification(new CounterChange
                                {
                                    ChangeVector = changeVector,
                                    DocumentId = documentId,
                                    CollectionName = collectionName.Name,
                                    Name = counterName,
                                    Value = value,
                                    Type = changeType
                                });

                                UpdateMetrics(counterKeySlice, counterName, changeVector, collection);
                            }
                        }
                    }

                    foreach (var kvp in entriesToUpdate)
                    {
                        var putCountersData = kvp.Value;
                        var currentData = putCountersData.Data;

                        if (putCountersData.Modified)
                        {
                            using (var old = currentData)
                            {
                                currentData = context.ReadObject(currentData, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                            }
                        }

                        var changeVectorToSave = ChangeVectorUtils.MergeVectors(putCountersData.ChangeVector, changeVector);

                        using (Slice.External(context.Allocator, kvp.Key, out var countersGroupKey))
                        {
                            if (currentData.Size > MaxCounterDocumentSize)
                            {
                                // after adding new counters to the counters blittable
                                // we caused the blittable to grow beyond 2KB

                                currentData.TryGet(Values, out BlittableJsonReaderObject localCounters);
                                currentData.TryGet(DbIds, out BlittableJsonReaderArray dbIds);
                                currentData.TryGet(CounterNames, out BlittableJsonReaderObject originalNames);

                                using (currentData)
                                {
                                    SplitCounterGroup(context, collectionName, table, documentKeyPrefix, countersGroupKey, localCounters, dbIds, originalNames,
                                        changeVectorToSave);
                                }

                                continue;
                            }

                            var etag = _documentsStorage.GenerateNextEtag();
                            using (Slice.From(context.Allocator, changeVectorToSave, out var cv))
                            using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
                            using (table.Allocate(out TableValueBuilder tvb))
                            {
                                tvb.Add(countersGroupKey);
                                tvb.Add(Bits.SwapBytes(etag));
                                tvb.Add(cv);
                                tvb.Add(currentData.BasePointer, currentData.Size);
                                tvb.Add(collectionSlice);
                                tvb.Add(context.GetTransactionMarker());

                                table.Set(tvb);
                            }
                        }
                    }
                }

                return documentCountersHaveChanged;
            }
            finally
            {
                foreach (var s in _counterModificationMemoryScopes)
                {
                    s.Dispose();
                }

                foreach (var kvp in entriesToUpdate)
                {
                    kvp.Value.KeyScope.Dispose();
                    kvp.Key.Dispose();
                }

                _counterModificationMemoryScopes.Clear();
                entriesToUpdate.Clear();
                _dictionariesPool.Free(entriesToUpdate);
            }
        }

        private void UpdateMetricsForNewCounterGroup(BlittableJsonReaderObject data)
        {
            _documentDatabase.Metrics.Counters.BytesPutsPerSec.MarkSingleThreaded(data.Size);

            if (data.TryGet(Values, out BlittableJsonReaderObject values) == false)
                return;

            _documentDatabase.Metrics.Counters.PutsPerSec.MarkSingleThreaded(values.Count);
        }

        internal bool MergeCounterIfNeeded(DocumentsOperationContext context,
            BlittableJsonReaderObject localCounters,
            ref BlittableJsonReaderObject.PropertyDetails incomingCountersProp,
            DbIdsHolder dbIdsHolder,
            BlittableJsonReaderArray sourceDbIds,
            BlittableJsonReaderObject sourceCounterNames,
            BlittableJsonReaderObject localCounterNames,
            out BlittableJsonReaderObject.RawBlob localCounterValues,
            out CounterChangeTypes changeType)
        {
            LazyStringValue deletedLocalCounter = null;
            localCounterValues = null;
            changeType = CounterChangeTypes.None;
            string counterName = incomingCountersProp.Name;

            if (sourceCounterNames == null)
            {
                // 4.2 source
                counterName = incomingCountersProp.Name.ToLower();
            }

            if (localCounters.TryGetMember(counterName, out object localVal))
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

            switch (incomingCountersProp.Value)
            {
                case BlittableJsonReaderObject.RawBlob sourceBlob:
                    {
                        if (deletedLocalCounter != null)
                        {
                            // blob + delete => resolve conflict

                            var conflictStatus = CompareCounterValuesAndDeletedCounter(sourceBlob, deletedLocalCounter, dbIdsHolder.dbIdsList, false, out _);

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

                            // need to add original counter name to 'CounterNames' array

                            AddNewCounterName(context, incomingCountersProp.Name, sourceCounterNames, localCounterNames, counterName);
                        }

                        // blob + blob => put counter
                        changeType = InternalPutCounter(context, localCounters, counterName, dbIdsHolder, sourceDbIds, localCounterValues, sourceBlob);
                        if (changeType == CounterChangeTypes.None)
                            return false;

                        return true;
                    }

                case LazyStringValue deletedSourceCounter:
                    {
                        if (deletedLocalCounter != null)
                        {
                            // delete + delete => merge change vectors if needed

                            if (deletedLocalCounter == deletedSourceCounter)
                                return false;

                            var mergedCv = MergeDeletedCounterVectors(deletedLocalCounter, deletedSourceCounter);

                            localCounters.Modifications ??= new DynamicJsonValue(localCounters);
                            localCounters.Modifications[counterName] = mergedCv;
                            return true;
                        }

                        if (localCounterValues != null)
                        {
                            // delete + blob => resolve conflict

                            var conflictStatus =
                                CompareCounterValuesAndDeletedCounter(localCounterValues, deletedSourceCounter, dbIdsHolder.dbIdsList, true, out var deletedCv);

                            switch (conflictStatus)
                            {
                                case ConflictStatus.Update:
                                    //delete is more up do date
                                    break;
                                case ConflictStatus.Conflict:
                                    // conflict => resolve to raw blob and merge change vectors
                                    MergeBlobAndDeleteVector(context, dbIdsHolder, localCounterValues, deletedCv);

                                    localCounters.Modifications ??= new DynamicJsonValue(localCounters);
                                    localCounters.Modifications[counterName] = localCounterValues;
                                    return true;
                                case ConflictStatus.AlreadyMerged:
                                    // raw blob is more up to date (no change)
                                    return false;
                                default:
                                    return false;
                            }
                        }
                        else
                        {
                            AddNewCounterName(context, incomingCountersProp.Name, sourceCounterNames, localCounterNames, counterName);
                        }

                        // put deleted counter

                        changeType = CounterChangeTypes.Delete;
                        localCounters.Modifications ??= new DynamicJsonValue(localCounters);
                        localCounters.Modifications[counterName] = deletedSourceCounter;
                        return true;
                    }

                default:
                    return false;
            }
        }

        private static void AddNewCounterName(DocumentsOperationContext context, LazyStringValue propName, BlittableJsonReaderObject sourceCounterNames,
            BlittableJsonReaderObject localCounterNames,
            string loweredName)
        {
            localCounterNames.Modifications ??= new DynamicJsonValue(localCounterNames);

            if (sourceCounterNames != null)
            {
                var originalName = GetOriginalName(context, sourceCounterNames, loweredName);

                localCounterNames.Modifications[loweredName] = originalName;
            }
            else
            {
                // 4.2 source

                bool shouldAdd = true;
                for (int i = 0; i < localCounterNames.Modifications.Properties.Count; i++)
                {
                    var current = localCounterNames.Modifications.Properties[i].Name;
                    if (string.Equals(current, loweredName) == false)
                        continue;

                    // already added a new counter with the same name but under a different casing
                    shouldAdd = false;
                    break;
                }

                if (shouldAdd)
                {
                    localCounterNames.Modifications[loweredName] = propName;
                }
            }
        }

        private static LazyStringValue GetOriginalName(DocumentsOperationContext context, BlittableJsonReaderObject counterNames, string counterName)
        {
            var lower = counterName.ToLower();
            if (counterNames.TryGet(lower, out object originalName) == false)
                return context.GetLazyString(counterName);
            return GetLazyStringCounterName(lower, originalName);
        }

        private static LazyStringValue GetLazyStringCounterName(string key, object value)
        {
            switch (value)
            {
                case LazyStringValue lsv:
                    return lsv;
                case LazyCompressedStringValue compressed:
                    return compressed.ToLazyStringValue();
                default:
                    throw new InvalidDataException($"Unexpected type in '{CounterNames}' object. property : '{key}', type : '{value.GetType().Name}'");
            }
        }

        private void MergeBlobAndDeleteVector(DocumentsOperationContext context, DbIdsHolder dbIdsHolder, BlittableJsonReaderObject.RawBlob localCounterValues, ChangeVectorEntry[] deletedCv)
        {
            foreach (var entry in deletedCv)
            {
                using (var dbIdLsv = context.GetLazyString(entry.DbId))
                {
                    var dbIdIndex = dbIdsHolder.GetOrAddDbIdIndex(dbIdLsv);
                    if (dbIdIndex < localCounterValues.Length / SizeOfCounterValues)
                    {
                        var current = (CounterValues*)localCounterValues.Ptr + dbIdIndex;
                        if (entry.Etag > current->Etag)
                        {
                            current->Etag = entry.Etag;
                        }
                    }
                    else
                    {
                        localCounterValues = AddPartialValueToExistingCounter(context, localCounterValues, dbIdIndex, 0, entry.Etag);
                    }
                }
            }
        }

        internal static long InternalGetCounterValue(BlittableJsonReaderObject.RawBlob localCounterValues, string docId, string counterName, bool capOnOverflow = false)
        {
            Debug.Assert(localCounterValues != null, "localCounterValues == null");
            var count = localCounterValues.Length / SizeOfCounterValues;
            long value = 0;
            long localValue = 0;
            try
            {
                for (int index = 0; index < count; index++)
                {
                    localValue = ((CounterValues*)localCounterValues.Ptr)[index].Value;
                    value = checked(value + localValue);
                }
            }
            catch (OverflowException e)
            {
                if (capOnOverflow == false)
                    CounterOverflowException.ThrowFor(docId, counterName, e);

                return value + localValue > 0 ?
                    long.MinValue :
                    long.MaxValue;
            }

            return value;
        }

        private static List<LazyStringValue> DbIdsToList(BlittableJsonReaderArray dbIds)
        {
            var localDbIdsList = new List<LazyStringValue>(dbIds.Length);
            for (int i = 0; i < dbIds.Length; i++)
            {
                localDbIdsList.Add(dbIds.GetByIndex<LazyStringValue>(i));
            }

            return localDbIdsList;
        }

        private CounterChangeTypes InternalPutCounter(
            DocumentsOperationContext context,
            BlittableJsonReaderObject counters,
            string counterName,
            DbIdsHolder localDbIds,
            BlittableJsonReaderArray sourceDbIds,
            BlittableJsonReaderObject.RawBlob existingCounter,
            BlittableJsonReaderObject.RawBlob source)
        {
            var existingCount = existingCounter.Length / SizeOfCounterValues;
            var sourceCount = source.Length / SizeOfCounterValues;
            var modified = false;

            var changeType = existingCount == 0 ? CounterChangeTypes.Put : CounterChangeTypes.None;

            for (var index = 0; index < sourceCount; index++)
            {
                var sourceDbId = sourceDbIds.GetByIndex<LazyStringValue>(index);
                var sourceValue = &((CounterValues*)source.Ptr)[index];

                int localDbIdIndex = localDbIds.GetOrAddDbIdIndex(sourceDbId);

                if (localDbIdIndex < existingCount)
                {
                    var localValuePtr = (CounterValues*)existingCounter.Ptr + localDbIdIndex;
                    if (localValuePtr->Etag >= sourceValue->Etag)
                        continue;

                    if (changeType == CounterChangeTypes.None)
                    {
                        changeType = CounterChangeTypes.Increment;
                    }

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
                if (changeType == CounterChangeTypes.None)
                {
                    changeType = CounterChangeTypes.Increment;
                }

                counters.Modifications ??= new DynamicJsonValue(counters);
                counters.Modifications[counterName] = existingCounter;
            }

            return changeType;
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

            foreach (string c in GetCountersForDocumentInternal(context, docId, table))
                yield return c;
        }

        internal IEnumerable<string> GetCountersForDocument(DocumentsOperationContext context, Transaction transaction, string docId)
        {
            var table = new Table(CountersSchema, transaction);

            foreach (string c in GetCountersForDocumentInternal(context, docId, table))
                yield return c;
        }

        private static IEnumerable<string> GetCountersForDocumentInternal(DocumentsOperationContext context, string docId, Table table)
        {
            using (DocumentIdWorker.GetSliceFromId(context, docId, out Slice key, separator: SpecialChars.RecordSeparator))
            {
                var names = GetCountersOriginalCasing(context, docId, table, key);
                if (names == null)
                    yield break;

                names.Sort();
                foreach (var name in names)
                {
                    yield return name;
                }
            }
        }

        private static List<LazyStringValue> GetCountersOriginalCasing(DocumentsOperationContext context, string docId, Table table, Slice key)
        {
            List<LazyStringValue> all = null;

            foreach (var counterGroup in table.SeekByPrimaryKeyPrefix(key, Slices.Empty, 0))
            {
                var data = GetCounterValuesData(context, ref counterGroup.Value.Reader);
                if (data.TryGet(CounterNames, out BlittableJsonReaderObject names) == false)
                    ThrowMissingProperty(counterGroup.Key, CounterNames);

                if (data.TryGet(Values, out BlittableJsonReaderObject counterValues) == false)
                    ThrowMissingProperty(counterGroup.Key, Values);

                all ??= new List<LazyStringValue>();

                var prop = new BlittableJsonReaderObject.PropertyDetails();
                for (var i = 0; i < names.Count; i++)
                {
                    names.GetPropertyByIndex(i, ref prop);
                    if (counterValues.TryGet(prop.Name, out object existing) == false ||
                        existing is LazyStringValue) // skip names of 'dead' counters
                        continue;
                    var originalName = GetLazyStringCounterName(prop.Name, prop.Value);
                    all.Add(originalName);
                }
            }

            return all;
        }

        public DynamicJsonArray GetCountersForDocumentList(DocumentsOperationContext context, string docId)
        {
            return new DynamicJsonArray(GetCountersForDocument(context, docId));
        }

        internal static BlittableJsonReaderObject GetCounterValuesData(JsonOperationContext context, ref TableValueReader existing)
        {
            return new BlittableJsonReaderObject(existing.Read((int)CountersTable.Data, out int oldSize), oldSize, context);
        }

        internal CounterValues? GetCounterValue(DocumentsOperationContext context, string docId, string counterName, bool capOnOverflow = false)
        {
            if (string.IsNullOrEmpty(counterName) ||
                TryGetRawBlob(context, docId, counterName, out var etag, out var blob) == false)
                return null;

            return new CounterValues
            {
                Value = InternalGetCounterValue(blob, docId, counterName, capOnOverflow),
                Etag = etag
            };
        }

        private static bool TryGetRawBlob(DocumentsOperationContext context, string docId, string counterName, out long etag, out BlittableJsonReaderObject.RawBlob blob)
        {
            blob = null;
            etag = -1;
            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            using (DocumentIdWorker.GetSliceFromId(context, docId, out Slice documentIdPrefix, separator: SpecialChars.RecordSeparator))
            using (DocumentIdWorker.GetLower(context.Allocator, counterName, out Slice counterNameSlice))
            using (context.Allocator.Allocate(counterNameSlice.Size + documentIdPrefix.Size, out var buffer))
            using (CreateCounterKeySlice(context, buffer, documentIdPrefix, counterNameSlice, out var counterKeySlice))
            {
                if (table.SeekOneBackwardByPrimaryKeyPrefix(documentIdPrefix, counterKeySlice, out var tvr) == false)
                    return false;

                var data = GetCounterValuesData(context, ref tvr);
                etag = GetCounterGroupEtag(ref tvr);
                var lowerName = Encodings.Utf8.GetString(counterNameSlice.Content.Ptr, counterNameSlice.Content.Length);

                if (data.TryGet(Values, out BlittableJsonReaderObject counters) == false ||
                    counters.TryGetMember(lowerName, out object counterValues) == false ||
                    counterValues is LazyStringValue)
                    return false;

                blob = counterValues as BlittableJsonReaderObject.RawBlob;
                return true;
            }
        }

        internal static ByteStringContext.ExternalScope CreateCounterKeySlice(DocumentsOperationContext context, ByteString buffer, Slice documentIdPrefix, Slice counterName, out Slice counterKeySlice)
        {
            var scope = Slice.External(context.Allocator, buffer.Ptr, buffer.Length, out counterKeySlice);
            documentIdPrefix.CopyTo(buffer.Ptr);
            counterName.CopyTo(buffer.Ptr + documentIdPrefix.Size);
            return scope;
        }

        public IEnumerable<CounterPartialValue> GetCounterPartialValues(DocumentsOperationContext context, string docId, string counterName)
        {
            if (string.IsNullOrEmpty(counterName))
                yield break;

            var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

            using (DocumentIdWorker.GetSliceFromId(context, docId, out Slice documentIdPrefix, separator: SpecialChars.RecordSeparator))
            using (DocumentIdWorker.GetLower(context.Allocator, counterName, out Slice counterNameSlice))
            using (context.Allocator.Allocate(counterNameSlice.Size + documentIdPrefix.Size, out var buffer))
            using (CreateCounterKeySlice(context, buffer, documentIdPrefix, counterNameSlice, out var counterKeySlice))
            {
                if (table.SeekOneBackwardByPrimaryKeyPrefix(documentIdPrefix, counterKeySlice, out var tvr) == false)
                    yield break;

                var data = GetCounterValuesData(context, ref tvr);
                var etag = GetCounterGroupEtag(ref tvr);
                var lowerName = counterNameSlice.ToString();
                if (data.TryGet(DbIds, out BlittableJsonReaderArray dbIds) == false ||
                    data.TryGet(Values, out BlittableJsonReaderObject counters) == false ||
                    counters.TryGetMember(lowerName, out object counterValues) == false ||
                    counterValues is LazyStringValue)
                    yield break;

                var dbCv = context.LastDatabaseChangeVector ?? GetDatabaseChangeVector(context);

                var blob = counterValues as BlittableJsonReaderObject.RawBlob;
                var existingCount = blob?.Length / SizeOfCounterValues ?? 0;

                for (var dbIdIndex = 0; dbIdIndex < existingCount; dbIdIndex++)
                {
                    var dbId = dbIds[dbIdIndex].ToString();
                    var value = GetPartialValue(dbIdIndex, blob);
                    var nodeTag = ChangeVectorUtils.GetNodeTagById(dbCv, dbId);
                    yield return new CounterPartialValue(value.Value, etag, $"{nodeTag ?? "?"}-{dbId}");
                }
            }
        }

        internal IEnumerable<CounterInfo> GetCountersFromCounterGroup(CounterGroupDetail counterGroup)
        {
            var data = counterGroup.Values;

            if (data.TryGet(DbIds, out BlittableJsonReaderArray dbIds) == false ||
                data.TryGet(Values, out BlittableJsonReaderObject counters) == false)
                yield break;

            foreach (var name in counters.GetPropertyNames())
            {
                var counterValues = counters[name];
                var blob = counterValues as BlittableJsonReaderObject.RawBlob;
                var existingCount = blob?.Length / SizeOfCounterValues ?? 0;

                for (var dbIdIndex = 0; dbIdIndex < existingCount; dbIdIndex++)
                {
                    var dbId = dbIds[dbIdIndex].ToString();
                    var val = GetPartialValue(dbIdIndex, blob);
                    yield return new CounterInfo
                    {
                        Name = name,
                        Value = val.Value,
                        DbId = dbId,
                        Etag = val.Etag,
                    };
                }
            }
        }

        private static long GetCounterGroupEtag(ref TableValueReader tvr)
        {
            return Bits.SwapBytes(*(long*)tvr.Read((int)CountersTable.Etag, out _));
        }

        internal static CounterValues GetPartialValue(int index, BlittableJsonReaderObject.RawBlob counterValues)
        {
            return *((CounterValues*)counterValues.Ptr + index);
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

        public string DeleteCounter(DocumentsOperationContext context, string documentId, string collection, string counterName)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false); // never hit
            }

            using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice documentKeyPrefix, separator: SpecialChars.RecordSeparator))
            using (DocumentIdWorker.GetLower(context.Allocator, counterName, out Slice counterNameSlice))
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

                var lowered = Encodings.Utf8.GetString(counterNameSlice.Content.Ptr, counterNameSlice.Content.Length); // lowered cased name
                if (counters.TryGetMember(lowered, out object counterToDelete) == false)
                    return null; // not found
                if (counterToDelete is LazyStringValue) // already deleted
                    return null;

                var deleteCv = GenerateDeleteChangeVectorFromRawBlob(data, counterToDelete as BlittableJsonReaderObject.RawBlob);
                counters.Modifications = new DynamicJsonValue(counters)
                {
                    [lowered] = deleteCv
                };

                using (var old = data)
                {
                    data = context.ReadObject(data, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                }

                var newEtag = _documentsStorage.GenerateNextEtag();
                var newChangeVector = _documentsStorage.GetNewChangeVector(context, newEtag);

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
                    tvb.Add(context.GetTransactionMarker());

                    table.Set(tvb);
                }

                context.Transaction.AddAfterCommitNotification(new CounterChange
                {
                    ChangeVector = newChangeVector,
                    DocumentId = documentId,
                    CollectionName = collectionName.Name,
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

            long newEtag = -1;
            for (int i = 0; i < count; i++)
            {
                if (i > 0)
                {
                    sb.Append(", ");
                }

                if (i != dbIdIndex)
                {
                    var etag = ((CounterValues*)counterToDelete.Ptr + i)->Etag;
                    if (etag <= 0)
                        continue;

                    var dbId = dbIds[i].ToString();

                    sb.Append(dbId)
                        .Append(":")
                        .Append(etag);

                    continue;
                }

                newEtag = _documentDatabase.DocumentsStorage.GenerateNextEtag();
                sb.Append(_documentDatabase.DbBase64Id)
                    .Append(":")
                    .Append(newEtag);
            }

            if (newEtag == -1)
            {
                // add local part to delete change vector

                if (count > 0)
                {
                    sb.Append(", ");
                }

                newEtag = _documentDatabase.DocumentsStorage.GenerateNextEtag();
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

        public string UpdateDocumentCounters(DocumentsOperationContext context, Document document, string docId,
            SortedSet<string> countersToAdd, HashSet<string> countersToRemove, NonPersistentDocumentFlags nonPersistentDocumentFlags)
        {
            var newData = ApplyCounterUpdatesToMetadata(context, document.Data, docId, countersToAdd, countersToRemove, ref document.Flags);
            if (newData == null)
                return null;

            var putResult = _documentDatabase.DocumentsStorage.Put(context, docId, expectedChangeVector: null, newData, flags: document.Flags, nonPersistentFlags: nonPersistentDocumentFlags);
            return putResult.ChangeVector;
        }

        internal static BlittableJsonReaderObject ApplyCounterUpdatesToMetadata(JsonOperationContext context, BlittableJsonReaderObject data, string docId,
            SortedSet<string> countersToAdd, HashSet<string> countersToRemove, ref DocumentFlags flags)
        {
            if ((countersToRemove == null || countersToRemove.Count == 0) && countersToAdd.Count == 0)
                return null;

            BlittableJsonReaderArray metadataCounters = null;
            if (data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
            {
                metadata.TryGet(Constants.Documents.Metadata.Counters, out metadataCounters);
            }

            var counters = UpdateNamesList(metadataCounters, countersToAdd, countersToRemove, out var hadModifications);
            if (hadModifications == false)
                return null;

            flags = flags.Strip(DocumentFlags.FromClusterTransaction | DocumentFlags.Resolved);
            if (counters.Count == 0)
            {
                flags = flags.Strip(DocumentFlags.HasCounters);
                if (metadata != null)
                {
                    metadata.Modifications = new DynamicJsonValue(metadata);
                    metadata.Modifications.Remove(Constants.Documents.Metadata.Counters);
                    data.Modifications = new DynamicJsonValue(data) { [Constants.Documents.Metadata.Key] = metadata };
                }
            }
            else
            {
                flags |= DocumentFlags.HasCounters;
                data.Modifications = new DynamicJsonValue(data);
                if (metadata == null)
                {
                    data.Modifications[Constants.Documents.Metadata.Key] = new DynamicJsonValue { [Constants.Documents.Metadata.Counters] = new DynamicJsonArray(counters) };
                }
                else
                {
                    metadata.Modifications = new DynamicJsonValue(metadata) { [Constants.Documents.Metadata.Counters] = new DynamicJsonArray(counters) };
                    data.Modifications[Constants.Documents.Metadata.Key] = metadata;
                }
            }

            using (data)
            {
                return context.ReadObject(data, docId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            }
        }

        internal static SortedSet<string> UpdateNamesList(BlittableJsonReaderArray existingNames, SortedSet<string> namesToAdd,
            HashSet<string> namesToRemove, out bool modified)
        {
            modified = false;
            if (existingNames == null)
            {
                modified = true;
                return namesToAdd;
            }

            foreach (var name in existingNames)
            {
                var str = name.ToString();
                if (namesToRemove?.Contains(str) == true)
                {
                    modified = true;
                    continue;
                }

                namesToAdd.Add(str);
            }

            if (modified == false)
            {
                // if no name was removed, we can be sure that there are no modification when the names count in metadata is equal to the count of namesToAdd
                modified = namesToAdd.Count != existingNames.Length;
            }

            return namesToAdd;
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
            List<LazyStringValue> dbIds, bool remoteDelete, out ChangeVectorEntry[] deletedCv)
        {
            //any missing entries from a change vector are assumed to have zero value
            var blobHasLargerEntries = false;
            var deleteHasLargerEntries = false;

            deletedCv = DeletedCounterToChangeVectorEntries(deletedCounter);

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

        // We aren't using ChangeVectorParser here because we don't store the node tag in the counters
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

        public long GetLastCounterEtag(DocumentsOperationContext context, string collection)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return 0;

            var table = context.Transaction.InnerTransaction.OpenTable(CountersSchema, collectionName.GetTableName(CollectionTableType.CounterGroups));

            // ReSharper disable once UseNullPropagation
            if (table == null)
                return 0;

            var result = table.ReadLast(CountersSchema.FixedSizeIndexes[CollectionCountersEtagsSlice]);
            if (result == null)
                return 0;

            return TableValueToEtag((int)CountersTable.Etag, ref result.Reader);
        }

        public class IndexingMethods
        {
            private readonly CountersStorage _countersStorage;

            public IndexingMethods(CountersStorage countersStorage)
            {
                _countersStorage = countersStorage;
            }

            public CounterGroupItemMetadata GetCountersMetadata(DocumentsOperationContext context, long etag)
            {
                var table = new Table(CountersSchema, context.Transaction.InnerTransaction);
                var index = CountersSchema.FixedSizeIndexes[AllCountersEtagSlice];

                if (table.Read(context.Allocator, index, etag, out var tvr) == false)
                    return null;

                foreach (var item in TableValueToCounterGroupItemMetadata(context, tvr))
                    return item;

                return null;
            }

            public CounterGroupItemMetadata GetCountersMetadata(DocumentsOperationContext context, Slice counterKeySlice)
            {
                var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

                using (ExtractDocumentIdAndCounterNameFromKey(context, counterKeySlice, out var documentIdPrefix, out var loweredCounterNameSlice))
                {
                    if (table.SeekOneBackwardByPrimaryKeyPrefix(documentIdPrefix, counterKeySlice, out var tvr) == false)
                        return null;

                    var loweredCounterName = CounterNameLsv();

                    foreach (var item in TableValueToCounterGroupItemMetadata(context, tvr, loweredCounterNameToFilter: loweredCounterName))
                    {
                        return item;
                    }

                    return null;

                    unsafe LazyStringValue CounterNameLsv()
                    {
                        return context.GetLazyString(loweredCounterNameSlice.Content.Ptr, loweredCounterNameSlice.Size);
                    }
                }
            }

            public IEnumerable<CounterGroupItemMetadata> GetCountersMetadataFrom(DocumentsOperationContext context, long etag, long skip, long take)
            {
                var table = new Table(CountersSchema, context.Transaction.InnerTransaction);

                foreach (var result in table.SeekForwardFrom(CountersSchema.FixedSizeIndexes[AllCountersEtagSlice], etag, skip))
                {
                    if (take-- <= 0)
                        yield break;

                    foreach (var item in TableValueToCounterGroupItemMetadata(context, result.Reader))
                        yield return item;
                }
            }

            public IEnumerable<CounterGroupItemMetadata> GetCountersMetadataFrom(DocumentsOperationContext context, string collection, long etag, long skip, long take)
            {
                var collectionName = _countersStorage._documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
                if (collectionName == null)
                    yield break;

                var table = _countersStorage.GetCountersTable(context.Transaction.InnerTransaction, collectionName);

                if (table == null)
                    yield break;

                foreach (var result in table.SeekForwardFrom(CountersSchema.FixedSizeIndexes[CollectionCountersEtagsSlice], etag, skip))
                {
                    if (take-- <= 0)
                        yield break;

                    foreach (var item in TableValueToCounterGroupItemMetadata(context, result.Reader))
                        yield return item;
                }
            }

            public IDisposable ExtractDocumentIdAndCounterNameFromKey(DocumentsOperationContext context, Slice key, out Slice loweredDocumentIdPrefix, out Slice loweredCounterName)
            {
                int sizeOfDocId = 0;
                for (; sizeOfDocId < key.Size; sizeOfDocId++)
                {
                    if (key[sizeOfDocId] == SpecialChars.RecordSeparator)
                        break;
                }

                var docIdScope = Slice.External(context.Allocator, key, sizeOfDocId + 1, out loweredDocumentIdPrefix);
                var counterNameScope = Slice.External(context.Allocator, key.Content, sizeOfDocId + 1, key.Size - sizeOfDocId - 1, out loweredCounterName);

                return new ExtractScopes(docIdScope, counterNameScope);
            }

            private static IEnumerable<CounterGroupItemMetadata> TableValueToCounterGroupItemMetadata(DocumentsOperationContext context, TableValueReader tvr, LazyStringValue loweredCounterNameToFilter = null)
            {
                var etag = TableValueToEtag((int)CountersTable.Etag, ref tvr);

                var valuesData = GetCounterValuesData(context, ref tvr);
                int size = 0;

                if (valuesData != null)
                {
                    size = valuesData.Size;

                    if (valuesData.TryGet(Values, out BlittableJsonReaderObject values))
                    {
                        size /= values.Count; // just 'estimating'
                        if (valuesData.TryGet(CounterNames, out BlittableJsonReaderObject originalNames) == false)
                            throw new InvalidDataException($"CounterGroup item '{CounterGroupKey()}' is missing '{CounterNames}' object. Shouldn't happen");

                        var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                        for (var i = 0; i < originalNames.Count; i++)
                        {
                            originalNames.GetPropertyByIndex(i, ref propertyDetails);
                            LazyStringValue originalLoweredCounterName = propertyDetails.Name;

                            if (loweredCounterNameToFilter != null && loweredCounterNameToFilter.Equals(originalLoweredCounterName) == false)
                                continue;

                            var counterName = GetLazyStringCounterName(originalLoweredCounterName, propertyDetails.Value);
                            var docId = ExtractDocId();
                            using (ToDocumentIdPrefix(docId, out var documentIdPrefix))
                            {
                                var keyScope = ToKey(documentIdPrefix, counterName, out var key);
                                var luceneKey = ToLuceneKey(docId, counterName);

                                yield return new CounterGroupItemMetadata(key, keyScope, luceneKey, docId, counterName, etag, size);
                            }
                        }

                        yield break;
                    }
                }

                yield return new CounterGroupItemMetadata(null, null, null, null, null, etag, size);

                LazyStringValue CounterGroupKey()
                {
                    var p = tvr.Read((int)CountersTable.CounterKey, out var size);
                    return context.GetLazyString(p, size);
                }

                LazyStringValue ExtractDocId()
                {
                    var p = tvr.Read((int)CountersTable.CounterKey, out var size);
                    int sizeOfDocId = 0;
                    for (; sizeOfDocId < size; sizeOfDocId++)
                    {
                        if (p[sizeOfDocId] == SpecialChars.RecordSeparator)
                            break;
                    }

                    return context.GetLazyString(p, sizeOfDocId);
                }

                IDisposable ToDocumentIdPrefix(LazyStringValue documentId, out Slice documentIdPrefix)
                {
                    var p = tvr.Read((int)CountersTable.CounterKey, out _);

                    return Slice.From(context.Allocator, p, documentId.Size + 1, out documentIdPrefix);
                }

                IDisposable ToKey(Slice documentIdPrefix, LazyStringValue counterName, out LazyStringValue key)
                {
                    using (DocumentIdWorker.GetLower(context.Allocator, counterName, out var counterNameSlice))
                    {
                        var scope = context.Allocator.Allocate(counterNameSlice.Size + documentIdPrefix.Size, out var buffer);
                        CreateCounterKeySlice(context, buffer, documentIdPrefix, counterNameSlice, out var counterKeySlice);

                        key = context.AllocateStringValue(null, counterKeySlice.Content.Ptr, counterKeySlice.Content.Length);
                        return scope;
                    }
                }

                LazyStringValue ToLuceneKey(LazyStringValue documentId, LazyStringValue counterName)
                {
                    using (DocumentIdWorker.GetLower(context.Allocator, counterName, out var counterNameSlice))
                    {
                        var size = documentId.Size
                           + 1 // separator
                           + counterNameSlice.Size;

                        using (context.Allocator.Allocate(size, out var buffer))
                        {
                            var bufferSpan = new Span<byte>(buffer.Ptr, size);
                            documentId.AsSpan().CopyTo(bufferSpan);
                            var offset = documentId.Size;
                            bufferSpan[offset++] = SpecialChars.LuceneRecordSeparator;
                            counterNameSlice.AsSpan().CopyTo(bufferSpan.Slice(offset));

                            return context.GetLazyString(buffer.Ptr, size);
                        }
                    }
                }
            }
        }

        private readonly struct ExtractScopes : IDisposable
        {
            private readonly ByteStringContext<ByteStringMemoryCache>.ExternalScope _docIdScope;
            private readonly ByteStringContext<ByteStringMemoryCache>.ExternalScope _counterNameScope;

            public ExtractScopes(ByteStringContext.ExternalScope docIdScope, ByteStringContext.ExternalScope counterNameScope)
            {
                _docIdScope = docIdScope;
                _counterNameScope = counterNameScope;
            }

            public void Dispose()
            {
                using (_docIdScope)
                using (_counterNameScope)
                {
                }
            }
        }
    }

    public class CounterGroupItemMetadata : IDisposable
    {
        private bool _disposed;
        private IDisposable _keyScope;

        public LazyStringValue Key;
        public LazyStringValue LuceneKey;
        public LazyStringValue DocumentId;
        public LazyStringValue CounterName;
        public readonly long Etag;
        public readonly int Size;

        public CounterGroupItemMetadata(LazyStringValue key, IDisposable keyScope, LazyStringValue luceneKey, LazyStringValue documentId, LazyStringValue counterName, long etag, int size)
        {
            Key = key;
            _keyScope = keyScope;
            LuceneKey = luceneKey;
            DocumentId = documentId;
            CounterName = counterName;
            Etag = etag;
            Size = size;
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            Key?.Dispose();
            Key = null;

            _keyScope.Dispose();
            _keyScope = null;

            LuceneKey?.Dispose();
            LuceneKey = null;

            DocumentId?.Dispose();
            DocumentId = null;

            CounterName?.Dispose();
            CounterName = null;

            _disposed = true;
        }
    }

    public struct CounterPartialValue
    {
        public readonly long PartialValue;
        public readonly long Etag;
        public readonly string ChangeVector;

        public CounterPartialValue(long partialValue, long etag, string changeVector)
        {
            PartialValue = partialValue;
            Etag = etag;
            ChangeVector = changeVector;
        }
    }

    public class CounterInfo
    {
        public long Value;
        public long Etag;
        public string DbId;
        public string Name;
    }
}

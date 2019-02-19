using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Exceptions;
using static Raven.Server.Documents.DocumentsStorage;
using Constants = Voron.Global.Constants;

namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public unsafe class From16 : ISchemaUpdate
    {
        private static readonly Slice CountersTombstonesSlice;
        private static readonly Slice AllCountersEtagSlice;
        private static readonly Slice CollectionCountersEtagsSlice;
        private static readonly Slice CounterKeysSlice;
        private static readonly string CountersTombstones = "Counters.Tombstones";

        private string _dbId;

        private static readonly TableSchema LegacyCountersSchema = new TableSchema
        {
            TableType = (byte)TableType.LegacyCounter
        };

        private enum LegacyCountersTable
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

        static From16()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "AllCountersEtags", ByteStringType.Immutable, out AllCountersEtagSlice);
                Slice.From(ctx, "CollectionCountersEtags", ByteStringType.Immutable, out CollectionCountersEtagsSlice);
                Slice.From(ctx, "CounterKeys", ByteStringType.Immutable, out CounterKeysSlice);
                Slice.From(ctx, CountersTombstones, ByteStringType.Immutable, out CountersTombstonesSlice);
            }

            LegacyCountersSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)LegacyCountersTable.CounterKey,
                Count = 1,
                Name = CounterKeysSlice,
                IsGlobal = true,
            });

            LegacyCountersSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)LegacyCountersTable.Etag,
                Name = AllCountersEtagSlice,
                IsGlobal = true
            });

            LegacyCountersSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)LegacyCountersTable.Etag,
                Name = CollectionCountersEtagsSlice
            });

        }

        public bool Update(UpdateStep step)
        {
            step.DocumentsStorage.CountersStorage = new CountersStorage(step.DocumentsStorage.DocumentDatabase, step.WriteTx);

            var readTable = new Table(LegacyCountersSchema, step.ReadTx);
            if (readTable.GetTree(LegacyCountersSchema.Key) != null)
            {
                _dbId = ReadDbId(step);
                using (step.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    string currentDocId = null;
                    var batch = new Dictionary<string, List<CounterDetail>>();
                    var dbIds = new HashSet<string>();

                    foreach (var counterDetail in GetAllCounters(readTable, context))
                    {
                        if (currentDocId == counterDetail.DocumentId)
                        {
                            if (batch.TryGetValue(counterDetail.CounterName, out var list) == false)
                            {
                                list = new List<CounterDetail>();
                                batch.Add(counterDetail.CounterName, list);
                            }
                            list.Add(counterDetail);
                        }
                        else
                        {
                            if (currentDocId != null)
                            {
                                PutCounters(step, context, dbIds, batch, currentDocId);
                            }

                            currentDocId = counterDetail.DocumentId;
                            batch = new Dictionary<string, List<CounterDetail>>
                            {
                                {
                                    counterDetail.CounterName, new List<CounterDetail>
                                    {
                                        counterDetail
                                    }
                                }
                            };
                        }

                        var dbId = ExtractDbId(context, counterDetail.CounterKey);
                        dbIds.Add(dbId);

                    }

                    if (batch.Count > 0)
                    {
                        PutCounters(step, context, dbIds, batch, currentDocId);
                    }
                }

                // delete all data from LegacyCounters table
                step.WriteTx.DeleteTree(CounterKeysSlice);

            }

            var counterTombstones = step.ReadTx.OpenTable(TombstonesSchema, CountersTombstonesSlice);
            if (counterTombstones != null)
            {
                // for each counter tombstone, delete the matching
                // counter from the new table (if exists)
                using (step.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    foreach (var result in counterTombstones.SeekByPrimaryKeyPrefix(Slices.BeforeAllKeys, Slices.Empty, 0))
                    {
                        var t = new Tombstone
                        {
                            LowerId = TableValueToString(context, (int)TombstoneTable.LowerId, ref result.Value.Reader),
                            Type = *(Tombstone.TombstoneType*)result.Value.Reader.Read((int)TombstoneTable.Type, out _),
                        };

                        if (t.Type != Tombstone.TombstoneType.Counter)
                            continue;

                        DeleteCounter(step, t.LowerId, context);
                    }

                    // delete counter-tombstones from Tombstones table
                    var countersTombstoneTable = step.WriteTx.OpenTable(TombstonesSchema, CountersTombstonesSlice);
                    DeleteFromTable(context, countersTombstoneTable, TombstonesSchema.Key, tvh =>
                    {
                        var type = *(Tombstone.TombstoneType*)tvh.Reader.Read((int)TombstoneTable.Type, out _);
                        return type != Tombstone.TombstoneType.Counter;
                    });
                }
            }


            return true;
        }

        private void DeleteCounter(UpdateStep step, LazyStringValue tombstoneKey, DocumentsOperationContext context)
        {
            var (docId, counterName) = ExtractDocIdAndNameFromCounterTombstone(context, tombstoneKey);
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, docId, out Slice lowerId, out _))
            {
                BlittableJsonReaderObject doc = null;
                var docsTable = new Table(DocsSchema, step.ReadTx);
                if (docsTable.ReadByKey(lowerId, out var tvr))
                {
                    doc = new BlittableJsonReaderObject(tvr.Read((int)DocumentsTable.Data, out int size), size, context);
                }

                var collection = CollectionName.GetCollectionName(doc);
                var collectionName = new CollectionName(collection);
                var table = step.DocumentsStorage.CountersStorage.GetCountersTable(step.WriteTx, collectionName);

                if (table.ReadByKey(lowerId, out var existing) == false)
                    return;

                // (int)CountersTable.Data = 3
                var data = new BlittableJsonReaderObject(existing.Read(3, out int oldSize), oldSize, context);

                if (data.TryGet(CountersStorage.Values, out BlittableJsonReaderObject counters) == false ||
                    counters.TryGetMember(counterName, out object counterToDelete) == false ||
                    counterToDelete is LazyStringValue) // already deleted
                    return;

                var deleteCv = step.DocumentsStorage.CountersStorage.GenerateDeleteChangeVectorFromRawBlob(data, counterToDelete as BlittableJsonReaderObject.RawBlob);
                counters.Modifications = new DynamicJsonValue(counters) {[counterName] = deleteCv};

                using (var old = data)
                {
                    data = context.ReadObject(data, null, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                }

                var newEtag = step.DocumentsStorage.GenerateNextEtag();
                var newChangeVector = ChangeVectorUtils.NewChangeVector(step.DocumentsStorage.DocumentDatabase.ServerStore.NodeTag, newEtag, _dbId);
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
        }

        private static IEnumerable<CounterDetail> GetAllCounters(Table table, DocumentsOperationContext ctx)
        {
            foreach (var result in table.SeekByPrimaryKeyPrefix(Slices.BeforeAllKeys, Slices.Empty, 0))
            {
                yield return TableValueToCounterDetail(ctx, result.Value.Reader);
            }
        }

        private void PutCounters(UpdateStep step, DocumentsOperationContext context, HashSet<string> dbIds, Dictionary<string, List<CounterDetail>> batch, string docId)
        {
            BlittableJsonReaderObject doc = null;
            using (DocumentIdWorker.GetSliceFromId(context, docId, out Slice lowerId))
            {
                var docsTable = new Table(DocsSchema, step.ReadTx);
                if (docsTable.ReadByKey(lowerId, out var tvr))
                {
                    doc = new BlittableJsonReaderObject(tvr.Read((int)DocumentsTable.Data, out int size), size, context);
                }
            }

            var collection = CollectionName.GetCollectionName(doc);
            var collectionName = new CollectionName(collection);

            var table = step.DocumentsStorage.CountersStorage.GetCountersTable(step.WriteTx, collectionName);

            var data = WriteNewCountersDocument(context, dbIds.ToList(), batch);
            var etag = step.DocumentsStorage.GenerateNextEtag();
            var changeVector = ChangeVectorUtils.NewChangeVector(
                step.DocumentsStorage.DocumentDatabase.ServerStore.NodeTag, etag, _dbId);

            using (table.Allocate(out TableValueBuilder tvb))
            {
                using (Slice.From(context.Allocator, changeVector, out var cv))
                using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
                using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, docId, out Slice counterKey, out _))
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

        private static string ReadDbId(UpdateStep step)
        {
            var metadataTree = step.WriteTx.ReadTree(Constants.MetadataTreeNameSlice);
            if (metadataTree == null)
                VoronUnrecoverableErrorException.Raise(step.WriteTx.LowLevelTransaction,
                    "Could not find metadata tree in database, possible mismatch / corruption?");

            Debug.Assert(metadataTree != null);
            // ReSharper disable once PossibleNullReferenceException
            var dbId = metadataTree.Read("db-id");
            if (dbId == null)
                VoronUnrecoverableErrorException.Raise(step.WriteTx.LowLevelTransaction,
                    "Could not find db id in metadata tree, possible mismatch / corruption?");

            var buffer = new byte[16];
            Debug.Assert(dbId != null);
            // ReSharper disable once PossibleNullReferenceException
            var dbIdBytes = dbId.Reader.Read(buffer, 0, 16);
            if (dbIdBytes != 16)
                VoronUnrecoverableErrorException.Raise(step.WriteTx.LowLevelTransaction,
                    "The db id value in metadata tree wasn't 16 bytes in size, possible mismatch / corruption?");

            var databaseGuidId = new Guid(buffer);
            var dbIdStr = new string(' ', 22);

            fixed (char* pChars = dbIdStr)
            {
                var result = Base64.ConvertToBase64ArrayUnpadded(pChars, (byte*)&databaseGuidId, 0, 16);
                Debug.Assert(result == 22);
            }

            return dbIdStr;
        }

        private static CounterDetail TableValueToCounterDetail(JsonOperationContext context, TableValueReader tvr)
        {
            var (doc, name) = ExtractDocIdAndNameFromLegacyCounter(context, ref tvr);

            return new CounterDetail
            {
                CounterKey = TableValueToString(context, (int)LegacyCountersTable.CounterKey, ref tvr),
                DocumentId = doc,
                CounterName = name,
                TotalValue = TableValueToLong((int)LegacyCountersTable.Value, ref tvr),
            };
        }

        private static (LazyStringValue Doc, LazyStringValue Name) ExtractDocIdAndNameFromLegacyCounter(JsonOperationContext context, ref TableValueReader tvr)
        {
            var p = tvr.Read((int)LegacyCountersTable.CounterKey, out var size);
            Debug.Assert(size > CountersStorage.DbIdAsBase64Size + 2 /* record separators */);
            int sizeOfDocId = 0;
            for (; sizeOfDocId < size; sizeOfDocId++)
            {
                if (p[sizeOfDocId] == SpecialChars.RecordSeparator)
                    break;
            }

            var doc = context.AllocateStringValue(null, p, sizeOfDocId);
            var name = TableValueToId(context, (int)LegacyCountersTable.Name, ref tvr);
            return (doc, name);
        }

        private static (LazyStringValue Doc, LazyStringValue Name) ExtractDocIdAndNameFromCounterTombstone(JsonOperationContext context, LazyStringValue counterKey)
        {
            var p = counterKey.Buffer;
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

        private static LazyStringValue ExtractDbId(JsonOperationContext context, LazyStringValue counterKey)
        {
            var offset = counterKey.Size - CountersStorage.DbIdAsBase64Size;
            var p = counterKey.Buffer + offset;

            return context.AllocateStringValue(null, p, CountersStorage.DbIdAsBase64Size);
        }

        private static BlittableJsonReaderObject WriteNewCountersDocument(DocumentsOperationContext context, List<string> dbIds, Dictionary<string, List<CounterDetail>> batch)
        {
            var toDispose = new List<IDisposable>();
            try
            {
                BlittableJsonReaderObject data;
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();

                    builder.WritePropertyName(CountersStorage.DbIds);

                    builder.StartWriteArray();

                    foreach (var dbId in dbIds)
                    {
                        builder.WriteValue(dbId);
                    }

                    builder.WriteArrayEnd();

                    builder.WritePropertyName(CountersStorage.Values);
                    builder.StartWriteObject();

                    foreach (var kvp in batch)
                    {
                        builder.WritePropertyName(kvp.Key);

                        var maxDbIdIndex = GetMaxDbIdIndex(context, dbIds, kvp.Value);

                        toDispose.Add(context.Allocator.Allocate((maxDbIdIndex + 1) * CountersStorage.SizeOfCounterValues, out var newVal));

                        WriteRawBlob(context, dbIds, kvp.Value, newVal, builder);
                    }

                    builder.WriteObjectEnd();

                    builder.WriteObjectEnd();
                    builder.FinalizeDocument();

                    data = builder.CreateReader();

                }

                return data;
            }

            finally 
            {
                foreach (var scope in toDispose)
                {
                    scope.Dispose();                 
                }
            }

        }

        private static void WriteRawBlob(DocumentsOperationContext context, List<string> dbIds, List<CounterDetail> counters, ByteString newVal, ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer> builder)
        {
            foreach (var counterDetail in counters)
            {
                var dbId = ExtractDbId(context, counterDetail.CounterKey);

                int dbIdIndex;
                for (dbIdIndex = 0; dbIdIndex < dbIds.Count; dbIdIndex++)
                {
                    if (dbIds[dbIdIndex] == dbId)
                        break;
                }

                var counterEtag = context.DocumentDatabase.DocumentsStorage.GenerateNextEtag();

                var newEntry = (CountersStorage.CounterValues*)newVal.Ptr + dbIdIndex;
                newEntry->Value = counterDetail.TotalValue;
                newEntry->Etag = counterEtag;
            }

            builder.WriteRawBlob(newVal.Ptr, newVal.Length);
        }

        private static int GetMaxDbIdIndex(DocumentsOperationContext context, List<string> dbIds, List<CounterDetail> counters)
        {
            var maxDbIdIndex = -1;
            foreach (var counter in counters)
            {
                var dbId = ExtractDbId(context, counter.CounterKey);

                int dbIdIndex;
                for (dbIdIndex = 0; dbIdIndex < dbIds.Count; dbIdIndex++)
                {
                    if (dbIds[dbIdIndex] == dbId)
                        break;
                }

                maxDbIdIndex = Math.Max(maxDbIdIndex, dbIdIndex);
            }

            return maxDbIdIndex;
        }

        private void DeleteFromTable(DocumentsOperationContext context, Table table, TableSchema.SchemaIndexDef pk, Func<Table.TableValueHolder, bool> shouldSkip = null)
        {
            Table.TableValueHolder tableValueHolder = null;
            var tree = table.GetTree(pk);
            var last = Slices.BeforeAllKeys;

            while (true)
            {
                using (var it = tree.Iterate(true))
                {
                    it.SetRequiredPrefix(last);
                    if (it.Seek(it.RequiredPrefix) == false)
                        return;

                    while (true)
                    {
                        long id = it.CreateReaderForCurrent().ReadLittleEndianInt64();

                        if (shouldSkip != null)
                        {
                            var ptr = table.DirectRead(id, out int size);
                            if (tableValueHolder == null)
                                tableValueHolder = new Table.TableValueHolder();

                            tableValueHolder.Reader = new TableValueReader(id, ptr, size);

                            if (shouldSkip.Invoke(tableValueHolder))
                            {
                                last = it.CurrentKey.Clone(context.Allocator);

                                if (it.MoveNext() == false)
                                    return;

                                continue;
                            }
                        }

                        table.Delete(id);
                        break;
                    }
                }
            }
        }
    }
}

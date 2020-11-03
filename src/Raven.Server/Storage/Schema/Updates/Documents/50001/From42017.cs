using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data;
using Voron.Data.Tables;
using static Raven.Server.Documents.CountersStorage;

namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public class From42017 : ISchemaUpdate
    {
        public int From => 42_017;

        public int To => 50_001;

        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        internal static int NumberOfCounterGroupsToMigrateInSingleTransaction = 10_000;

        internal static int MaxSizeToMigrateInSingleTransaction = 64 * 1024 * 1024;

        private string _dbId;

        private class CounterBatchUpdate
        {
            public readonly List<CounterReplicationItem> Counters = new List<CounterReplicationItem>();

            public void Clear()
            {
                foreach (var counterGroup in Counters)
                {
                    counterGroup.Id.Dispose();
                    counterGroup.Collection.Dispose();
                    counterGroup.Dispose();
                }

                Counters.Clear();
            }
        }

        public bool Update(UpdateStep step)
        {
            UpdateSchemaForDocumentsAndRevisions(step);

            var readTable = new Table(CountersSchema, step.WriteTx);
            var countersTree = readTable.GetTree(CountersSchema.Key);
            if (countersTree == null)
                return true;

            step.DocumentsStorage.CountersStorage = new CountersStorage(step.DocumentsStorage.DocumentDatabase, step.WriteTx);

            // this schema update uses DocumentsStorage.GenerateNextEtag() so we need to read and set LastEtag in storage
            _dbId = From41016.ReadDbId(step);
            step.DocumentsStorage.InitializeLastEtag(step.ReadTx);

            var entriesToDelete = new List<long>();
            var batch = new CounterBatchUpdate();
            string currentDocId = null;
            string currentPrefix = null;
            CollectionName currentCollection = null;
            var done = false;

            // 4.2 counters processing

            while (done == false)
            {
                readTable = new Table(CountersSchema, step.ReadTx);

                var processedInCurrentTx = 0;

                using (step.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    context.TransactionMarkerOffset = (short)step.WriteTx.LowLevelTransaction.Id;

                    var commit = false;

                    foreach (var counterGroup in GetCounters(readTable, context, currentPrefix))
                    {
                        if (currentDocId == counterGroup.Id)
                        {
                            batch.Counters.Add(counterGroup);
                        }
                        else
                        {
                            if (currentDocId != null)
                            {
                                // finished processing all counter groups for current document
                                DeleteProcessedEntries(step, entriesToDelete, currentCollection);
                                PutCounterGroups(step, batch, context, currentCollection);
                                UpdateDocumentCounters(step, context, currentDocId, currentCollection);

                                if (processedInCurrentTx >= NumberOfCounterGroupsToMigrateInSingleTransaction || 
                                    context.AllocatedMemory >= MaxSizeToMigrateInSingleTransaction)
                                {
                                    commit = true;
                                    currentPrefix = counterGroup.Id;
                                    break;
                                }
                            }

                            // start new batch
                            currentDocId = counterGroup.Id;
                            currentCollection = new CollectionName(counterGroup.Collection);
                            batch.Counters.Add(counterGroup);
                        }

                        // add table etag to delete-list
                        entriesToDelete.Add(counterGroup.Etag);

                        processedInCurrentTx++;
                    }

                    if (commit)
                    {
                        step.Commit(context);
                        step.RenewTransactions();
                        currentDocId = null;
                        continue;
                    }

                    if (batch.Counters.Count > 0)
                    {
                        DeleteProcessedEntries(step, entriesToDelete, currentCollection);
                        PutCounterGroups(step, batch, context, currentCollection);
                        UpdateDocumentCounters(step, context, currentDocId, currentCollection);
                    }

                    done = true;
                }
            }

            return true;
        }

        private static void UpdateSchemaForDocumentsAndRevisions(UpdateStep step)
        {
            using var _ = step.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
            var collections = step.WriteTx.OpenTable(DocumentsStorage.CollectionsSchema, DocumentsStorage.CollectionsSlice);
            foreach (var tvr in collections.SeekByPrimaryKey(Slices.BeforeAllKeys, 0))
            {
                var collection = DocumentsStorage.TableValueToId(context, (int)DocumentsStorage.CollectionsTable.Name, ref tvr.Reader);
                var collectionName = new CollectionName(collection);
                var tableTree = step.WriteTx.CreateTree(collectionName.GetTableName(CollectionTableType.Documents), RootObjectType.Table);
                DocumentsStorage.DocsSchema.SerializeSchemaIntoTableTree(tableTree);

                var revisionsTree = step.WriteTx.ReadTree(collectionName.GetTableName(CollectionTableType.Revisions), RootObjectType.Table);
                if (revisionsTree != null)
                    RevisionsStorage.RevisionsSchema.SerializeSchemaIntoTableTree(revisionsTree);
            }
        }

        private static unsafe void UpdateDocumentCounters(UpdateStep step, DocumentsOperationContext context, string docId, CollectionName collection)
        {
            using (DocumentIdWorker.GetSliceFromId(context, docId, out Slice lowerDocId))
            {
                var table = step.WriteTx.OpenTable(DocumentsStorage.DocsSchema, collection.GetTableName(CollectionTableType.Documents));
                if (table.ReadByKey(lowerDocId, out var tvr) == false)
                    return; // document doesn't exists

                var tableId = tvr.Id;
                var counterNames = step.DocumentsStorage.CountersStorage.GetCountersForDocument(context, step.WriteTx, docId).ToList();
                var doc = step.DocumentsStorage.TableValueToDocument(context, ref tvr, skipValidationInDebug: true);
                if (doc.TryGetMetadata(out var metadata) == false)
                {
                    if (counterNames.Count > 0)
                    {
                        doc.Flags |= DocumentFlags.HasCounters;

                        var dvj = new DynamicJsonValue
                        {
                            [Constants.Documents.Metadata.Counters] = counterNames
                        };
                        doc.Data.Modifications = new DynamicJsonValue(doc.Data)
                        {
                            [Constants.Documents.Metadata.Key] = dvj
                        };
                    }
                    else
                    {
                        doc.Flags &= ~DocumentFlags.HasCounters;
                    }
                }
                else
                {
                    metadata.Modifications = new DynamicJsonValue(metadata);
                    if (counterNames.Count == 0)
                    {
                        doc.Flags &= ~DocumentFlags.HasCounters;
                        metadata.Modifications.Remove(Constants.Documents.Metadata.Counters);
                    }
                    else
                    {
                        doc.Flags |= DocumentFlags.HasCounters;
                        metadata.Modifications[Constants.Documents.Metadata.Counters] = counterNames;
                    }

                    if (metadata.TryGet(Constants.Documents.Metadata.RevisionCounters, out object _))
                    {
                        // remove "@counters-snapshot" from metadata
                        metadata.Modifications.Remove(Constants.Documents.Metadata.RevisionCounters);
                    }
                }

                using (doc.Data)
                {
                    doc.Data = context.ReadObject(doc.Data, docId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                }

                using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, doc.Id, out Slice lowerId, out Slice idPtr))
                using (Slice.From(context.Allocator, doc.ChangeVector, out var cv))
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(lowerId);
                    tvb.Add(Bits.SwapBytes(doc.Etag));
                    tvb.Add(idPtr);
                    tvb.Add(doc.Data.BasePointer, doc.Data.Size);
                    tvb.Add(cv.Content.Ptr, cv.Size);
                    tvb.Add(doc.LastModified.Ticks);
                    tvb.Add((int)doc.Flags);
                    tvb.Add(doc.TransactionMarker);

                    table.Update(tableId, tvb);
                }
            }
        }

        private void PutCounterGroups(UpdateStep step, CounterBatchUpdate batch, DocumentsOperationContext context, CollectionName collection)
        {
            foreach (var cg in batch.Counters)
            {
                PutCounters(context, step, cg.Id, cg.ChangeVector, cg.Values, collection);
            }

            batch.Clear();
        }

        private static void DeleteProcessedEntries(UpdateStep step, List<long> toDelete, CollectionName collection)
        {
            var table = step.WriteTx.OpenTable(CountersSchema, collection.GetTableName(CollectionTableType.CounterGroups));
            foreach (var etag in toDelete)
            {
                table.DeleteByIndex(CountersSchema.FixedSizeIndexes[CollectionCountersEtagsSlice], etag);
            }

            toDelete.Clear();
        }

        private static IEnumerable<CounterReplicationItem> GetCounters(Table table, DocumentsOperationContext ctx, string prefix)
        {
            if (prefix == null)
            {
                foreach (var result in table.SeekByPrimaryKeyPrefix(Slices.BeforeAllKeys, Slices.Empty, skip: 0))
                {
                    yield return CreateReplicationBatchItem(ctx, ref result.Value.Reader);
                }

                yield break;
            }

            using (DocumentIdWorker.GetSliceFromId(ctx, prefix, out Slice documentKeyPrefix, separator: SpecialChars.RecordSeparator))
            {
                if (table.SeekOnePrimaryKeyPrefix(documentKeyPrefix, out var reader))
                {
                    // first entry with prefix
                    yield return CreateReplicationBatchItem(ctx, ref reader);
                }

                foreach (var result in table.SeekByPrimaryKeyPrefix(Slices.BeforeAllKeys, documentKeyPrefix, skip: 0))
                {
                    //all entries that start after prefix
                    yield return CreateReplicationBatchItem(ctx, ref result.Value.Reader);
                }
            }
        }

        private readonly ObjectPool<Dictionary<LazyStringValue, PutCountersData>> _dictionariesPool
            = new ObjectPool<Dictionary<LazyStringValue, PutCountersData>>(() => new Dictionary<LazyStringValue, PutCountersData>(LazyStringValueComparer.Instance));

        private unsafe void PutCounters(DocumentsOperationContext context, UpdateStep step, string documentId, string changeVector, BlittableJsonReaderObject sourceData, CollectionName collection)
        {
            using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice lowerId))
            {
                var docsTable = new Table(DocumentsStorage.DocsSchema, step.ReadTx);
                if (docsTable.ReadByKey(lowerId, out _) == false)
                {
                    // document does not exist
                    return;
                }
            }

            var entriesToUpdate = _dictionariesPool.Allocate();
            try
            {
                var table = step.DocumentsStorage.CountersStorage.GetCountersTable(step.WriteTx, collection);

                using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice documentKeyPrefix, separator: SpecialChars.RecordSeparator))
                {
                    if (sourceData.TryGet(Values, out BlittableJsonReaderObject sourceCounters) == false)
                    {
                        throw new InvalidDataException($"Counter-Group document '{documentId}' is missing '{Values}' property. Shouldn't happen");
                    }

                    if (sourceData.TryGet(DbIds, out BlittableJsonReaderArray sourceDbIds) == false)
                    {
                        throw new InvalidDataException($"Counter-Group document '{documentId}' is missing '{DbIds}' property. Shouldn't happen");
                    }

                    sourceData.TryGet(CounterNames, out BlittableJsonReaderObject sourceCounterNames);
                    BlittableJsonReaderObject data;

                    if (table.SeekOnePrimaryKeyPrefix(documentKeyPrefix, out _) == false)
                    {
                        // simplest case of having no counters for this document, can use the raw data from the source as-is

                        CreateFirstEntry(context, documentId, changeVector, sourceData, sourceCounterNames, sourceCounters, collection, table, documentKeyPrefix);
                        return;
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

                            using (var counterGroupKey = DocumentsStorage.TableValueToString(context, (int)CountersTable.CounterKey, ref tvr))
                            {
                                if (entriesToUpdate.TryGetValue(counterGroupKey, out var putCountersData))
                                {
                                    data = putCountersData.Data;
                                }
                                else
                                {
                                    var existingChangeVector = DocumentsStorage.TableValueToChangeVector(context, (int)CountersTable.ChangeVector, ref tvr);

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

                                if (step.DocumentsStorage.CountersStorage.MergeCounterIfNeeded(context, localCounters, ref prop, putCountersData.DbIdsHolder, sourceDbIds, sourceCounterNames, originalNames,
                                        out _, out var changeType) == false)
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
                                    SplitCounterGroup(context, collection, table, documentKeyPrefix, countersGroupKey, localCounters, dbIds, originalNames,
                                        changeVectorToSave, _dbId);
                                }

                                continue;
                            }

                            var etag = context.DocumentDatabase.DocumentsStorage.GenerateNextEtag();
                            using (Slice.From(context.Allocator, changeVectorToSave, out var cv))
                            using (DocumentIdWorker.GetStringPreserveCase(context, collection.Name, out Slice collectionSlice))
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
            }
            finally
            {
                foreach (var s in step.DocumentsStorage.CountersStorage._counterModificationMemoryScopes)
                {
                    s.Dispose();
                }

                foreach (var kvp in entriesToUpdate)
                {
                    kvp.Value.KeyScope.Dispose();
                    kvp.Key.Dispose();
                }

                step.DocumentsStorage.CountersStorage._counterModificationMemoryScopes.Clear();
                entriesToUpdate.Clear();
                _dictionariesPool.Free(entriesToUpdate);
            }
        }

        private static unsafe void CreateFirstEntry(DocumentsOperationContext context, string documentId, string changeVector, BlittableJsonReaderObject sourceData,
            BlittableJsonReaderObject sourceCounterNames, BlittableJsonReaderObject sourceCounters, CollectionName collectionName, Table table, Slice documentKeyPrefix)
        {
            if (sourceCounterNames == null)
            {
                // 4.2 source
                // need to create @names property and to lower the property names in 'sourceCounters'

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

                sourceData.Modifications = new DynamicJsonValue(sourceData) { [CounterNames] = originalNames };
            }

            BlittableJsonReaderObject data = context.ReadObject(sourceData, documentId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            using (Slice.From(context.Allocator, changeVector, out var cv))
            using (DocumentIdWorker.GetStringPreserveCase(context, collectionName.Name, out Slice collectionSlice))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(documentKeyPrefix);
                tvb.Add(Bits.SwapBytes(context.DocumentDatabase.DocumentsStorage.GenerateNextEtag()));
                tvb.Add(cv);
                tvb.Add(data.BasePointer, data.Size);
                tvb.Add(collectionSlice);
                tvb.Add(context.GetTransactionMarker());

                table.Set(tvb);
            }
        }
        

        private static unsafe void SplitCounterGroup(DocumentsOperationContext context, CollectionName collectionName, Table table, Slice documentKeyPrefix,
    Slice countersGroupKey, BlittableJsonReaderObject values, BlittableJsonReaderArray dbIds, BlittableJsonReaderObject originalNames, string changeVector, string dbId)
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
                for (; firstChange < lastPropertyFirst.Name.Length; firstChange++)
                {
                    if (firstPropertySnd.Name[firstChange] != lastPropertyFirst.Name[firstChange])
                        break;
                }

                var etag = context.DocumentDatabase.DocumentsStorage.GenerateNextEtag();
                var cv2 = ChangeVectorUtils.NewChangeVector(
                    context.DocumentDatabase.ServerStore.NodeTag, etag, dbId);

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

        private static (BlittableJsonReaderObject First, BlittableJsonReaderObject Second) SplitCounterDocument(DocumentsOperationContext context,
            BlittableJsonReaderObject values, BlittableJsonReaderArray dbIds, BlittableJsonReaderObject originalNames)
        {
            // here we rely on the internal sort order of the blittables, because we go through them
            // in lexical order
            var fst = CreateHalfDocument(context, values, 0, values.Count / 2, dbIds, originalNames);
            var snd = CreateHalfDocument(context, values, values.Count / 2, values.Count, dbIds, originalNames);

            return (fst, snd);
        }

        private static unsafe BlittableJsonReaderObject CreateHalfDocument(DocumentsOperationContext context, BlittableJsonReaderObject values, int start, int end,
            BlittableJsonReaderArray dbIds, BlittableJsonReaderObject originalNames)
        {
            BlittableJsonReaderObject.PropertyDetails prop = default;

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
                        builder.WriteRawBlob(blob.Address, blob.Length);
                    else if (prop.Value is LazyStringValue lsv)
                        builder.WriteValue(lsv); // delete counter
                    else
                        throw new InvalidDataException("Unknown type: " + prop.Token + " when trying to split counter doc");
                }

                builder.WriteObjectEnd();

                builder.WritePropertyName(DbIds);

                builder.StartWriteArray();

                for (var index = 0; index < dbIds.Length; index++)
                {
                    var item = (LazyStringValue)dbIds[index];
                    builder.WriteValue(item);
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
    }
}

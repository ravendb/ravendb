// -----------------------------------------------------------------------
//  <copyright file="IndexingStorageActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Globalization;
using System.IO;
using Mono.CSharp;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Storage.Voron.StorageActions.StructureSchemas;
using Voron.Trees;

namespace Raven.Database.Storage.Voron.StorageActions
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;

    using Raven.Abstractions;
    using Raven.Abstractions.Data;
    using Raven.Abstractions.Exceptions;
    using Raven.Abstractions.Extensions;
    using Raven.Database.Impl;
    using Raven.Database.Indexing;
    using Raven.Database.Storage.Voron.Impl;
    using Raven.Json.Linq;

    using global::Voron;
    using global::Voron.Impl;

    internal class IndexingStorageActions : StorageActionsBase, IIndexingStorageActions
    {
        private readonly TableStorage tableStorage;

        private readonly Reference<WriteBatch> writeBatch;

        private readonly IUuidGenerator generator;

        private readonly IStorageActionsAccessor currentStorageActionsAccessor;
        private readonly GeneralStorageActions generalStorageActions;

        public IndexingStorageActions(TableStorage tableStorage, IUuidGenerator generator, Reference<SnapshotReader> snapshot, Reference<WriteBatch> writeBatch, IStorageActionsAccessor storageActionsAccessor, IBufferPool bufferPool, GeneralStorageActions generalStorageActions)
            : base(snapshot, bufferPool)
        {
            this.tableStorage = tableStorage;
            this.generator = generator;
            this.writeBatch = writeBatch;
            this.currentStorageActionsAccessor = storageActionsAccessor;
            this.generalStorageActions = generalStorageActions;
        }

        public void Dispose()
        {
        }

        public IEnumerable<IndexStats> GetIndexesStats()
        {
            using (var indexingStatsIterator = tableStorage.IndexingStats.Iterate(Snapshot, writeBatch.Value))
            {
                if (!indexingStatsIterator.Seek(Slice.BeforeAllKeys))
                    yield break;

                do
                {
                    var key = indexingStatsIterator.CurrentKey.ToString();
                    var keySlice = (Slice)key;

                    ushort version;
                    var indexStats = indexingStatsIterator.ReadStructForCurrent(tableStorage.IndexingStats.Schema);

                    var reduceStats = LoadStruct(tableStorage.ReduceStats, keySlice, out version);
                    var lastIndexedEtag = LoadStruct(tableStorage.LastIndexedEtags, keySlice, out version);
                    var priority = ReadPriority(key);
                    var touches = ReadTouches(key);

                    yield return GetIndexStats(indexStats, reduceStats, lastIndexedEtag, priority, touches);
                }
                while (indexingStatsIterator.MoveNext());
            }
        }

        public IndexStats GetIndexStats(int id)
        {
            var key = CreateKey(id);
            var keySlice = new Slice(key);

            ushort version;

            var indexStats = LoadStruct(tableStorage.IndexingStats, keySlice, out version);
            var reduceStats = LoadStruct(tableStorage.ReduceStats, keySlice, out version);
            var lastIndexedEtags = LoadStruct(tableStorage.LastIndexedEtags, keySlice, out version);
            var priority = ReadPriority(key);
            var touches = ReadTouches(key);

            return GetIndexStats(indexStats, reduceStats, lastIndexedEtags, priority, touches);
        }

        public void AddIndex(int id, bool createMapReduce)
        {
            var key = new Slice(CreateKey(id));

            if (tableStorage.IndexingStats.Contains(Snapshot, key, writeBatch.Value))
                throw new ArgumentException(string.Format("There is already an index with the name: '{0}'", id));

            tableStorage.IndexingStats.AddStruct(
                writeBatch.Value,
                key,
                new Structure<IndexingWorkStatsFields>(tableStorage.IndexingStats.Schema)
                    .Set(IndexingWorkStatsFields.IndexId, id)
                    .Set(IndexingWorkStatsFields.IndexingAttempts, 0)
                    .Set(IndexingWorkStatsFields.IndexingSuccesses, 0)
                    .Set(IndexingWorkStatsFields.IndexingErrors, 0)
                    .Set(IndexingWorkStatsFields.CreatedTimestamp, SystemTime.UtcNow.ToBinary())
                    .Set(IndexingWorkStatsFields.LastIndexingTime, DateTime.MinValue.ToBinary()),
                0);

            var idKey = CreateKey(id);

            tableStorage.IndexingMetadata.Add(writeBatch.Value, (Slice)AppendToKey(idKey, "priority"), BitConverter.GetBytes(1), 0);
            tableStorage.IndexingMetadata.Increment(writeBatch.Value, (Slice)AppendToKey(idKey, "touches"), 0, 0);

            tableStorage.ReduceStats.AddStruct(
                writeBatch.Value,
                key,
                new Structure<ReducingWorkStatsFields>(tableStorage.ReduceStats.Schema)
                    .Set(ReducingWorkStatsFields.ReduceAttempts, createMapReduce ? 0 : -1)
                    .Set(ReducingWorkStatsFields.ReduceSuccesses, createMapReduce ? 0 : -1)
                    .Set(ReducingWorkStatsFields.ReduceErrors, createMapReduce ? 0 : -1)
                    .Set(ReducingWorkStatsFields.LastReducedEtag, createMapReduce ? Etag.Empty.ToByteArray() : Etag.InvalidEtag.ToByteArray())
                    .Set(ReducingWorkStatsFields.LastReducedTimestamp, createMapReduce ? DateTime.MinValue.ToBinary() : -1L),
                0);

            tableStorage.LastIndexedEtags.AddStruct(
                writeBatch.Value,
                key,
                new Structure<LastIndexedStatsFields>(tableStorage.LastIndexedEtags.Schema)
                    .Set(LastIndexedStatsFields.IndexId, id)
                    .Set(LastIndexedStatsFields.LastEtag, Etag.Empty.ToByteArray())
                    .Set(LastIndexedStatsFields.LastTimestamp, DateTime.MinValue.ToBinary()),
                0);
        }

        public void PrepareIndexForDeletion(int id)
        {
            var key = CreateKey(id);

            tableStorage.IndexingStats.Delete(writeBatch.Value, key);
            tableStorage.IndexingMetadata.Delete(writeBatch.Value, AppendToKey(key, "priority"));
            tableStorage.IndexingMetadata.Delete(writeBatch.Value, AppendToKey(key, "touches"));
            tableStorage.ReduceStats.Delete(writeBatch.Value, key);
            tableStorage.LastIndexedEtags.Delete(writeBatch.Value, key);
        }

        public void DeleteIndex(int id, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();

            RemoveAllDocumentReferencesByView(id, token);

            var mappedResultsStorageActions = (MappedResultsStorageActions)currentStorageActionsAccessor.MapReduce;

            mappedResultsStorageActions.DeleteMappedResultsForView(id, token);
            mappedResultsStorageActions.DeleteScheduledReductionForView(id, token);
            mappedResultsStorageActions.RemoveReduceResultsForView(id, token);
        }

        public void SetIndexPriority(int id, IndexingPriority priority)
        {
            tableStorage.IndexingMetadata.Add(writeBatch.Value, CreateKey(id, "priority"), BitConverter.GetBytes((int)priority));
        }

        public void SetIndexesPriority(int[] ids, IndexingPriority[] priorities)
        {
            for (int i = 0; i < ids.Length; i++)
            {
                var id = ids[i];
                var priority = priorities[i];
                tableStorage.IndexingMetadata.Add(writeBatch.Value, CreateKey(id, "priority"), BitConverter.GetBytes((int)priority));
            }
        }
        public IndexFailureInformation GetFailureRate(int id)
        {
            var key = new Slice(CreateKey(id));

            ushort version;
            var indexStats = LoadStruct(tableStorage.IndexingStats, key, out version);
            var reduceStats = LoadStruct(tableStorage.ReduceStats, key, out version);

            var reduceAttempts = reduceStats.ReadInt(ReducingWorkStatsFields.ReduceAttempts);
            var reduceErrors = reduceStats.ReadInt(ReducingWorkStatsFields.ReduceErrors);
            var reduceSuccesses = reduceStats.ReadInt(ReducingWorkStatsFields.ReduceSuccesses);

            var indexFailureInformation = new IndexFailureInformation
            {
                Attempts = indexStats.ReadInt(IndexingWorkStatsFields.IndexingAttempts),
                Errors = indexStats.ReadInt(IndexingWorkStatsFields.IndexingErrors),
                Successes = indexStats.ReadInt(IndexingWorkStatsFields.IndexingSuccesses),
                ReduceAttempts = reduceAttempts == -1 ? (int?)null : reduceAttempts,
                ReduceErrors = reduceErrors == -1 ? (int?)null : reduceErrors,
                ReduceSuccesses = reduceSuccesses == -1 ? (int?)null : reduceSuccesses,
                Id = indexStats.ReadInt(IndexingWorkStatsFields.IndexId)
            };

            return indexFailureInformation;
        }

        public void UpdateLastIndexed(int id, Etag etag, DateTime timestamp)
        {
            var key = new Slice(CreateKey(id));

            var version = tableStorage.LastIndexedEtags.ReadVersion(Snapshot, key, writeBatch.Value);

            if(version == null)
                throw new IndexDoesNotExistsException(string.Format("There is no index with the name: '{0}'", id));

            var indexStats = new Structure<LastIndexedStatsFields>(tableStorage.LastIndexedEtags.Schema)
                .Set(LastIndexedStatsFields.IndexId, id)
                .Set(LastIndexedStatsFields.LastEtag, etag.ToByteArray())
                .Set(LastIndexedStatsFields.LastTimestamp, timestamp.ToBinary());

            tableStorage.LastIndexedEtags.AddStruct(writeBatch.Value, key, indexStats, version);
        }

        public void UpdateLastReduced(int id, Etag etag, DateTime timestamp)
        {
            var key = (Slice)CreateKey(id);

            ushort version;
            var reduceStats = LoadStruct(tableStorage.ReduceStats, key, out version);

            if (Etag.Parse(reduceStats.ReadBytes(ReducingWorkStatsFields.LastReducedEtag)).CompareTo(etag) >= 0)
                return;

            var updated = new Structure<ReducingWorkStatsFields>(tableStorage.ReduceStats.Schema)
                .Set(ReducingWorkStatsFields.LastReducedEtag, etag.ToByteArray())
                .Set(ReducingWorkStatsFields.LastReducedTimestamp, timestamp.ToBinary());

            tableStorage.ReduceStats.AddStruct(writeBatch.Value, key, updated, version);
        }

        public void TouchIndexEtag(int id)
        {
            Slice key = (Slice) CreateKey(id, "touches");
            var readResult = tableStorage.IndexingMetadata.Read(Snapshot, key, writeBatch.Value);
            if (readResult == null)
            {
                // index doesn't exist
                return;
            }

            tableStorage.IndexingMetadata.Increment(writeBatch.Value, key, 1);
        }

        public void UpdateIndexingStats(int id, IndexingWorkStats stats)
        {
            var key = (Slice) CreateKey(id);

            var version = tableStorage.IndexingStats.ReadVersion(Snapshot, key, writeBatch.Value);

            if (version == null)
                throw new IndexDoesNotExistsException(string.Format("There is no index with the name: '{0}'", id));

            var indexStats = new Structure<IndexingWorkStatsFields>(tableStorage.IndexingStats.Schema)
                .Set(IndexingWorkStatsFields.IndexId, id)
                .Increment(IndexingWorkStatsFields.IndexingAttempts, stats.IndexingAttempts)
                .Increment(IndexingWorkStatsFields.IndexingSuccesses, stats.IndexingSuccesses)
                .Increment(IndexingWorkStatsFields.IndexingErrors, stats.IndexingErrors)
                .Set(IndexingWorkStatsFields.LastIndexingTime, SystemTime.UtcNow.ToBinary());

            tableStorage.IndexingStats.AddStruct(writeBatch.Value, key, indexStats, version);
        }

        public void UpdateReduceStats(int id, IndexingWorkStats stats)
        {
            var key = (Slice) CreateKey(id);

            var existingStats = tableStorage.ReduceStats.ReadStruct(Snapshot, key, writeBatch.Value);

            var version = existingStats.Version;

            var updated = new Structure<ReducingWorkStatsFields>(tableStorage.ReduceStats.Schema)
                .Increment(ReducingWorkStatsFields.ReduceAttempts, stats.ReduceAttempts)
                .Increment(ReducingWorkStatsFields.ReduceSuccesses, stats.ReduceSuccesses)
                .Increment(ReducingWorkStatsFields.ReduceErrors, stats.ReduceErrors)
                .Set(ReducingWorkStatsFields.LastReducedEtag, existingStats.Reader.ReadBytes(ReducingWorkStatsFields.LastReducedEtag))
                .Set(ReducingWorkStatsFields.LastReducedTimestamp, existingStats.Reader.ReadLong(ReducingWorkStatsFields.LastReducedTimestamp));

            tableStorage.ReduceStats.AddStruct(writeBatch.Value, key, updated, version);
        }

        public void RemoveAllDocumentReferencesFrom(string key)
        {
            RemoveDocumentReferenceByKey((Slice)key);
        }

        public void RemoveAllDocumentReferencesByView(int view, CancellationToken token)
        {
            var documentReferencesByView = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByView);
            RemoveDocumentReference(() => documentReferencesByView.MultiRead(Snapshot, CreateKey(view)), tryPulseTransaction: true, token: token);
        }

        public void UpdateDocumentReferences(int id, string key, HashSet<string> references)
        {
            var documentReferencesByKey = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByKey);
            var documentReferencesByRef = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByRef);
            var documentReferencesByView = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByView);
            var documentReferencesByViewAndKey = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByViewAndKey);

            var idKey = CreateKey(id);
            
            RemoveDocumentReference(() => documentReferencesByViewAndKey.MultiRead(Snapshot, AppendToKey(idKey, key)), false, CancellationToken.None);

            var loweredKey = (Slice) CreateKey(key);

            foreach (var reference in references)
            {
                var newKey = generator.CreateSequentialUuid(UuidType.DocumentReferences);

                var value = new Structure<DocumentReferencesFields>(tableStorage.DocumentReferences.Schema)
                    .Set(DocumentReferencesFields.IndexId, id)
                    .Set(DocumentReferencesFields.Key, key)
                    .Set(DocumentReferencesFields.Reference, reference);

                var newKeyAsSlice = (Slice) newKey.ToString();

                tableStorage.DocumentReferences.AddStruct(writeBatch.Value, newKeyAsSlice, value);
                documentReferencesByKey.MultiAdd(writeBatch.Value, loweredKey, newKeyAsSlice);
                documentReferencesByRef.MultiAdd(writeBatch.Value, (Slice) CreateKey(reference), newKeyAsSlice);
                documentReferencesByView.MultiAdd(writeBatch.Value, (Slice) idKey, newKeyAsSlice);
                documentReferencesByViewAndKey.MultiAdd(writeBatch.Value, (Slice) AppendToKey(idKey, key), newKeyAsSlice);
            }
        }

        public IEnumerable<string> GetDocumentsReferencing(string reference)
        {
            var documentReferencesByRef = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByRef);

            using (var iterator = documentReferencesByRef.MultiRead(Snapshot, (Slice)CreateKey(reference)))
            {

                if (!iterator.Seek(Slice.BeforeAllKeys))
                    yield break;

                var result = new HashSet<string>();
                do
                {
                    ushort version;
                    var structReader = LoadStruct(tableStorage.DocumentReferences, iterator.CurrentKey, writeBatch.Value, out version);
                    if (structReader == null)
                        continue;
                    var item = structReader.ReadString(DocumentReferencesFields.Key);
                    if (result.Add(item))
                        yield return item;
                }
                while (iterator.MoveNext());
            }
        }

        public int GetCountOfDocumentsReferencing(string reference)
        {
            var documentReferencesByRef = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByRef);

            using (var iterator = documentReferencesByRef.MultiRead(Snapshot, (Slice)CreateKey(reference)))
            {
                var count = 0;

                if (!iterator.Seek(Slice.BeforeAllKeys)) 
                    return count;

                do
                {
                    count++;
                }
                while (iterator.MoveNext());

                return count;
            }
        }

        public Dictionary<string,int> GetDocumentReferencesStats()
        {
            var documentReferencesByRef = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByRef);
            var results = new Dictionary<string, int>();
            using (var outerIterator = documentReferencesByRef.Iterate(Snapshot, null))
            {
                if (outerIterator.Seek(Slice.BeforeAllKeys) == false)
                    return results;
                do
                {
                    using (var iterator = documentReferencesByRef.MultiRead(Snapshot, outerIterator.CurrentKey))
                    {
                        var count = 0;

                        if (!iterator.Seek(Slice.BeforeAllKeys))
                            continue;

                        do
                        {
                            count++;
                        }
                        while (iterator.MoveNext());

                        results[outerIterator.CurrentKey.ToString()] = count;
                    }
                    
                } while (outerIterator.MoveNext());
            }

            return results;
        }

        public IEnumerable<string> GetDocumentsReferencesFrom(string key)
        {
            var documentReferencesByKey = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByKey);

            using (var iterator = documentReferencesByKey.MultiRead(Snapshot, (Slice)CreateKey(key)))
            {
                var result = new List<string>();

                if (!iterator.Seek(Slice.BeforeAllKeys))
                    return result;

                do
                {
                    ushort version;
                    var value = LoadStruct(tableStorage.DocumentReferences, iterator.CurrentKey, writeBatch.Value, out version);
                    if (value == null)
                        continue;
                    result.Add(value.ReadString(DocumentReferencesFields.Reference));
                }
                while (iterator.MoveNext());

                return result.Distinct(StringComparer.OrdinalIgnoreCase);
            }
        }

        public void DumpAllReferancesToCSV(StreamWriter writer, int numberOfSampleDocs)
        {
            var documentReferencesByKey = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByKey);            
            using (var docRefIterator = documentReferencesByKey.Iterate(Snapshot, writeBatch.Value))
            {
                if (!docRefIterator.Seek(Slice.BeforeAllKeys))
                {
                    // ref table empty
                    return;
                }
                var keysToRef = new Dictionary<string, DocCountWithSampleDocIds>();
                do
                {
                    using (var iterator = documentReferencesByKey.MultiRead(Snapshot, (Slice)CreateKey(docRefIterator.CurrentKey)))
                    {
                        if (!iterator.Seek(Slice.BeforeAllKeys))
                            break;
                        do
                        {
                            ushort version;
                            var value = LoadStruct(tableStorage.DocumentReferences, iterator.CurrentKey, writeBatch.Value, out version);
                            if (value == null)
                                continue;
                            var currentKeyStr = docRefIterator.CurrentKey.ToString();
                            DocCountWithSampleDocIds docData;
                            if (keysToRef.TryGetValue(currentKeyStr, out docData) == false)
                            {
                                keysToRef[currentKeyStr] = docData = new DocCountWithSampleDocIds
                                {
                                    Count = 0,
                                    SampleDocsIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                                };
                            }
                            if (docData.Count < numberOfSampleDocs)
                                docData.SampleDocsIds.Add(value.ReadString(DocumentReferencesFields.Reference));
                            docData.Count++;
                        } while (iterator.MoveNext());
                    }
                } while (docRefIterator.MoveNext());
                
                foreach (var kvp in keysToRef.OrderByDescending(x=>x.Value.Count))
                {
                    writer.WriteLine("{0},{1},\"{2}\"", kvp.Value.Count, kvp.Key, string.Join(", ", kvp.Value.SampleDocsIds));
                }
            }
        }

        private StructureReader<T> LoadStruct<T>(TableOfStructures<T> table, Slice name, out ushort version) where T : struct
        {
            var reader = LoadStruct(table, name, writeBatch.Value, out version);
            if(reader == null)
                throw new IndexDoesNotExistsException(string.Format("There is no index with the name: '{0}'", name.ToString()));

            return reader;
        }

        private static IndexStats GetIndexStats(StructureReader<IndexingWorkStatsFields> indexingStats, StructureReader<ReducingWorkStatsFields> reduceStats, StructureReader<LastIndexedStatsFields> lastIndexedEtags, int priority, int touches)
        {
            var lastReducedEtag = Etag.Parse(reduceStats.ReadBytes(ReducingWorkStatsFields.LastReducedEtag));
            var reduceAttempts = reduceStats.ReadInt(ReducingWorkStatsFields.ReduceAttempts);
            var reduceErrors = reduceStats.ReadInt(ReducingWorkStatsFields.ReduceErrors);
            var reduceSuccesses = reduceStats.ReadInt(ReducingWorkStatsFields.ReduceSuccesses);
            var lastReducedTimestamp = reduceStats.ReadLong(ReducingWorkStatsFields.LastReducedTimestamp);

            return new IndexStats
            {
                TouchCount = touches,
                IndexingAttempts = indexingStats.ReadInt(IndexingWorkStatsFields.IndexingAttempts),
                IndexingErrors = indexingStats.ReadInt(IndexingWorkStatsFields.IndexingErrors),
                IndexingSuccesses = indexingStats.ReadInt(IndexingWorkStatsFields.IndexingSuccesses),
                ReduceIndexingAttempts = reduceAttempts == -1 ? (int?)null : reduceAttempts,
                ReduceIndexingErrors = reduceErrors == -1 ? (int?)null : reduceErrors,
                ReduceIndexingSuccesses = reduceSuccesses == -1 ? (int?)null : reduceSuccesses,
                Id = indexingStats.ReadInt(IndexingWorkStatsFields.IndexId),
                Priority = (IndexingPriority)priority,
                LastIndexedEtag = Etag.Parse(lastIndexedEtags.ReadBytes(LastIndexedStatsFields.LastEtag)),
                LastIndexedTimestamp = DateTime.FromBinary(lastIndexedEtags.ReadLong(LastIndexedStatsFields.LastTimestamp)),
                CreatedTimestamp = DateTime.FromBinary(indexingStats.ReadLong(IndexingWorkStatsFields.CreatedTimestamp)),
                LastIndexingTime = DateTime.FromBinary(indexingStats.ReadLong(IndexingWorkStatsFields.LastIndexingTime)),
                LastReducedEtag =
                    lastReducedEtag.CompareTo(Etag.InvalidEtag) != 0
                        ? lastReducedEtag
                        : null,
                LastReducedTimestamp = lastReducedTimestamp == -1 ? (DateTime?)null : DateTime.FromBinary(lastReducedTimestamp)
            };
        }

        private void RemoveDocumentReferenceByKey(Slice key)
        {
            var keySlice = new Slice(CreateKey(key));

            var documentReferencesByKey = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByKey);

            RemoveDocumentReference(() => documentReferencesByKey.MultiRead(Snapshot, keySlice), false, CancellationToken.None);
        }

        private void RemoveDocumentReference(Func<IIterator> createIterator, bool tryPulseTransaction, CancellationToken token)
        {
            var iterator = createIterator();
            try
            {
                if (iterator.Seek(Slice.BeforeAllKeys) == false)
                    return;

                var documentReferencesByKey = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByKey);
                var documentReferencesByRef = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByRef);
                var documentReferencesByView = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByView);
                var documentReferencesByViewAndKey = tableStorage.DocumentReferences.GetIndex(Tables.DocumentReferences.Indices.ByViewAndKey);

                bool skipMoveNext;
                do
                {
                    skipMoveNext = false;
                    // TODO: Check if we can avoid the clone.
                    var id = iterator.CurrentKey.Clone();

                    ushort version;
                    var value = LoadStruct(tableStorage.DocumentReferences, id, writeBatch.Value, out version);
                    if (value == null)
                        continue;
                    var reference = value.ReadString(DocumentReferencesFields.Reference);
                    var view = value.ReadInt(DocumentReferencesFields.IndexId).ToString(CultureInfo.InvariantCulture);
                    var key = value.ReadString(DocumentReferencesFields.Key);

                    var viewKey = CreateKey(view);

                    tableStorage.DocumentReferences.Delete(writeBatch.Value, id);
                    documentReferencesByKey.MultiDelete(writeBatch.Value, (Slice)CreateKey(key), id);
                    documentReferencesByRef.MultiDelete(writeBatch.Value, (Slice)CreateKey(reference), id);
                    documentReferencesByView.MultiDelete(writeBatch.Value, (Slice)viewKey, id);
                    documentReferencesByViewAndKey.MultiDelete(writeBatch.Value, (Slice)AppendToKey(viewKey, key), id);

                    if (tryPulseTransaction)
                    {
                        if (generalStorageActions.MaybePulseTransaction(iterator))
                        {
                            iterator = createIterator();
                            if (iterator.Seek(Slice.BeforeAllKeys) == false)
                                break;
                            skipMoveNext = true;
                        }
                    }
                }
                while ((skipMoveNext || iterator.MoveNext()) && token.IsCancellationRequested == false);
            }
            finally
            {
                if (iterator != null)
                    iterator.Dispose();
            }
        }

        private int ReadPriority(string key)
        {
            var readResult = tableStorage.IndexingMetadata.Read(Snapshot, (Slice)AppendToKey(key, "priority"), writeBatch.Value);
            if (readResult == null)
                return -1;
            return readResult.Reader.ReadLittleEndianInt32();
        }

        private int ReadTouches(string key)
        {
            var readResult = tableStorage.IndexingMetadata.Read(Snapshot, (Slice)AppendToKey(key, "touches"), writeBatch.Value);
            if (readResult == null)
                return -1;
            return readResult.Reader.ReadLittleEndianInt32();
        }
    }
}

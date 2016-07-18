using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.MEF;
using Raven.Abstractions.Util.Encryptors;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Storage.Voron.StorageActions.StructureSchemas;
using Sparrow.Collections;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Sparrow;
using Voron.Trees;
using VoronIndex = Raven.Database.Storage.Voron.Impl.Index;

namespace Raven.Database.Storage.Voron.StorageActions
{
    using Database.Impl;
    using global::Voron;
    using global::Voron.Impl;
    using Impl;
    using Indexing;
    using Plugins;
    using Raven.Json.Linq;
    using Abstractions.Util;

    internal class MappedResultsStorageActions : StorageActionsBase, IMappedResultsStorageAction
    {
        private readonly TableStorage tableStorage;

        private readonly IUuidGenerator generator;

        private readonly Reference<WriteBatch> writeBatch;
        private readonly IStorageActionsAccessor storageActionsAccessor;

        private readonly OrderedPartCollection<AbstractDocumentCodec> documentCodecs;

        private readonly ConcurrentDictionary<int, RemainingReductionPerLevel> scheduledReductionsPerViewAndLevel;
        private readonly GeneralStorageActions generalStorageActions;

        public MappedResultsStorageActions(TableStorage tableStorage, IUuidGenerator generator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs, Reference<SnapshotReader> snapshot, Reference<WriteBatch> writeBatch, IBufferPool bufferPool, IStorageActionsAccessor storageActionsAccessor, ConcurrentDictionary<int, RemainingReductionPerLevel> ScheduledReductionsPerViewAndLevel, GeneralStorageActions generalStorageActions)
            : base(snapshot, bufferPool)
        {
            this.tableStorage = tableStorage;
            this.generator = generator;
            this.documentCodecs = documentCodecs;
            this.writeBatch = writeBatch;
            this.storageActionsAccessor = storageActionsAccessor;
            this.scheduledReductionsPerViewAndLevel = ScheduledReductionsPerViewAndLevel;
            this.generalStorageActions = generalStorageActions;
        }

        public IEnumerable<ReduceKeyAndCount> GetKeysStats(int view, int start, int pageSize)
        {
            var reduceKeyCountsByView = tableStorage.ReduceKeyCounts.GetIndex(Tables.ReduceKeyCounts.Indices.ByView);
            using (var iterator = reduceKeyCountsByView.MultiRead(Snapshot, CreateViewKey(view)))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
                    yield break;

                var count = 0;
                do
                {
                    ushort version;
                    var value = LoadStruct(tableStorage.ReduceKeyCounts, iterator.CurrentKey, writeBatch.Value, out version);

                    Debug.Assert(value != null);
                    yield return new ReduceKeyAndCount
                    {
                        Count = value.ReadInt(ReduceKeyCountFields.MappedItemsCount),
                        Key = value.ReadString(ReduceKeyCountFields.ReduceKey)
                    };

                    count++;
                }
                while (iterator.MoveNext() && count < pageSize);
            }
        }

        public void PutMappedResult(int view, string docId, string reduceKey, RavenJObject data)
        {
            var mappedResultsByViewAndDocumentId = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndDocumentId);
            var mappedResultsByView = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);
            var mappedResultsByViewAndReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);
            var mappedResultsByViewAndReduceKeyAndSourceBucket = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket);

            var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

            var ms = CreateStream();
            using (var stream = documentCodecs.Aggregate((Stream)new UndisposableStream(ms), (ds, codec) => codec.Value.Encode(reduceKey, data, null, ds)))
            {
                data.WriteTo(stream);
                stream.Flush();
            }

            var id = generator.CreateSequentialUuid(UuidType.MappedResults);
            var idSlice = (Slice)id.ToString();
            var bucket = IndexingUtil.MapBucket(docId);

            var mappedResult = new Structure<MappedResultFields>(tableStorage.MappedResults.Schema)
                .Set(MappedResultFields.IndexId, view)
                .Set(MappedResultFields.Bucket, bucket)
                .Set(MappedResultFields.Timestamp, SystemTime.UtcNow.ToBinary())
                .Set(MappedResultFields.ReduceKey, reduceKey)
                .Set(MappedResultFields.DocId, docId)
                .Set(MappedResultFields.Etag, id.ToByteArray());

            tableStorage.MappedResults.AddStruct(
                writeBatch.Value,
                idSlice,
                mappedResult, 0);

            ms.Position = 0;
            mappedResultsData.Add(writeBatch.Value, idSlice, ms, 0);

            var viewKey = CreateViewKey(view);
            var viewReduceKey = CreateMappedResultKey(view, reduceKey);

            mappedResultsByViewAndDocumentId.MultiAdd(writeBatch.Value, AppendToKey(CreateKey(view), docId), idSlice);
            mappedResultsByView.MultiAdd(writeBatch.Value, viewKey, idSlice);
            mappedResultsByViewAndReduceKey.MultiAdd(writeBatch.Value, viewReduceKey, idSlice);
            mappedResultsByViewAndReduceKeyAndSourceBucket.MultiAdd(writeBatch.Value, CreateMappedResultWithBucketKey(view, reduceKey, bucket), idSlice);
        }

        public void IncrementReduceKeyCounter(int view, string reduceKey, int val)
        {
            var viewKey = CreateViewKey(view);

            var keySlice = CreateMappedResultKey(view, reduceKey);

            ushort version;
            var value = LoadStruct(tableStorage.ReduceKeyCounts, keySlice, writeBatch.Value, out version);

            var newValue = val;
            if (value != null)
                newValue += value.ReadInt(ReduceKeyCountFields.MappedItemsCount);

            if (newValue == 0)
            {
                DeleteReduceKeyCount(keySlice, viewKey, version, shouldIgnoreConcurrencyExceptions: true);
                return;
            }

            AddReduceKeyCount(keySlice, view, viewKey, reduceKey, newValue, version, shouldIgnoreConcurrencyExceptions: true);
        }

        private void DecrementReduceKeyCounter(int view, string reduceKey, int val)
        {
            var viewKeySlice = CreateViewKey(view);
            var keySlice = CreateMappedResultKey(view, reduceKey);

            ushort reduceKeyCountVersion;
            var reduceKeyCount = LoadStruct(tableStorage.ReduceKeyCounts, keySlice, writeBatch.Value, out reduceKeyCountVersion);

            var newValue = -val;
            if (reduceKeyCount != null)
            {
                var currentValue = reduceKeyCount.ReadInt(ReduceKeyCountFields.MappedItemsCount);
                if (currentValue == val)
                {
                    var reduceKeyTypeVersion = tableStorage.ReduceKeyTypes.ReadVersion(Snapshot, keySlice, writeBatch.Value);

                    DeleteReduceKeyCount(keySlice, viewKeySlice, reduceKeyCountVersion);
                    DeleteReduceKeyType(keySlice, viewKeySlice, reduceKeyTypeVersion);
                    return;
                }

                newValue += currentValue;
            }

            AddReduceKeyCount(keySlice, view, viewKeySlice, reduceKey, newValue, reduceKeyCountVersion);
        }

        public bool HasMappedResultsForIndex(int view)
        {
            var mappedResultsByView = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);
            var viewKey = CreateViewKey(view);

            using (var iterator = mappedResultsByView.MultiRead(Snapshot, viewKey))
            {
                if (iterator.Seek(Slice.BeforeAllKeys) == false)
                    return false;

                return true;
            }
        }

        public void DeleteMappedResultsForDocumentId(string documentId, int view, Dictionary<ReduceKeyAndBucket, int> removed)
        {
            var viewKey = CreateViewKey(view);
            var viewAndDocumentId = new Slice(AppendToKey(CreateKey(view), documentId));

            var mappedResultsByViewAndDocumentId = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndDocumentId);
            var mappedResultsByView = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);
            var mappedResultsByViewAndReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);
            var mappedResultsByViewAndReduceKeyAndSourceBucket = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket);
            var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

            using (var iterator = mappedResultsByViewAndDocumentId.MultiRead(Snapshot, viewAndDocumentId))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                    return;

                do
                {
                    // TODO: Check if we can relax the clone.
                    var id = iterator.CurrentKey.Clone();

                    ushort version;
                    var value = LoadStruct(tableStorage.MappedResults, id, writeBatch.Value, out version);
                    if (value == null)
                        continue;
                    var reduceKey = value.ReadString(MappedResultFields.ReduceKey);
                    var bucket = value.ReadInt(MappedResultFields.Bucket);

                    var viewAndReduceKey = CreateMappedResultKey(view, reduceKey);
                    var viewAndReduceKeyAndSourceBucket = CreateMappedResultWithBucketKey(view, reduceKey, bucket);

                    tableStorage.MappedResults.Delete(writeBatch.Value, id);

                    mappedResultsByViewAndDocumentId.MultiDelete(writeBatch.Value, viewAndDocumentId, id);
                    mappedResultsByView.MultiDelete(writeBatch.Value, viewKey, id);
                    mappedResultsByViewAndReduceKey.MultiDelete(writeBatch.Value, viewAndReduceKey, id);
                    mappedResultsByViewAndReduceKeyAndSourceBucket.MultiDelete(writeBatch.Value, viewAndReduceKeyAndSourceBucket, id);
                    mappedResultsData.Delete(writeBatch.Value, id);

                    var reduceKeyAndBucket = new ReduceKeyAndBucket(bucket, reduceKey);
                    removed[reduceKeyAndBucket] = removed.GetOrDefault(reduceKeyAndBucket) + 1;
                }
                while (iterator.MoveNext());
            }
        }

        public void UpdateRemovedMapReduceStats(int view, Dictionary<ReduceKeyAndBucket, int> removed, CancellationToken token)
        {
            foreach (var keyAndBucket in removed)
            {
                token.ThrowIfCancellationRequested();
                IncrementReduceKeyCounter(view, keyAndBucket.Key.ReduceKey, -keyAndBucket.Value);
            }
        }

        public void DeleteMappedResultsForView(int view, CancellationToken token)
        {
            var deletedReduceKeys = new List<string>();

            var mappedResultsByViewAndDocumentId = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndDocumentId);
            var mappedResultsByView = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);
            var mappedResultsByViewAndReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);
            var mappedResultsByViewAndReduceKeyAndSourceBucket = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket);
            var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

            var viewKey = CreateViewKey(view);

            var iterator = mappedResultsByView.MultiRead(Snapshot, viewKey);
            try
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                    return;

                bool skipMoveNext;
                do
                {
                    skipMoveNext = false;
                    var id = iterator.CurrentKey.Clone();

                    ushort version;
                    var value = LoadStruct(tableStorage.MappedResults, id, writeBatch.Value, out version);
                    if (value == null)
                        continue;
                    var reduceKey = value.ReadString(MappedResultFields.ReduceKey);
                    var bucket = value.ReadInt(MappedResultFields.Bucket);
                    var documentId = value.ReadString(MappedResultFields.DocId);

                    var viewAndDocumentId = AppendToKey(CreateKey(view), documentId);
                    var viewAndReduceKey = CreateMappedResultKey(view, reduceKey);
                    var viewAndReduceKeyAndSourceBucket = CreateMappedResultWithBucketKey(view, reduceKey, bucket);

                    tableStorage.MappedResults.Delete(writeBatch.Value, id);

                    mappedResultsByViewAndDocumentId.MultiDelete(writeBatch.Value, viewAndDocumentId, id);
                    mappedResultsByView.MultiDelete(writeBatch.Value, viewKey, id);
                    mappedResultsByViewAndReduceKey.MultiDelete(writeBatch.Value, viewAndReduceKey, id);
                    mappedResultsByViewAndReduceKeyAndSourceBucket.MultiDelete(writeBatch.Value, viewAndReduceKeyAndSourceBucket, id);
                    mappedResultsData.Delete(writeBatch.Value, id);

                    deletedReduceKeys.Add(reduceKey);
                    if (generalStorageActions.MaybePulseTransaction(iterator))
                    {
                        iterator = mappedResultsByView.MultiRead(Snapshot, viewKey);
                        if (!iterator.Seek(Slice.BeforeAllKeys))
                            break;
                        skipMoveNext = true;
                    }
                } while ((skipMoveNext || iterator.MoveNext()) && token.IsCancellationRequested == false);
            }
            finally
            {
                if (iterator != null)
                    iterator.Dispose();
            }

            foreach (var g in deletedReduceKeys.GroupBy(x => x, StringComparer.InvariantCultureIgnoreCase))
            {
                DecrementReduceKeyCounter(view, g.Key, g.Count());
            }
        }

        public IEnumerable<string> GetKeysForIndexForDebug(int view, string startsWith, string sourceId, int start, int take)
        {
            var mappedResultsByView = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);
            using (var iterator = mappedResultsByView.MultiRead(Snapshot, CreateViewKey(view)))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                    return Enumerable.Empty<string>();

                var needExactMatch = take == 1;
                var results = new List<string>();
                do
                {
                    ushort version;
                    var value = LoadStruct(tableStorage.MappedResults, iterator.CurrentKey, writeBatch.Value, out version);
                    if (value == null)
                        continue;
                    if (string.IsNullOrEmpty(sourceId) == false)
                    {
                        var docId = value.ReadString(MappedResultFields.DocId);
                        if (string.Equals(sourceId, docId, StringComparison.OrdinalIgnoreCase) == false)
                            continue;
                    }

                    var reduceKey = value.ReadString(MappedResultFields.ReduceKey);

                    if (StringHelper.Compare(startsWith, reduceKey, needExactMatch) == false)
                        continue;

                    results.Add(reduceKey);
                }
                while (iterator.MoveNext());

                return results
                    .Distinct()
                    .Skip(start)
                    .Take(take);
            }
        }

        public IEnumerable<MappedResultInfo> GetMappedResultsForDebug(int view, string reduceKey, int start, int take)
        {
            var viewAndReduceKey = CreateMappedResultKey(view, reduceKey);
            var mappedResultsByViewAndReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);
            var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

            using (var iterator = mappedResultsByViewAndReduceKey.MultiRead(Snapshot, viewAndReduceKey))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
                    yield break;

                var count = 0;
                do
                {
                    ushort version;
                    var value = LoadStruct(tableStorage.MappedResults, iterator.CurrentKey, writeBatch.Value, out version);
                    if (value == null)
                        continue;
                    var size = tableStorage.MappedResults.GetDataSize(Snapshot, iterator.CurrentKey);
                    yield return new MappedResultInfo
                    {
                        ReduceKey = value.ReadString(MappedResultFields.ReduceKey),
                        Etag = Etag.Parse(value.ReadBytes(MappedResultFields.Etag)),
                        Timestamp = DateTime.FromBinary(value.ReadLong(MappedResultFields.Timestamp)),
                        Bucket = value.ReadInt(MappedResultFields.Bucket),
                        Source = value.ReadString(MappedResultFields.DocId),
                        Size = size,
                        Data = LoadMappedResult(iterator.CurrentKey, value.ReadString(MappedResultFields.ReduceKey), mappedResultsData)
                    };

                    count++;
                }
                while (iterator.MoveNext() && count < take);
            }
        }

        public IEnumerable<string> GetSourcesForIndexForDebug(int view, string startsWith, int take)
        {
            var mappedResultsByView = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByView);
            using (var iterator = mappedResultsByView.MultiRead(Snapshot, (Slice)CreateKey(view)))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                    return Enumerable.Empty<string>();

                var needExactMatch = take == 1;
                var results = new HashSet<string>();
                do
                {
                    ushort version;
                    var value = LoadStruct(tableStorage.MappedResults, iterator.CurrentKey, writeBatch.Value, out version);
                    if (value == null)
                        continue;
                    var docId = value.ReadString(MappedResultFields.DocId);

                    if (StringHelper.Compare(startsWith, docId, needExactMatch) == false)
                        continue;

                    results.Add(docId);
                }
                while (iterator.MoveNext() && results.Count <= take);

                return results;
            }
        }

        public IEnumerable<MappedResultInfo> GetReducedResultsForDebug(int view, string reduceKey, int level, int start, int take)
        {
            var viewAndReduceKeyAndLevel = CreateReduceResultsKey(view, reduceKey, level);
            var reduceResultsByViewAndReduceKeyAndLevel =
                tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel);
            var reduceResultsData = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.Data);

            using (var iterator = reduceResultsByViewAndReduceKeyAndLevel.MultiRead(Snapshot, viewAndReduceKeyAndLevel))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
                    yield break;

                var count = 0;
                do
                {
                    ushort version;
                    var value = LoadStruct(tableStorage.ReduceResults, iterator.CurrentKey, writeBatch.Value, out version);
                    if (value == null)
                        continue;
                    var size = tableStorage.ReduceResults.GetDataSize(Snapshot, iterator.CurrentKey);

                    var readReduceKey = value.ReadString(ReduceResultFields.ReduceKey);

                    yield return
                        new MappedResultInfo
                        {
                            ReduceKey = readReduceKey,
                            Etag = Etag.Parse(value.ReadBytes(ReduceResultFields.Etag)),
                            Timestamp = DateTime.FromBinary(value.ReadLong(ReduceResultFields.Timestamp)),
                            Bucket = value.ReadInt(ReduceResultFields.Bucket),
                            Source = value.ReadInt(ReduceResultFields.SourceBucket).ToString(),
                            Size = size,
                            Data = LoadMappedResult(iterator.CurrentKey, readReduceKey, reduceResultsData)
                        };

                    count++;
                }
                while (iterator.MoveNext() && count < take);
            }
        }

        public IEnumerable<ScheduledReductionDebugInfo> GetScheduledReductionForDebug(int view, int start, int take)
        {
            var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
            using (var iterator = scheduledReductionsByView.MultiRead(Snapshot, CreateViewKey(view)))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
                    yield break;

                var count = 0;
                do
                {
                    ushort version;
                    var value = LoadStruct(tableStorage.ScheduledReductions, iterator.CurrentKey, writeBatch.Value, out version);
                    if (value == null)
                        continue;

                    yield return new ScheduledReductionDebugInfo
                    {
                        Key = value.ReadString(ScheduledReductionFields.ReduceKey),
                        Bucket = value.ReadInt(ScheduledReductionFields.Bucket),
                        Etag = new Guid(value.ReadBytes(ScheduledReductionFields.Etag)),
                        Level = value.ReadInt(ScheduledReductionFields.Level),
                        Timestamp = DateTime.FromBinary(value.ReadLong(ScheduledReductionFields.Timestamp)),
                    };

                    count++;
                }
                while (iterator.MoveNext() && count < take);
            }
        }

        public void ScheduleReductions(int view, int level, ReduceKeyAndBucket reduceKeysAndBuckets)
        {
            var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
            var scheduledReductionsByViewAndLevelAndReduceKey = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);

            var id = generator.CreateSequentialUuid(UuidType.ScheduledReductions);
            var idSlice = (Slice)id.ToString();
            var scheduledReduction = new Structure<ScheduledReductionFields>(tableStorage.ScheduledReductions.Schema);

            scheduledReduction.Set(ScheduledReductionFields.IndexId, view)
                .Set(ScheduledReductionFields.ReduceKey, reduceKeysAndBuckets.ReduceKey)
                .Set(ScheduledReductionFields.Bucket, reduceKeysAndBuckets.Bucket)
                .Set(ScheduledReductionFields.Level, level)
                .Set(ScheduledReductionFields.Etag, id.ToByteArray())
                .Set(ScheduledReductionFields.Timestamp, SystemTime.UtcNow.ToBinary());

            tableStorage.ScheduledReductions.AddStruct(writeBatch.Value, idSlice, scheduledReduction);

            var viewKey = CreateViewKey(view);

            scheduledReductionsByView.MultiAdd(writeBatch.Value, viewKey, idSlice);
            var scheduleReductionKey = CreateScheduleReductionKey(view, level, reduceKeysAndBuckets.ReduceKey);
            scheduledReductionsByViewAndLevelAndReduceKey.MultiAdd(writeBatch.Value, scheduleReductionKey, CreateBucketAndEtagKey(reduceKeysAndBuckets.Bucket, id));
            if (scheduledReductionsPerViewAndLevel != null)
                scheduledReductionsPerViewAndLevel.AddOrUpdate(view, new RemainingReductionPerLevel(level), (key, oldvalue) => oldvalue.IncrementPerLevelCounters(level));
        }

        private Slice CreateBucketAndEtagKey(int bucket, Etag id)
        {
            var sliceWriter = new SliceWriter(20);
            sliceWriter.WriteBigEndian(bucket);
            sliceWriter.Write(id.ToByteArray());
            return sliceWriter.CreateSlice();
        }

        public IList<MappedResultInfo> GetItemsToReduce(GetItemsToReduceParams getItemsToReduceParams, CancellationToken cancellationToken)
        {
            var scheduledReductionsByViewAndLevelAndReduceKey = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);
            var deleter = new ScheduledReductionDeleter(getItemsToReduceParams.ItemsToDelete, o =>
            {
                var etag = o as Etag;
                if (etag == null)
                    return null;

                return (Slice)etag.ToString();
            });

            var keysToRemove = new List<string>();

            try
            {
                var seenLocally = new HashSet<ReduceKeyAndBucket>(ReduceKeyAndBucketEqualityComparer.Instance);
                var mappedResults = new List<MappedResultInfo>();

                var first = true;
                foreach (var reduceKey in getItemsToReduceParams.ReduceKeys)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    Slice start = Slice.BeforeAllKeys;
                    bool needToMoveNext = false;
                    if (first)
                    {
                        first = false;
                        if (getItemsToReduceParams.LastReduceKeyAndBucket != null)
                        {
                            if (getItemsToReduceParams.LastReduceKeyAndBucket.ReduceKey == reduceKey)
                            {
                                needToMoveNext = true;
                                start = CreateBucketAndEtagKey(getItemsToReduceParams.LastReduceKeyAndBucket.Bucket, Etag.Empty);
                            }
                        }
                    }
                    var viewAndLevelAndReduceKey = CreateScheduleReductionKey(getItemsToReduceParams.Index, getItemsToReduceParams.Level, reduceKey);
                    using (var iterator = scheduledReductionsByViewAndLevelAndReduceKey.MultiRead(Snapshot, viewAndLevelAndReduceKey))
                    {
                        if (iterator.Seek(start) == false ||
                            (needToMoveNext && iterator.MoveNext() == false))
                        {
                            keysToRemove.Add(reduceKey);
                            continue;
                        }

                        do
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            if (getItemsToReduceParams.Take <= 0)
                                return mappedResults;

                            var idValueReader = iterator.CurrentKey.CreateReader();
                            idValueReader.ReadBigEndianInt32(); // bucket
                            int _;
                            var id = new Slice(Etag.Parse(idValueReader.ReadBytes(16, out _)));


                            ushort version;
                            var value = LoadStruct(tableStorage.ScheduledReductions, id, writeBatch.Value, out version);
                            if (value == null) // TODO: Check if this is correct. 
                                continue;

                            var reduceKeyFromDb = value.ReadString(ScheduledReductionFields.ReduceKey);

                            var bucket = value.ReadInt(ScheduledReductionFields.Bucket);
                            var rowKey = new ReduceKeyAndBucket(bucket, reduceKeyFromDb);

                            var thisIsNewScheduledReductionRow = deleter.Delete(iterator.CurrentKey, Etag.Parse(value.ReadBytes(ScheduledReductionFields.Etag)));

                            if (thisIsNewScheduledReductionRow)
                            {
                                if (seenLocally.Add(rowKey))
                                {
                                    getItemsToReduceParams.LastReduceKeyAndBucket = rowKey;
                                    foreach (var mappedResultInfo in GetResultsForBucket(getItemsToReduceParams.Index, getItemsToReduceParams.Level, reduceKeyFromDb, bucket, getItemsToReduceParams.LoadData, cancellationToken))
                                    {
                                        getItemsToReduceParams.Take--;

                                        mappedResults.Add(mappedResultInfo);
                                    }
                                }
                            }
                        }
                        while (iterator.MoveNext());
                    }

                    keysToRemove.Add(reduceKey);

                    if (getItemsToReduceParams.Take <= 0)
                        break;
                }

                return mappedResults;
            }
            finally
            {
                foreach (var keyToRemove in keysToRemove)
                    getItemsToReduceParams.ReduceKeys.Remove(keyToRemove);
            }
        }

        private IEnumerable<MappedResultInfo> GetResultsForBucket(int view, int level, string reduceKey, int bucket, bool loadData, CancellationToken cancellationToken)
        {
            switch (level)
            {
                case 0:
                    return GetMappedResultsForBucket(view, reduceKey, bucket, loadData, cancellationToken);
                case 1:
                case 2:
                    return GetReducedResultsForBucket(view, reduceKey, level, bucket, loadData, cancellationToken);
                default:
                    throw new ArgumentException("Invalid level: " + level);
            }
        }

        private IEnumerable<MappedResultInfo> GetReducedResultsForBucket(int view, string reduceKey, int level, int bucket, bool loadData, CancellationToken cancellationToken)
        {
            var viewAndReduceKeyAndLevelAndBucket = CreateReduceResultsWithBucketKey(view, reduceKey, level, bucket);

            var reduceResultsByViewAndReduceKeyAndLevelAndBucket = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndBucket);
            var reduceResultsData = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.Data);
            using (var iterator = reduceResultsByViewAndReduceKeyAndLevelAndBucket.MultiRead(Snapshot, viewAndReduceKeyAndLevelAndBucket))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                {
                    yield return new MappedResultInfo
                    {
                        Bucket = bucket,
                        ReduceKey = reduceKey
                    };

                    yield break;
                }

                do
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    ushort version;
                    var value = LoadStruct(tableStorage.ReduceResults, iterator.CurrentKey, writeBatch.Value, out version);
                    if (value == null)
                        continue;
                    var size = tableStorage.ReduceResults.GetDataSize(Snapshot, iterator.CurrentKey);

                    var readReduceKey = value.ReadString(ReduceResultFields.ReduceKey);

                    yield return new MappedResultInfo
                    {
                        ReduceKey = readReduceKey,
                        Etag = Etag.Parse(value.ReadBytes(ReduceResultFields.Etag)),
                        Timestamp = DateTime.FromBinary(value.ReadLong(ReduceResultFields.Timestamp)),
                        Bucket = value.ReadInt(ReduceResultFields.Bucket),
                        Source = null,
                        Size = size,
                        Data = loadData ? LoadMappedResult(iterator.CurrentKey, readReduceKey, reduceResultsData) : null
                    };
                }
                while (iterator.MoveNext());
            }
        }

        private IEnumerable<MappedResultInfo> GetMappedResultsForBucket(int view, string reduceKey, int bucket, bool loadData, CancellationToken cancellationToken)
        {
            var viewAndReduceKeyAndSourceBucket = CreateMappedResultWithBucketKey(view, reduceKey, bucket);

            var mappedResultsByViewAndReduceKeyAndSourceBucket = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket);
            var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);

            using (var iterator = mappedResultsByViewAndReduceKeyAndSourceBucket.MultiRead(Snapshot, viewAndReduceKeyAndSourceBucket))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                {
                    yield return new MappedResultInfo
                    {
                        Bucket = bucket,
                        ReduceKey = reduceKey
                    };

                    yield break;
                }

                do
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    ushort version;
                    var value = LoadStruct(tableStorage.MappedResults, iterator.CurrentKey, writeBatch.Value, out version);
                    if (value == null)
                        continue;
                    var size = tableStorage.MappedResults.GetDataSize(Snapshot, iterator.CurrentKey);

                    var readReduceKey = value.ReadString(MappedResultFields.ReduceKey);
                    yield return new MappedResultInfo
                    {
                        ReduceKey = readReduceKey,
                        Etag = Etag.Parse(value.ReadBytes(MappedResultFields.Etag)),
                        Timestamp = DateTime.FromBinary(value.ReadLong(MappedResultFields.Timestamp)),
                        Bucket = value.ReadInt(MappedResultFields.Bucket),
                        Source = null,
                        Size = size,
                        Data = loadData ? LoadMappedResult(iterator.CurrentKey, readReduceKey, mappedResultsData) : null
                    };
                }
                while (iterator.MoveNext());
            }
        }

        public ScheduledReductionInfo DeleteScheduledReduction(IEnumerable<object> itemsToDelete)
        {
            if (itemsToDelete == null)
                return null;

            var result = new ScheduledReductionInfo();
            var hasResult = false;
            var currentEtag = Etag.Empty;
            foreach (Etag etag in itemsToDelete.Where(x => x != null))
            {
                var etagAsString = (Slice)etag.ToString();

                ushort version;
                var value = LoadStruct(tableStorage.ScheduledReductions, etagAsString, writeBatch.Value, out version);
                if (value == null)
                    continue;

                if (etag.CompareTo(currentEtag) > 0)
                {
                    hasResult = true;
                    result.Etag = etag;
                    result.Timestamp = DateTime.FromBinary(value.ReadLong(ScheduledReductionFields.Timestamp));
                }

                var view = value.ReadInt(ScheduledReductionFields.IndexId);
                var level = value.ReadInt(ScheduledReductionFields.Level);
                var reduceKey = value.ReadString(ScheduledReductionFields.ReduceKey);
                var bucket = value.ReadInt(ScheduledReductionFields.Bucket);

                DeleteScheduledReduction(etag, etagAsString, CreateScheduleReductionKey(view, level, reduceKey), CreateViewKey(view), bucket);
                generalStorageActions.MaybePulseTransaction();

                if (scheduledReductionsPerViewAndLevel != null)
                    scheduledReductionsPerViewAndLevel.AddOrUpdate(view, new RemainingReductionPerLevel(level), (key, oldvalue) => oldvalue.DecrementPerLevelCounters(level));
            }

            return hasResult ? result : null;
        }

        private Slice CreateScheduleReductionKey(int view, int level, string reduceKey)
        {
            var sliceWriter = new SliceWriter(16);
            sliceWriter.WriteBigEndian(view);
            sliceWriter.WriteBigEndian(level);
            sliceWriter.WriteBigEndian(Hashing.XXHash64.CalculateRaw(reduceKey));

            return sliceWriter.CreateSlice();
        }

        private Slice CreateReduceResultsKey(int view, string reduceKey, int level)
        {
            var sliceWriter = new SliceWriter(16);
            sliceWriter.WriteBigEndian(view);
            sliceWriter.WriteBigEndian(Hashing.XXHash64.CalculateRaw(reduceKey));
            sliceWriter.WriteBigEndian(level);


            return sliceWriter.CreateSlice();
        }

        private Slice CreateReduceResultsWithBucketKey(int view, string reduceKey, int level, int bucket)
        {
            var sliceWriter = new SliceWriter(20);
            sliceWriter.WriteBigEndian(view);
            sliceWriter.WriteBigEndian(Hashing.XXHash64.CalculateRaw(reduceKey));
            sliceWriter.WriteBigEndian(level);
            sliceWriter.WriteBigEndian(bucket);

            return sliceWriter.CreateSlice();
        }


        private Slice CreateMappedResultKey(int view, string reduceKey)
        {
            var sliceWriter = new SliceWriter(12);
            sliceWriter.WriteBigEndian(view);
            sliceWriter.WriteBigEndian(Hashing.XXHash64.CalculateRaw(reduceKey));

            return sliceWriter.CreateSlice();
        }

        private Slice CreateMappedResultWithBucketKey(int view, string reduceKey, int bucket)
        {
            var sliceWriter = new SliceWriter(16);
            sliceWriter.WriteBigEndian(view);
            sliceWriter.WriteBigEndian(Hashing.XXHash64.CalculateRaw(reduceKey));
            sliceWriter.WriteBigEndian(bucket);

            return sliceWriter.CreateSlice();
        }

        public Dictionary<int, RemainingReductionPerLevel> GetRemainingScheduledReductionPerIndex()
        {
            var res = new Dictionary<int, RemainingReductionPerLevel>();
            if (scheduledReductionsPerViewAndLevel == null) return null;
            var iterator = scheduledReductionsPerViewAndLevel.GetEnumerator();
            do
            {
                res.Add(iterator.Current.Key, iterator.Current.Value);
            } while (iterator.MoveNext());
            return res;
        }

        public void PutReducedResult(int view, string reduceKey, int level, int sourceBucket, int bucket, RavenJObject data)
        {
            var reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket =
                tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket);
            var reduceResultsByViewAndReduceKeyAndLevelAndBucket =
                tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndBucket);
            var reduceResultsByViewAndReduceKeyAndLevel =
                tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel);
            var reduceResultsByView =
                tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByView);
            var reduceResultsData = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.Data);

            var ms = CreateStream();
            using (
                var stream = documentCodecs.Aggregate((Stream)new UndisposableStream(ms),
                    (ds, codec) => codec.Value.Encode(reduceKey, data, null, ds)))
            {
                data.WriteTo(stream);
                stream.Flush();
            }

            var id = generator.CreateSequentialUuid(UuidType.MappedResults);
            var idAsSlice = (Slice)id.ToString();

            var reduceResult = new Structure<ReduceResultFields>(tableStorage.ReduceResults.Schema)
                .Set(ReduceResultFields.IndexId, view)
                .Set(ReduceResultFields.Etag, id.ToByteArray())
                .Set(ReduceResultFields.ReduceKey, reduceKey)
                .Set(ReduceResultFields.Level, level)
                .Set(ReduceResultFields.SourceBucket, sourceBucket)
                .Set(ReduceResultFields.Bucket, bucket)
                .Set(ReduceResultFields.Timestamp, SystemTime.UtcNow.ToBinary());

            tableStorage.ReduceResults.AddStruct(writeBatch.Value, idAsSlice, reduceResult, 0);

            ms.Position = 0;
            reduceResultsData.Add(writeBatch.Value, idAsSlice, ms, 0);

            var viewKey = CreateViewKey(view);
            var viewAndReduceKeyAndLevel = CreateReduceResultsKey(view, reduceKey, level);
            var viewAndReduceKeyAndLevelAndSourceBucket = CreateReduceResultsWithBucketKey(view, reduceKey, level, sourceBucket);
            var viewAndReduceKeyAndLevelAndBucket = CreateReduceResultsWithBucketKey(view, reduceKey, level, bucket);

            reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket.MultiAdd(writeBatch.Value, viewAndReduceKeyAndLevelAndSourceBucket, idAsSlice);
            reduceResultsByViewAndReduceKeyAndLevel.MultiAdd(writeBatch.Value, viewAndReduceKeyAndLevel, idAsSlice);
            reduceResultsByViewAndReduceKeyAndLevelAndBucket.MultiAdd(writeBatch.Value, viewAndReduceKeyAndLevelAndBucket, idAsSlice);
            reduceResultsByView.MultiAdd(writeBatch.Value, viewKey, idAsSlice);
        }

        public void RemoveReduceResults(int view, int level, string reduceKey, int sourceBucket)
        {
            var viewAndReduceKeyAndLevelAndSourceBucket = CreateReduceResultsWithBucketKey(view, reduceKey, level, sourceBucket);
            var reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket);

            RemoveReduceResults(() => reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket.MultiRead(Snapshot, viewAndReduceKeyAndLevelAndSourceBucket), false, CancellationToken.None);
        }

        public IEnumerable<ReduceTypePerKey> GetReduceTypesPerKeys(int view, int take, int limitOfItemsToReduceInSingleStep, CancellationToken cancellationToken)
        {
            if (take <= 0)
                take = 1;

            var allKeysToReduce = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            var key = CreateViewKey(view);
            var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
            using (var iterator = scheduledReductionsByView.MultiRead(Snapshot, key))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                    yield break;

                var processedItems = 0;

                do
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    ushort version;
                    var value = LoadStruct(tableStorage.ScheduledReductions, iterator.CurrentKey, writeBatch.Value, out version);
                    if (value == null) // TODO: Check if this is correct. 
                        continue;

                    allKeysToReduce.Add(value.ReadString(ScheduledReductionFields.ReduceKey));
                    processedItems++;
                }
                while (iterator.MoveNext() && processedItems < take);
            }

            foreach (var reduceKey in allKeysToReduce)
            {
                var reduceKeySlice = CreateMappedResultKey(view, reduceKey);
                var count = GetNumberOfMappedItemsPerReduceKey(reduceKeySlice);
                var reduceType = count >= limitOfItemsToReduceInSingleStep ? ReduceType.MultiStep : ReduceType.SingleStep;
                yield return new ReduceTypePerKey(reduceKey, reduceType);
            }
        }

        private int GetNumberOfMappedItemsPerReduceKey(Slice key)
        {
            ushort version;
            var value = LoadStruct(tableStorage.ReduceKeyCounts, key, writeBatch.Value, out version);
            return value == null ? 0 : value.ReadInt(ReduceKeyCountFields.MappedItemsCount);
        }

        public void UpdatePerformedReduceType(int view, string reduceKey, ReduceType reduceType, bool skipAdd = false)
        {
            var reduceKeySlice = CreateMappedResultKey(view, reduceKey);
            var reduceKeyTypeVersion = tableStorage.ReduceKeyTypes.ReadVersion(Snapshot, reduceKeySlice, writeBatch.Value);
            if (GetNumberOfMappedItemsPerReduceKey(reduceKeySlice) == 0)
            {
                // the reduce key doesn't exist anymore,
                // we can delete the reduce key type for this reduce key
                DeleteReduceKeyType(reduceKeySlice, CreateViewKey(view), reduceKeyTypeVersion);
                return;
            }

            if (skipAdd)
                return;

            AddReduceKeyType(reduceKeySlice, view, reduceKey, reduceType, reduceKeyTypeVersion);
        }

        private void DeleteReduceKeyCount(Slice key, Slice viewKey,
            ushort? expectedVersion, bool shouldIgnoreConcurrencyExceptions = false)
        {
            var reduceKeyCountsByView = tableStorage.ReduceKeyCounts.GetIndex(Tables.ReduceKeyCounts.Indices.ByView);

            tableStorage.ReduceKeyCounts.Delete(writeBatch.Value, key, expectedVersion, shouldIgnoreConcurrencyExceptions);
            reduceKeyCountsByView.MultiDelete(writeBatch.Value, viewKey, key);
        }

        private void DeleteReduceKeyType(Slice key, Slice viewKey, ushort? expectedVersion)
        {
            var reduceKeyTypesByView = tableStorage.ReduceKeyTypes.GetIndex(Tables.ReduceKeyTypes.Indices.ByView);

            tableStorage.ReduceKeyTypes.Delete(writeBatch.Value, key, expectedVersion);
            reduceKeyTypesByView.MultiDelete(writeBatch.Value, viewKey, key);
        }

        private void AddReduceKeyCount(Slice key, int view, Slice viewKey, string reduceKey, int count,
            ushort? expectedVersion, bool shouldIgnoreConcurrencyExceptions = false)
        {
            var reduceKeyCountsByView = tableStorage.ReduceKeyCounts.GetIndex(Tables.ReduceKeyCounts.Indices.ByView);

            tableStorage.ReduceKeyCounts.AddStruct(
                writeBatch.Value,
                key,
                new Structure<ReduceKeyCountFields>(tableStorage.ReduceKeyCounts.Schema)
                    .Set(ReduceKeyCountFields.IndexId, view)
                    .Set(ReduceKeyCountFields.MappedItemsCount, count)
                    .Set(ReduceKeyCountFields.ReduceKey, reduceKey),
                expectedVersion, shouldIgnoreConcurrencyExceptions);

            reduceKeyCountsByView.MultiAdd(writeBatch.Value, viewKey, key);
        }

        private void AddReduceKeyType(Slice key, int view, string reduceKey, ReduceType status, ushort? expectedVersion)
        {
            var reduceKeyTypesByView = tableStorage.ReduceKeyTypes.GetIndex(Tables.ReduceKeyTypes.Indices.ByView);

            tableStorage.ReduceKeyTypes.AddStruct(
                writeBatch.Value,
                key,
                new Structure<ReduceKeyTypeFields>(tableStorage.ReduceKeyTypes.Schema)
                    .Set(ReduceKeyTypeFields.IndexId, view)
                    .Set(ReduceKeyTypeFields.ReduceType, (int)status)
                    .Set(ReduceKeyTypeFields.ReduceKey, reduceKey),
                expectedVersion);

            reduceKeyTypesByView.MultiAdd(writeBatch.Value, CreateViewKey(view), key);
        }

        public ReduceType GetLastPerformedReduceType(int view, string reduceKey)
        {
            var key = CreateMappedResultKey(view, reduceKey);

            ushort version;
            var value = LoadStruct(tableStorage.ReduceKeyTypes, key, writeBatch.Value, out version);
            if (value == null)
                return ReduceType.None;

            return (ReduceType)value.ReadInt(ReduceKeyTypeFields.ReduceType);
        }

        public IEnumerable<int> GetMappedBuckets(int view, string reduceKey, CancellationToken cancellationToken)
        {
            var viewAndReduceKey = CreateMappedResultKey(view, reduceKey);
            var mappedResultsByViewAndReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);

            using (var iterator = mappedResultsByViewAndReduceKey.MultiRead(Snapshot, viewAndReduceKey))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                    yield break;

                do
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    ushort version;
                    var value = LoadStruct(tableStorage.MappedResults, iterator.CurrentKey, writeBatch.Value, out version);
                    if (value == null)
                        continue;
                    yield return value.ReadInt(MappedResultFields.Bucket);
                }
                while (iterator.MoveNext());
            }
        }

        public List<MappedResultInfo> GetMappedResults(int view, HashSet<string> keysLeftToReduce, bool loadData, int take, HashSet<string> keysReturned, CancellationToken cancellationToken, List<MappedResultInfo> outputCollection = null)
        {
            if (outputCollection == null)
                outputCollection = new List<MappedResultInfo>();

            var mappedResultsByViewAndReduceKey = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.ByViewAndReduceKey);
            var mappedResultsData = tableStorage.MappedResults.GetIndex(Tables.MappedResults.Indices.Data);
            
            var keysToReduce = new HashSet<string>(keysLeftToReduce);
            foreach (var reduceKey in keysToReduce)
            {
                cancellationToken.ThrowIfCancellationRequested();

                keysLeftToReduce.Remove(reduceKey);

                var viewAndReduceKey = CreateMappedResultKey(view, reduceKey);
                using (var iterator = mappedResultsByViewAndReduceKey.MultiRead(Snapshot, viewAndReduceKey))
                {
                    keysReturned.Add(reduceKey);

                    if (!iterator.Seek(Slice.BeforeAllKeys))
                        continue;

                    do
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        ushort version;
                        var value = LoadStruct(tableStorage.MappedResults, iterator.CurrentKey, writeBatch.Value, out version);
                        if (value == null)
                            continue;
                        var size = tableStorage.MappedResults.GetDataSize(Snapshot, iterator.CurrentKey);

                        var readReduceKey = value.ReadString(MappedResultFields.ReduceKey);

                        take--; // We have worked with this reduce key, so we consider it an output even if we don't add it. 

                        RavenJObject data = null;
                        if (loadData)
                        {
                            data = LoadMappedResult(iterator.CurrentKey, readReduceKey, mappedResultsData);
                            if (data == null)
                                continue; // If we request to load data and it is not there, we ignore it
                        }

                        var mappedResult = new MappedResultInfo
                        {
                            Bucket = value.ReadInt(MappedResultFields.Bucket),
                            ReduceKey = readReduceKey,
                            Etag = Etag.Parse(value.ReadBytes(MappedResultFields.Etag)),
                            Timestamp = DateTime.FromBinary(value.ReadLong(MappedResultFields.Timestamp)),
                            Data = data,
                            Size = size
                        };

                        outputCollection.Add(mappedResult);
                    }
                    while (iterator.MoveNext());
                }

                if (take < 0)
                    return outputCollection;
            }

            return outputCollection;
        }

        private RavenJObject LoadMappedResult(Slice key, string reduceKey, VoronIndex dataIndex)
        {
            var read = dataIndex.Read(Snapshot, key, writeBatch.Value);
            if (read == null)
                return null;

            using (var readerStream = read.Reader.AsStream())
            {
                using (var stream = documentCodecs.Aggregate(readerStream, (ds, codec) => codec.Decode(reduceKey, null, ds)))
                    return stream.ToJObject();
            }
        }

        public IEnumerable<ReduceTypePerKey> GetReduceKeysAndTypes(int view, int start, int take)
        {
            var reduceKeyTypesByView = tableStorage.ReduceKeyTypes.GetIndex(Tables.ReduceKeyTypes.Indices.ByView);
            using (var iterator = reduceKeyTypesByView.MultiRead(Snapshot, CreateViewKey(view)))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys) || !iterator.Skip(start))
                    yield break;

                var count = 0;
                do
                {
                    ushort version;
                    var value = LoadStruct(tableStorage.ReduceKeyTypes, iterator.CurrentKey, writeBatch.Value, out version);

                    yield return new ReduceTypePerKey(value.ReadString(ReduceKeyTypeFields.ReduceKey), (ReduceType)value.ReadInt(ReduceKeyTypeFields.ReduceType));

                    count++;
                }
                while (iterator.MoveNext() && count < take);
            }
        }

        public Dictionary<int, long> DeleteObsoleteScheduledReductions(List<int> mapReduceIndexIds, long delete)
        {
            var results = new Dictionary<int, long>();
            var reductionsToDelete = GetScheduledReductionsToDelete(mapReduceIndexIds, delete);
            foreach (var data in reductionsToDelete)
            {
                var scheduleReductionKey = CreateScheduleReductionKey(data.View, data.Level, data.ReduceKey);
                var viewKey = CreateViewKey(data.View);
                DeleteScheduledReduction(data.Etag, data.EtagAsSlice, scheduleReductionKey, viewKey, data.Bucket);
                generalStorageActions.MaybePulseTransaction();

                long currentCount = 0;
                results.TryGetValue(data.View, out currentCount);
                results[data.View] = currentCount + 1;
            }

            return results;
        }

        private List<ScheduleReductionData> GetScheduledReductionsToDelete(List<int> mapReduceIndexIds, long delete)
        {
            var results = new List<ScheduleReductionData>();
            var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
            using (var outerIterator = scheduledReductionsByView.Iterate(Snapshot, writeBatch.Value))
            {
                if (outerIterator.Seek(Slice.BeforeAllKeys) == false)
                    return results;

                do
                {
                    var key = outerIterator.CurrentKey;
                    using (var iterator = scheduledReductionsByView.MultiRead(Snapshot, key))
                    {
                        if (iterator.Seek(Slice.BeforeAllKeys) == false)
                            return results;

                        long count = 0;
                        do
                        {
                            var id = iterator.CurrentKey.Clone();
                            ushort version;
                            var value = LoadStruct(tableStorage.ScheduledReductions, id, writeBatch.Value, out version);
                            if (value == null)
                                continue;

                            var view = value.ReadInt(ScheduledReductionFields.IndexId);
                            if (mapReduceIndexIds.Exists(x => x == view))
                                continue; // index id exists, no need to delete the scheduled reduction

                            var etag = Etag.Parse(value.ReadBytes(ScheduledReductionFields.Etag));
                            var level = value.ReadInt(ScheduledReductionFields.Level);
                            var reduceKey = value.ReadString(ScheduledReductionFields.ReduceKey);
                            var bucket = value.ReadInt(ScheduledReductionFields.Bucket);

                            results.Add(new ScheduleReductionData
                            {
                                Etag = etag,
                                EtagAsSlice = id,
                                View = view,
                                Level = level,
                                ReduceKey = reduceKey,
                                Bucket = bucket
                            });

                            count++;
                        }
                        while (iterator.MoveNext() && count < delete);
                    }
                } while (outerIterator.MoveNext());
            }

            return results;
        }

        public void DeleteScheduledReductionForView(int view, CancellationToken token)
        {
            var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);

            var viewKey = CreateViewKey(view);
            var iterator = scheduledReductionsByView.MultiRead(Snapshot, viewKey);
            try
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                    return;

                bool skipMoveNext;
                do
                {
                    skipMoveNext = false;
                    var id = iterator.CurrentKey.Clone();

                    ushort version;
                    var value = LoadStruct(tableStorage.ScheduledReductions, id, writeBatch.Value, out version);
                    if (value == null)
                        continue;

                    var idAsEtag = Etag.Parse(value.ReadBytes(ScheduledReductionFields.Etag));
                    var level = value.ReadInt(ScheduledReductionFields.Level);
                    var reduceKey = value.ReadString(ScheduledReductionFields.ReduceKey);
                    var bucket = value.ReadInt(ScheduledReductionFields.Bucket);
                    DeleteScheduledReduction(idAsEtag, id, CreateScheduleReductionKey(view, level, reduceKey), viewKey, bucket);
                    if (generalStorageActions.MaybePulseTransaction(iterator))
                    {
                        iterator = scheduledReductionsByView.MultiRead(Snapshot, viewKey);
                        if (!iterator.Seek(Slice.BeforeAllKeys))
                            break;
                        skipMoveNext = true;
                    }
                } while ((skipMoveNext || iterator.MoveNext()) && token.IsCancellationRequested == false);
            }
            finally
            {
                if (iterator != null)
                    iterator.Dispose();
            }
        }

        public void RemoveReduceResultsForView(int view, CancellationToken token)
        {
            var reduceResultsByView = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByView);

            RemoveReduceResults(() => reduceResultsByView.MultiRead(Snapshot, CreateViewKey(view)), true, token);
        }

        private void DeleteScheduledReduction(Etag etag, Slice etagAsString, Slice scheduleReductionKey, Slice viewKey, int bucket)
        {
            var scheduledReductionsByView = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByView);
            var scheduledReductionsByViewAndLevelAndReduceKey = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);

            tableStorage.ScheduledReductions.Delete(writeBatch.Value, etagAsString);

            scheduledReductionsByView.MultiDelete(writeBatch.Value, viewKey, etagAsString);
            scheduledReductionsByViewAndLevelAndReduceKey.MultiDelete(writeBatch.Value, scheduleReductionKey, CreateBucketAndEtagKey(bucket, etag));
        }


        public void DeleteScheduledReduction(int view, int level, string reduceKey)
        {
            var scheduledReductionsByViewAndLevelAndReduceKey = tableStorage.ScheduledReductions.GetIndex(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey);
            var scheduleReductionKey = CreateScheduleReductionKey(view, level, reduceKey);
            using (var iterator = scheduledReductionsByViewAndLevelAndReduceKey.MultiRead(Snapshot, scheduleReductionKey))
            {
                if (!iterator.Seek(Slice.BeforeAllKeys))
                    return;

                var viewKey = CreateViewKey(view);
                do
                {
                    var idValueReader = iterator.CurrentKey.CreateReader();
                    var bucket = idValueReader.ReadBigEndianInt32();
                    int _;
                    var etag = Etag.Parse(idValueReader.ReadBytes(16, out _));
                    var id = new Slice(etag.ToString());

                    DeleteScheduledReduction(etag, id, scheduleReductionKey, viewKey, bucket);
                    if (scheduledReductionsPerViewAndLevel != null)
                        scheduledReductionsPerViewAndLevel.AddOrUpdate(view, new RemainingReductionPerLevel(level), (key, oldvalue) => oldvalue.DecrementPerLevelCounters(level));
                }
                while (iterator.MoveNext());
            }
        }

        private void RemoveReduceResults(Func<IIterator> createIterator, bool tryPulseTransaction, CancellationToken token)
        {
            var iterator = createIterator();

            try
            {
                if (iterator.Seek(Slice.BeforeAllKeys) == false)
                    return;

                var reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket);
                var reduceResultsByViewAndReduceKeyAndLevel = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel);
                var reduceResultsByViewAndReduceKeyAndLevelAndBucket = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndBucket);
                var reduceResultsByView = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.ByView);
                var reduceResultsData = tableStorage.ReduceResults.GetIndex(Tables.ReduceResults.Indices.Data);

                bool skipMoveNext;
                do
                {
                    skipMoveNext = false;

                    // TODO: Check if we can avoid the clone.
                    Slice id = iterator.CurrentKey.Clone();

                    ushort version;
                    var value = LoadStruct(tableStorage.ReduceResults, id, writeBatch.Value, out version);
                    if (value == null)
                        continue;
                    var view = value.ReadInt(ReduceResultFields.IndexId);
                    var reduceKey = value.ReadString(ReduceResultFields.ReduceKey);
                    var level = value.ReadInt(ReduceResultFields.Level);
                    var bucket = value.ReadInt(ReduceResultFields.Bucket);
                    var sourceBucket = value.ReadInt(ReduceResultFields.SourceBucket);

                    var viewKey = CreateViewKey(view);
                    var viewAndReduceKeyAndLevel = CreateReduceResultsKey(view, reduceKey, level);
                    var viewAndReduceKeyAndLevelAndSourceBucket = CreateReduceResultsWithBucketKey(view, reduceKey, level, sourceBucket);
                    var viewAndReduceKeyAndLevelAndBucket = CreateReduceResultsWithBucketKey(view, reduceKey, level, bucket);

                    tableStorage.ReduceResults.Delete(writeBatch.Value, id);

                    reduceResultsByViewAndReduceKeyAndLevelAndSourceBucket.MultiDelete(writeBatch.Value, viewAndReduceKeyAndLevelAndSourceBucket, id);
                    reduceResultsByViewAndReduceKeyAndLevel.MultiDelete(writeBatch.Value, viewAndReduceKeyAndLevel, id);
                    reduceResultsByViewAndReduceKeyAndLevelAndBucket.MultiDelete(writeBatch.Value, viewAndReduceKeyAndLevelAndBucket, id);
                    reduceResultsByView.MultiDelete(writeBatch.Value, viewKey, id);

                    reduceResultsData.Delete(writeBatch.Value, id);

                    if (tryPulseTransaction && generalStorageActions.MaybePulseTransaction(iterator))
                    {
                        iterator = createIterator();
                        if (!iterator.Seek(Slice.BeforeAllKeys))
                            break;

                        skipMoveNext = true;
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
    }

    internal class ScheduleReductionData
    {
        public Etag Etag { get; set; }
        public Slice EtagAsSlice { get; set; }
        public int View { get; set; }
        public int Level { get; set; }
        public string ReduceKey { get; set; }
        public int Bucket { get; set; }
    }

    internal class ScheduledReductionDeleter
    {
        private readonly ConcurrentSet<object> innerSet;

        private readonly IDictionary<Slice, object> state = new Dictionary<Slice, object>(new SliceComparer());

        public ScheduledReductionDeleter(ConcurrentSet<object> set, Func<object, Slice> extractKey)
        {
            innerSet = set;

            InitializeState(set, extractKey);
        }

        private void InitializeState(IEnumerable<object> set, Func<object, Slice> extractKey)
        {
            foreach (var item in set)
            {
                var key = extractKey(item);
                if (key == null)
                    continue;

                state.Add(key, null);
            }
        }

        public bool Delete(Slice key, object value)
        {
            if (state.ContainsKey(key))
                return false;

            state.Add(key, null);
            innerSet.Add(value);

            return true;
        }
    }
}

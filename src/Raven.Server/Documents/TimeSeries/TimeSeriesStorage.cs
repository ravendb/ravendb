using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Workers.Cleanup;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using static Raven.Server.Documents.Schemas.DeletedRanges;
using static Raven.Server.Documents.Schemas.TimeSeries;

namespace Raven.Server.Documents.TimeSeries
{
    public unsafe partial class TimeSeriesStorage
    {
        public const int MaxSegmentSize = 2048;
        public readonly TimeSeriesStats Stats;
        public readonly TimeSeriesRollups Rollups;

        internal readonly TableSchema TimeSeriesSchema;
        internal readonly TableSchema DeleteRangesSchema;

        private readonly DocumentDatabase _documentDatabase;
        private readonly DocumentsStorage _documentsStorage;
        private HashSet<string> _tableCreated = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly Logger _logger;

        public TimeSeriesStorage([NotNull] DocumentDatabase documentDatabase, [NotNull] Transaction tx, [NotNull] TableSchema timeSeriesSchema, [NotNull] TableSchema deleteRangesSchema)
        {
            if (tx == null)
                throw new ArgumentNullException(nameof(tx));

            _documentDatabase = documentDatabase ?? throw new ArgumentNullException(nameof(documentDatabase));
            _documentsStorage = documentDatabase.DocumentsStorage;

            TimeSeriesSchema = timeSeriesSchema ?? throw new ArgumentNullException(nameof(timeSeriesSchema));
            DeleteRangesSchema = deleteRangesSchema ?? throw new ArgumentNullException(nameof(deleteRangesSchema));

            tx.CreateTree(TimeSeriesKeysSlice);
            tx.CreateTree(DeletedRangesKey);

            Stats = new TimeSeriesStats(this, tx);
            Rollups = new TimeSeriesRollups(_documentDatabase);
            _logger = LoggingSource.Instance.GetLogger<TimeSeriesStorage>(documentDatabase.Name);
        }

        public long PurgeSegmentsAndDeletedRanges(DocumentsOperationContext context, string collection, long upto, long numberOfEntriesToDelete)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return 0;

            var deletedSegments = PurgeSegments(upto, context, collectionName, numberOfEntriesToDelete);
            var deletedRanges = PurgeDeletedRanged(upto, context, collectionName, numberOfEntriesToDelete - deletedSegments);
            return deletedRanges + deletedSegments;
        }

        private long PurgeDeletedRanged(in long upto, DocumentsOperationContext context, CollectionName collectionName, long numberOfEntriesToDelete)
        {
            var tableName = collectionName.GetTableName(CollectionTableType.TimeSeriesDeletedRanges);
            var table = context.Transaction.InnerTransaction.OpenTable(DeleteRangesSchema, tableName);

            if (table == null || table.NumberOfEntries == 0 || numberOfEntriesToDelete <= 0)
                return 0;

            return table.DeleteBackwardFrom(DeleteRangesSchema.FixedSizeIndexes[CollectionDeletedRangesEtagsSlice], upto, numberOfEntriesToDelete);
        }

        private long PurgeSegments(long upto, DocumentsOperationContext context, CollectionName collectionName, long numberOfEntriesToDelete)
        {
            var tableName = collectionName.GetTableName(CollectionTableType.TimeSeries);
            var table = context.Transaction.InnerTransaction.OpenTable(TimeSeriesSchema, tableName);

            if (table == null || table.NumberOfEntries == 0 || numberOfEntriesToDelete <= 0)
                return 0;

            var pendingDeletion = context.Transaction.InnerTransaction.CreateTree(PendingDeletionSegments);
            var outdated = new List<Slice>();
            var uniqueTimeSeries = new HashSet<Slice>(SliceComparer.Instance);

            var hasMore = true;
            var deletedCount = 0;
            while (hasMore)
            {
                using (var it = pendingDeletion.MultiRead(collectionName.Name))
                {
                    if (it.Seek(Slices.BeforeAllKeys) == false)
                        return deletedCount;

                    do
                    {
                        var etag = it.CurrentKey.CreateReader().ReadBigEndianInt64();
                        if (etag > upto)
                        {
                            hasMore = false;
                            break;
                        }

                        if (table.FindByIndex(TimeSeriesSchema.FixedSizeIndexes[CollectionTimeSeriesEtagsSlice], etag, out var reader))
                        {
                            var keyPtr = reader.Read((int)TimeSeriesTable.TimeSeriesKey, out int keySize);
                            using (Slice.From(context.Allocator, keyPtr, keySize, ByteStringType.Immutable, out var key))
                            {
                                var size = key.Size - sizeof(long);
                                using (Slice.External(context.Allocator, key, size, out var tsKey))
                                {
                                    if (uniqueTimeSeries.Contains(tsKey) == false)
                                        uniqueTimeSeries.Add(tsKey.Clone(context.Allocator));
                                }
                            }
                        }

                        if (table.DeleteByIndex(TimeSeriesSchema.FixedSizeIndexes[CollectionTimeSeriesEtagsSlice], etag))
                            deletedCount++;

                        outdated.Add(it.CurrentKey.Clone(context.Allocator));

                        hasMore = it.MoveNext();
                    } while (hasMore && outdated.Count < numberOfEntriesToDelete);
                }

                foreach (var etagSlice in outdated)
                {
                    pendingDeletion.MultiDelete(collectionName.Name, etagSlice);
                }
                outdated.Clear();

                foreach (var tsKey in uniqueTimeSeries)
                {
                    if (table.SeekOnePrimaryKeyPrefix(tsKey, out _) == false)
                    {
                        using (Slice.External(context.Allocator, tsKey, tsKey.Size - 1, out var statsKey))
                        {
                            if (Stats.GetStats(context, statsKey).Count == 0)
                                Stats.DeleteStats(context, collectionName, statsKey);
                        }
                    }
                }
                uniqueTimeSeries.Clear();
            }

            return deletedCount;
        }

        public long GetNumberOfTimeSeriesDeletedRanges(DocumentsOperationContext context)
        {
            var fstIndex = DeleteRangesSchema.FixedSizeIndexes[AllDeletedRangesEtagSlice];
            var fst = context.Transaction.InnerTransaction.FixedTreeFor(fstIndex.Name, sizeof(long));
            return fst.NumberOfEntries;
        }

        public long GetNumberOfTimeSeriesPendingDeletionSegments(DocumentsOperationContext context)
        {
            var timeSeriesPurgeSegments = 0L;
            foreach (DocumentsStorage.CollectionStats collection in _documentsStorage.GetCollections(context))
            {
                var pendingDeletion = context.Transaction.InnerTransaction.ReadTree(PendingDeletionSegments);
                if (pendingDeletion == null)
                    continue;

                using (Slice.From(context.Allocator, collection.Name, ByteStringType.Immutable, out var keySlice))
                {
                    timeSeriesPurgeSegments += pendingDeletion.MultiCount(keySlice);
                }
            }

            return timeSeriesPurgeSegments;
        }

        public sealed class DeletionRangeRequest
        {
            public string DocumentId;
            public string Collection;
            public string Name;
            public DateTime From;
            public DateTime To;

            public override string ToString()
            {
                return $"Deletion request for time-series {Name} in document {DocumentId} from {From} to {To}";
            }
        }

        private ChangeVector InsertDeletedRange(DocumentsOperationContext context, DeletionRangeRequest deletionRangeRequest, ChangeVector remoteChangeVector = null)
        {
            var collection = deletionRangeRequest.Collection;
            var documentId = deletionRangeRequest.DocumentId;
            var from = deletionRangeRequest.From;
            var to = deletionRangeRequest.To;
            var name = deletionRangeRequest.Name;

            var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            var table = GetOrCreateDeleteRangesTable(context.Transaction.InnerTransaction, collectionName);

            from = EnsureMillisecondsPrecision(from);
            to = EnsureMillisecondsPrecision(to);

            long etag;
            ChangeVector changeVector;
            if (remoteChangeVector != null)
            {
                changeVector = remoteChangeVector;
                etag = _documentsStorage.GenerateNextEtag();
            }
            else
            {
                (changeVector, etag) = _documentsStorage.GetNewChangeVector(context);
            }

            var hash = (long)Hashing.XXHash64.Calculate(changeVector.Version, Encoding.UTF8);

            using (var sliceHolder = new TimeSeriesSliceHolder(context, documentId, name, collectionName.Name).WithChangeVectorHash(hash))
            {
                if (table.ReadByKey(sliceHolder.TimeSeriesKeySlice, out var tableValueReader))
                {
                    var existingChangeVector = ExtractDeletedRangeChangeVector(context, ref tableValueReader);

                    if (ChangeVectorUtils.GetConflictStatus(changeVector, existingChangeVector) == ConflictStatus.AlreadyMerged)
                    {
                        return null;
                    }

                    // in case of a hash collision (Conflict) we overwrite the existing entry
                }

                using (table.Allocate(out var tvb))
                using (Slice.From(context.Allocator, changeVector, out var cv))
                {
                    tvb.Add(sliceHolder.TimeSeriesKeySlice);
                    tvb.Add(Bits.SwapBytes(etag));
                    tvb.Add(cv);
                    tvb.Add(sliceHolder.CollectionSlice);
                    tvb.Add(context.GetTransactionMarker());
                    tvb.Add(from.Ticks);
                    tvb.Add(to.Ticks);

                    table.Set(tvb);
                }

                return changeVector;
            }
        }

        public string DeleteTimestampRange(DocumentsOperationContext context, DeletionRangeRequest deletionRangeRequest, ChangeVector remoteChangeVector = null, bool updateMetadata = true)
        {
            deletionRangeRequest.From = EnsureMillisecondsPrecision(deletionRangeRequest.From);
            deletionRangeRequest.To = EnsureMillisecondsPrecision(deletionRangeRequest.To);

            remoteChangeVector = InsertDeletedRange(context, deletionRangeRequest, remoteChangeVector);
            if (remoteChangeVector == null)
                return null;

            var collection = deletionRangeRequest.Collection;
            var documentId = deletionRangeRequest.DocumentId;
            var from = deletionRangeRequest.From;
            var to = deletionRangeRequest.To;
            var name = deletionRangeRequest.Name;

            var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            var table = GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);

            using (var slicer = new TimeSeriesSliceHolder(context, documentId, name))
            {
                var stats = Stats.GetStats(context, slicer);
                if (stats == default || stats.Count == 0)
                    return null; // nothing to delete here

                if (stats.End < from)
                    return null; // nothing to delete here

                Debug.Assert(stats.Start.Kind == DateTimeKind.Utc);
                slicer.SetBaselineToKey(stats.Start > from ? stats.Start : from);

                // first try to find the previous segment containing from value
                if (table.SeekOneBackwardByPrimaryKeyPrefix(slicer.TimeSeriesPrefixSlice, slicer.TimeSeriesKeySlice, out var segmentValueReader) == false)
                {
                    // or the first segment _after_ the from value
                    if (table.SeekOnePrimaryKeyWithPrefix(slicer.TimeSeriesPrefixSlice, slicer.TimeSeriesKeySlice, out segmentValueReader) == false)
                        return null;
                }

                if (from == DateTime.MinValue && to == DateTime.MaxValue ||
                    from <= stats.Start && to >= stats.End)
                {
                    table.DeleteByPrimaryKeyPrefix(slicer.TimeSeriesPrefixSlice);
                    Stats.DeleteStats(context, collectionName, slicer.StatsKey);

                    if (updateMetadata)
                        RemoveTimeSeriesNameFromMetadata(context, slicer.DocId, slicer.Name);

                    context.Transaction.AddAfterCommitNotification(new TimeSeriesChange
                    {
                        ChangeVector = remoteChangeVector,
                        DocumentId = documentId,
                        Name = name,
                        Type = TimeSeriesChangeTypes.Delete,
                        From = from,
                        To = to,
                        CollectionName = collection
                    });

                    return remoteChangeVector;
                }

                var baseline = GetBaseline(segmentValueReader);
                string changeVector = null;
                var deleted = 0;

                while (true)
                {
                    if (TryDeleteRange(out var nextSegment) == false)
                        break;

                    if (nextSegment == null)
                        break;

                    baseline = nextSegment.Value;
                }

                if (deleted == 0)
                    return null; // nothing happened, the deletion request was out of date

                context.Transaction.AddAfterCommitNotification(new TimeSeriesChange
                {
                    ChangeVector = changeVector,
                    DocumentId = documentId,
                    Name = name,
                    Type = TimeSeriesChangeTypes.Delete,
                    From = from,
                    To = to,
                    CollectionName = collectionName.Name
                });

                return changeVector;

                bool TryDeleteRange(out DateTime? next)
                {
                    next = default;

                    if (baseline > to)
                        return false; // we got to the end

                    using (var holder = new TimeSeriesSegmentHolder(this, context, documentId, name, collectionName, baseline, remoteChangeVector))
                    {
                        if (holder.LoadClosestSegment() == false)
                            return false;

                        // we need to get the next segment before the actual remove, since it might lead to a split
                        next = BaselineOfNextSegment(slicer.TimeSeriesKeySlice, slicer.TimeSeriesPrefixSlice, table, baseline);

                        var readOnlySegment = holder.ReadOnlySegment;
                        var end = readOnlySegment.GetLastTimestamp(baseline);

                        if (baseline > end)
                            return false;

                            if (ChangeVectorUtils.GetConflictStatus(remoteChangeVector, holder.ReadOnlyChangeVector) == ConflictStatus.AlreadyMerged)
                            {
                                // the deleted range is older than this segment, so we don't touch this segment
                                return false;
                            }

                        if (readOnlySegment.NumberOfLiveEntries == 0)
                            return true; // nothing to delete here

                        var newSegment = new TimeSeriesValuesSegment(holder.SliceHolder.SegmentBuffer.Ptr, MaxSegmentSize);

                        var canDeleteEntireSegment = baseline >= from && end <= to; // entire segment can be deleted
                        var numberOfValues = canDeleteEntireSegment ? 0 : readOnlySegment.NumberOfValues;

                        newSegment.Initialize(numberOfValues);

                        if (canDeleteEntireSegment && readOnlySegment.NumberOfLiveEntries > 1)
                        {
                            deleted += readOnlySegment.NumberOfLiveEntries;
                            Span<double> emptyValues = stackalloc double[numberOfValues];
                            holder.AddNewValue(baseline, emptyValues, Slices.Empty.AsSpan(), ref newSegment, TimeSeriesValuesSegment.Dead);
                            holder.AddNewValue(end, emptyValues, Slices.Empty.AsSpan(), ref newSegment, TimeSeriesValuesSegment.Dead);
                            holder.AppendDeadSegment(newSegment);
                            if (updateMetadata)
                            {
                                // in case this deleted segment was the only segment that exists for this TS
                                // we need to remove its name from the metadata
                                RemoveTimeSeriesNameFromMetadata(context, slicer.DocId, slicer.Name);
                            }

                            changeVector = holder.ChangeVector;
                            return true;
                        }

                        var segmentChanged = false;
                        using (var enumerator = readOnlySegment.GetEnumerator(context.Allocator))
                        {
                            var state = new TimestampState[readOnlySegment.NumberOfValues];
                            Span<double> values = stackalloc double[readOnlySegment.NumberOfValues];
                            Span<double> emptyValues = stackalloc double[numberOfValues];
                            var tag = new TimeSeriesValuesSegment.TagPointer();

                            while (enumerator.MoveNext(out int ts, values, state, ref tag, out var status))
                            {
                                var current = baseline.AddMilliseconds(ts);

                                if (current > to && segmentChanged == false)
                                    return false; // we can exit here earlier

                                if (current >= from && current <= to)
                                {
                                    holder.AddNewValue(current, emptyValues, Slices.Empty.AsSpan(), ref newSegment, TimeSeriesValuesSegment.Dead);
                                    segmentChanged = true;
                                    deleted++;
                                    continue;
                                }

                                holder.AddExistingValue(current, values, tag.AsSpan(), ref newSegment, status);
                            }
                        }

                        if (segmentChanged)
                        {
                            var count = holder.AppendExistingSegment(newSegment);
                            if (count == 0 && updateMetadata)
                            {
                                // this ts was completely deleted
                                RemoveTimeSeriesNameFromMetadata(context, slicer.DocId, slicer.Name);
                            }
                            changeVector = holder.ChangeVector;
                        }
                        else if (holder.FromReplication)
                            holder.UpdateSegmentChangeVector(newSegment);

                        return end < to;
                    }
                }
            }
        }

        private DateTime? BaselineOfNextSegment(Slice key, Slice prefix, Table table, DateTime baseline)
        {
            var offset = key.Size - sizeof(long);
            *(long*)(key.Content.Ptr + offset) = Bits.SwapBytes(baseline.Ticks / 10_000);

            foreach (var (_, tvh) in table.SeekByPrimaryKeyPrefix(prefix, key, 0))
            {
                return GetBaseline(tvh.Reader);
            }

            return null;
        }

        private static DateTime GetBaseline(TableValueReader reader)
        {
            var key = reader.Read((int)TimeSeriesTable.TimeSeriesKey, out int keySize);
            return GetBaseline(key, keySize);
        }

        public static DateTime GetBaseline(byte* key, int keySize)
        {
            var baselineMilliseconds = Bits.SwapBytes(
                *(long*)(key + keySize - sizeof(long))
            );
            var ticks = baselineMilliseconds * 10_000;
            var baseline = new DateTime(ticks);
            return baseline;
        }

        public static void RemoveTimeSeriesNameFromMetadata(DocumentsOperationContext ctx, string docId, string tsName)
        {
            var storage = ctx.DocumentDatabase.DocumentsStorage;

            var tss = storage.TimeSeriesStorage;
            if (tss.Stats.GetStats(ctx, docId, tsName).Count > 0)
                return;
            
            var doc = storage.Get(ctx, docId);
            if (doc == null)
                return;

            var data = doc.Data;
            var flags = DocumentFlags.None;

            BlittableJsonReaderArray tsNames = null;
            if (doc.TryGetMetadata(out var metadata))
            {
                metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out tsNames);
            }

            if (metadata == null || tsNames == null)
                return;

            var tsNamesList = new List<string>(tsNames.Length + 1);
            for (var i = 0; i < tsNames.Length; i++)
            {
                var val = tsNames.GetStringByIndex(i);
                if (val == null)
                    continue;
                tsNamesList.Add(val);
            }

            var location = tsNames.BinarySearch(tsName, StringComparison.OrdinalIgnoreCase);
            if (location < 0)
                return;

            tsNamesList.RemoveAt(location);

            data.Modifications = new DynamicJsonValue(data);
            metadata.Modifications = new DynamicJsonValue(metadata);

            if (tsNamesList.Count == 0)
            {
                metadata.Modifications.Remove(Constants.Documents.Metadata.TimeSeries);
            }
            else
            {
                flags = DocumentFlags.HasTimeSeries;
                metadata.Modifications[Constants.Documents.Metadata.TimeSeries] = tsNamesList;
            }

            data.Modifications[Constants.Documents.Metadata.Key] = metadata;

            using (data)
            {
                var newDocumentData = ctx.ReadObject(doc.Data, doc.Id, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                storage.Put(ctx, doc.Id, null, newDocumentData, flags: flags,
                    nonPersistentFlags: NonPersistentDocumentFlags.ByTimeSeriesUpdate);
            }
        }

        private static TimeSeriesValuesSegment TableValueToSegment(ref TableValueReader segmentValueReader, out DateTime baseline)
        {
            var segmentPtr = segmentValueReader.Read((int)TimeSeriesTable.Segment, out int segmentSize);
            var segment = new TimeSeriesValuesSegment(segmentPtr, segmentSize);
            Debug.Assert(segmentSize == segment.NumberOfBytes);

            var key = segmentValueReader.Read((int)TimeSeriesTable.TimeSeriesKey, out int keySize);
            var baselineMilliseconds = Bits.SwapBytes(
                *(long*)(key + keySize - sizeof(long))
            );
            var ticks = baselineMilliseconds * 10_000;
            baseline = new DateTime(ticks);
            return segment;
        }

        public static DateTime EnsureMillisecondsPrecision(DateTime dt)
        {
            if (dt == DateTime.MinValue || dt == DateTime.MaxValue)
                return dt;

            var remainder = dt.Ticks % 10_000;
            if (remainder != 0)
                dt = dt.AddTicks(-remainder);

            return dt;
        }

        public void DeleteAllTimeSeriesForDocument(DocumentsOperationContext context, string documentId, CollectionName collection, DocumentFlags flags)
        {
            // this will be called as part of document's delete

            // create 'DeletedRange' items
            var seriesNames = Stats.GetTimeSeriesNamesForDocumentOriginalCasing(context, documentId);

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Normal, "This is workaround until proper fix of https://issues.hibernatingrhinos.com/issue/RavenDB-19635/Handle-delete-bucket-for-time-series-counters");
            if (flags.HasFlag(DocumentFlags.FromResharding | DocumentFlags.Artificial) == false)
            {
                var deletionRangeRequest = new DeletionRangeRequest
                {
                    DocumentId = documentId,
                    Collection = collection.Name,
                    From = DateTime.MinValue,
                    To = DateTime.MaxValue
                };
                foreach (var name in seriesNames)
                {
                    deletionRangeRequest.Name = name;
                    InsertDeletedRange(context, deletionRangeRequest);
                }
            }

            // delete segments, stats and roll-ups
            var table = GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collection);
            using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice documentKeyPrefix, SpecialChars.RecordSeparator))
            {
                table.DeleteByPrimaryKeyPrefix(documentKeyPrefix);
                Stats.DeleteByPrimaryKeyPrefix(context, collection, documentKeyPrefix);
                Rollups.DeleteByPrimaryKeyPrefix(context, documentKeyPrefix);
            }
        }

        public TimeSeriesReader GetReader(DocumentsOperationContext context, string documentId, string name, DateTime from, DateTime to, TimeSpan? offset = null)
        {
            return new TimeSeriesReader(context, documentId, name, from, to, offset);
        }

        public bool TryAppendEntireSegment(DocumentsOperationContext context, TimeSeriesReplicationItem item, string docId, LazyStringValue name, ChangeVector changeVector, DateTime baseline)
        {
            var collectionName = _documentsStorage.ExtractCollectionName(context, item.Collection);
            return TryAppendEntireSegment(context, item.Key, docId, name, collectionName, changeVector, item.Segment, baseline);
        }

        private bool TryAppendEntireSegment(
            DocumentsOperationContext context,
            Slice key,
            string documentId,
            string name,
            CollectionName collectionName,
            ChangeVector changeVector,
            TimeSeriesValuesSegment segment,
            DateTime baseline)
        {
            var table = GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);

            if (table.ReadByKey(key, out var tvr))
            {
                var existingChangeVector = DocumentsStorage.TableValueToChangeVector(context, (int)TimeSeriesTable.ChangeVector, ref tvr);

                var status = ChangeVectorUtils.GetConflictStatus(changeVector, existingChangeVector);

                if (status == ConflictStatus.AlreadyMerged)
                    return true; // nothing to do, we already have this

                if (status == ConflictStatus.Update)
                {
                    return AppendEntireSegment(context, key, documentId, name, collectionName, changeVector, segment, baseline);
                }

                return false;
            }

            if (Stats.GetStats(context, documentId, name) == default &&
                SegmentAlreadyDeleted(context, documentId, name, changeVector, collectionName, segment, baseline))
            {
                // if we reach this point, it means the entire time series was deleted,
                // and the deletion has a newer change vector than this segment.
                // since the deletion is more recent, we are up-to-date and can safely return
                return true;
            }

            return TryPutSegmentDirectly(context, key, documentId, name, collectionName, changeVector, segment, baseline);
        }

        public bool TryAppendEntireSegmentFromSmuggler(DocumentsOperationContext context, Slice key, CollectionName collectionName, TimeSeriesItem item)
        {
            if (item.Segment.NumberOfLiveEntries == 0)
            {
                // in case of a dead segment we need to update the stats properly
                return AppendEntireSegment(context, key, item.DocId, context.GetLazyStringForFieldWithCaching(item.Name), collectionName, null, item.Segment, item.Baseline);
            }

            // we will generate new change vector, so we pass null here
            return TryPutSegmentDirectly(context, key, item.DocId, context.GetLazyStringForFieldWithCaching(item.Name), collectionName, null, item.Segment, item.Baseline);
        }

        private bool TryPutSegmentDirectly(
            DocumentsOperationContext context,
            Slice key,
            string documentId,
            string name,
            CollectionName collectionName,
            ChangeVector changeVector,
            TimeSeriesValuesSegment segment,
            DateTime baseline)
        {
            if (IsOverlapping(context, key, collectionName, segment, baseline))
                return false;

            // if this segment isn't overlap with any other we can put it directly
            using (var holder = new TimeSeriesSegmentHolder(this, context, documentId, name, collectionName, fromReplicationChangeVector: changeVector, timeStamp: baseline))
            {
                if (holder.LoadCurrentSegment())
                {
                    // if we got here it means that `IsOverlapping` is false
                    // but a segment with matching ranges exists 
                    // so we should append to this segment instead of creating a new one
                    return false;
                }

                holder.AppendToNewSegment(segment, baseline);

                context.Transaction.AddAfterCommitNotification(new TimeSeriesChange
                {
                    CollectionName = collectionName.Name,
                    ChangeVector = holder.ChangeVector,
                    DocumentId = documentId,
                    Name = name,
                    Type = TimeSeriesChangeTypes.Put,
                    From = DateTime.MinValue,
                    To = DateTime.MaxValue
                });
            }

            return true;
        }

        private bool AppendEntireSegment(DocumentsOperationContext context,
            Slice key,
            string documentId,
            string name,
            CollectionName collectionName,
            ChangeVector changeVector,
            TimeSeriesValuesSegment segment,
            DateTime baseline)
        {
            if (IsOverlapping(context, key, collectionName, segment, baseline, canUpdateExistingSegment: true))
                return false;

            using (var holder = new TimeSeriesSegmentHolder(this, context, documentId, name, collectionName, fromReplicationChangeVector: changeVector, timeStamp: baseline))
            {
                if (holder.LoadClosestSegment() == false)
                    holder.AppendToNewSegment(segment, baseline);
                else
                    holder.AppendExistingSegment(segment);

                context.Transaction.AddAfterCommitNotification(new TimeSeriesChange
                {
                    CollectionName = collectionName.Name,
                    ChangeVector = holder.ChangeVector,
                    DocumentId = documentId,
                    Name = name,
                    Type = TimeSeriesChangeTypes.Put,
                    From = DateTime.MinValue,
                    To = DateTime.MaxValue
                });

                return true;
            }
        }

        public bool EnsureNoOverlap(
            DocumentsOperationContext context,
            Slice key,
            CollectionName collectionName,
            TimeSeriesValuesSegment segment,
            DateTime baseline)
        {
            var table = GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);
            using (Slice.From(context.Allocator, key.Content.Ptr, key.Size - sizeof(long), ByteStringType.Immutable, out var prefix))
            {
                return (IsOverlapWithHigherSegment(prefix) || IsOverlapWithLowerSegment(prefix)) == false;
            }

            bool IsOverlapWithHigherSegment(Slice prefix)
            {
                var lastTimestamp = segment.GetLastTimestamp(baseline);
                var nextSegmentBaseline = BaselineOfNextSegment(key, prefix, table, baseline);
                return lastTimestamp >= nextSegmentBaseline;
            }

            bool IsOverlapWithLowerSegment(Slice prefix)
            {
                TableValueReader tvr;
                using (Slice.From(context.Allocator, key.Content.Ptr, key.Size, ByteStringType.Immutable, out var lastKey))
                {
                    *(long*)(lastKey.Content.Ptr + lastKey.Size - sizeof(long)) = Bits.SwapBytes(baseline.Ticks / 10_000);
                    if (table.SeekOneBackwardByPrimaryKeyPrefix(prefix, lastKey, out tvr, excludeValueFromSeek: true) == false)
                    {
                        return false;
                    }
                }

                var prevSegment = TableValueToSegment(ref tvr, out var prevBaseline);
                var last = prevSegment.GetLastTimestamp(prevBaseline);
                return last >= baseline;
            }
        }

        public bool IsOverlapping(
                    DocumentsOperationContext context,
                    Slice key,
                    CollectionName collectionName,
                    TimeSeriesValuesSegment segment,
                    DateTime baseline,
                    bool canUpdateExistingSegment = false)
        {
            var table = GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);
            using (Slice.From(context.Allocator, key.Content.Ptr, key.Size - sizeof(long), ByteStringType.Immutable, out var prefix))
            {
                return IsOverlapWithHigherSegment(prefix) || IsOverlapWithLowerSegment(prefix);
            }

            bool IsOverlapWithHigherSegment(Slice prefix)
            {
                var lastTimestamp = segment.GetLastTimestamp(baseline);
                var nextSegmentBaseline = BaselineOfNextSegment(key, prefix, table, baseline);
                return lastTimestamp >= nextSegmentBaseline;
            }

            bool IsOverlapWithLowerSegment(Slice prefix)
            {
                var myLastTimestamp = segment.GetLastTimestamp(baseline);
                TableValueReader tvr;
                using (Slice.From(context.Allocator, key.Content.Ptr, key.Size, ByteStringType.Immutable, out var lastKey))
                {
                    *(long*)(lastKey.Content.Ptr + lastKey.Size - sizeof(long)) = Bits.SwapBytes(myLastTimestamp.Ticks / 10_000);
                    if (table.SeekOneBackwardByPrimaryKeyPrefix(prefix, lastKey, out tvr) == false)
                    {
                        return false;
                    }
                }

                var prevSegment = TableValueToSegment(ref tvr, out var prevBaseline);
                var prevLastTimestamp = prevSegment.GetLastTimestamp(prevBaseline);

                if (canUpdateExistingSegment)
                {
                    // in case of an update, we must accept only segments that their baseline equals to the existing segment baseline
                    // and their last timestamp greater or equal to last timestamp of existing segment
                    // by that, we can ensure we will not lose values in case of a split or delete
                    return baseline > prevBaseline || myLastTimestamp < prevLastTimestamp;
                }

                return prevLastTimestamp >= baseline;
            }
        }

        private bool SegmentAlreadyDeleted(DocumentsOperationContext context, string documentId, string name, string changeVector, 
            CollectionName collectionName, TimeSeriesValuesSegment segment, DateTime baseline)
        {
            var hash = (long)Hashing.XXHash64.Calculate(changeVector, Encoding.UTF8);
            using (var sliceHolder = new TimeSeriesSliceHolder(context, documentId, name, collectionName.Name).WithChangeVectorHash(hash))
            {
                var table = GetOrCreateDeleteRangesTable(context.Transaction.InnerTransaction, collectionName);
                if (table == null || table.NumberOfEntries == 0)
                    return false;

                foreach (var (_, tableValueHolder) in table.SeekByPrimaryKeyPrefix(sliceHolder.TimeSeriesPrefixSlice, Slices.Empty, skip: 0))
                {
                    var item = CreateDeletedRangeItem(context, ref tableValueHolder.Reader);
                    
                    if (item.From > baseline || item.To < segment.GetLastTimestamp(baseline))
                        continue;

                    if (ChangeVectorUtils.GetConflictStatus(changeVector, item.ChangeVector) == ConflictStatus.AlreadyMerged)
                        return true;
                }

                return false;
            }
        }

        public sealed class SegmentSummary
        {
            public string DocumentId;
            public string Name;
            public DateTime StartTime;
            public int NumberOfEntries;
            public int NumberOfLiveEntries;
            public string ChangeVector;

            public DynamicJsonValue ToJson()
            {
                var json = new DynamicJsonValue
                {
                    [nameof(DocumentId)] = DocumentId,
                    [nameof(Name)] = Name,
                    [nameof(StartTime)] = StartTime,
                    [nameof(NumberOfEntries)] = NumberOfEntries,
                    [nameof(NumberOfLiveEntries)] = NumberOfLiveEntries,
                    [nameof(ChangeVector)] = ChangeVector,
                };
                return json;
            }
        }

        public sealed class TimeSeriesSegmentHolder : IDisposable
        {
            private readonly TimeSeriesStorage _tss;
            private readonly DocumentsOperationContext _context;
            public readonly TimeSeriesSliceHolder SliceHolder;
            public readonly bool FromReplication;
            public readonly bool FromSmuggler;
            public ChangeVector ChangeVectorFromReplication;
            private readonly string _docId;
            private readonly CollectionName _collection;
            private readonly string _name;

            private TableValueReader _tvr;

            public long BaselineMilliseconds => BaselineDate.Ticks / 10_000;
            public DateTime BaselineDate;
            public TimeSeriesValuesSegment ReadOnlySegment;
            public ChangeVector ReadOnlyChangeVector;

            private long _currentEtag;

            private ChangeVector _currentChangeVector;
            public ChangeVector ChangeVector => _currentChangeVector;

            private AllocatedMemoryData _clonedReadonlySegment;

            private TimeSeriesSegmentHolder(
                TimeSeriesStorage tss,
                DocumentsOperationContext context,
                string docId,
                string name,
                CollectionName collection,
                ChangeVector fromReplicationChangeVector
            )
            {
                _tss = tss;
                _context = context;
                _collection = collection;
                _docId = docId;
                _name = name;

                FromReplication = fromReplicationChangeVector != null;
                ChangeVectorFromReplication = fromReplicationChangeVector;
            }

            public TimeSeriesSegmentHolder(
                TimeSeriesStorage tss,
                DocumentsOperationContext context,
                string docId,
                string name,
                CollectionName collection,
                DateTime timeStamp,
                ChangeVector fromReplicationChangeVector = null
            ) : this(tss, context, docId, name, collection, fromReplicationChangeVector)
            {
                SliceHolder = new TimeSeriesSliceHolder(_context, docId, name, _collection.Name).WithBaseline(timeStamp);
                SliceHolder.CreateSegmentBuffer();
            }

            public TimeSeriesSegmentHolder(
                TimeSeriesStorage tss,
                DocumentsOperationContext context,
                TimeSeriesSliceHolder allocator,
                string docId,
                string name,
                CollectionName collection,
                AppendOptions options
            ) : this(tss, context, docId, name, collection, options.ChangeVectorFromReplication)
            {
                FromSmuggler = options.FromSmuggler;
                SliceHolder = allocator;
                BaselineDate = allocator.CurrentBaseline;
                allocator.CreateSegmentBuffer();
            }

            private void Initialize()
            {
                Debug.Assert(_tvr.Equals(default) == false);
                var readOnlySegment = TableValueToSegment(ref _tvr, out BaselineDate);

                // while appending or deleting, we might change the same segment.
                // So we clone it.
                Debug.Assert(_clonedReadonlySegment == null);
                _clonedReadonlySegment = readOnlySegment.Clone(_context, out ReadOnlySegment);
                ReadOnlyChangeVector = DocumentsStorage.TableValueToChangeVector(_context, (int)TimeSeriesTable.ChangeVector, ref _tvr);

                SliceHolder.SetBaselineToKey(BaselineDate);

                _countWasReduced = false;
            }

            public Table Table => _tss.GetOrCreateTimeSeriesTable(_context.Transaction.InnerTransaction, _collection);

            private bool _countWasReduced;

            private void ReduceCountBeforeAppend()
            {
                if (_countWasReduced)
                    return;

                // we modified this segment so we need to reduce the original count
                _countWasReduced = true;
                _tss.Stats.UpdateCountOfExistingStats(_context, SliceHolder, _collection, -ReadOnlySegment.NumberOfLiveEntries);
            }

            private ChangeVector GetSegmentMergedChangeVector()
            {
                if (ReadOnlyChangeVector == null)
                {
                    // append new segment - take change vector from replication or create a new one
                    return ChangeVectorFromReplication ?? _tss._documentsStorage.GetNewChangeVector(_context, _currentEtag);
                }

                if (ChangeVectorFromReplication == null)
                {
                    // local change - merge existing change vector with new database change vector
                    return ChangeVector.MergeWithNewDatabaseChangeVector(_context, ReadOnlyChangeVector, _currentEtag);
                }

                ChangeVector mergedChangeVector = ChangeVector.Merge(ReadOnlyChangeVector, ChangeVectorFromReplication, _context);
                return ChangeVectorUtils.GetConflictStatus(ChangeVectorFromReplication, ReadOnlyChangeVector) switch
                {
                    ConflictStatus.Update => mergedChangeVector,
                    ConflictStatus.Conflict => ChangeVector.MergeWithNewDatabaseChangeVector(_context, mergedChangeVector, _currentEtag),
                    ConflictStatus.AlreadyMerged => ReadOnlyChangeVector,
                    _ => throw new ArgumentOutOfRangeException()
                };
            }

            public long AppendExistingSegment(TimeSeriesValuesSegment newValueSegment)
            {
                if (newValueSegment.RecomputeRequired)
                    newValueSegment = newValueSegment.Recompute(_context.Allocator);

                _currentEtag = _tss._documentsStorage.GenerateNextEtag();
                _currentChangeVector = GetSegmentMergedChangeVector();

                ValidateSegment(newValueSegment);
                if (newValueSegment.NumberOfLiveEntries == 0)
                {
                    MarkSegmentAsPendingDeletion(_context, _collection.Name, _currentEtag);
                }

                var modifiedEntries = Math.Abs(newValueSegment.NumberOfLiveEntries - ReadOnlySegment.NumberOfLiveEntries);
                ReduceCountBeforeAppend();
                var count = _tss.Stats.UpdateStats(_context, SliceHolder, _collection, newValueSegment, BaselineDate, modifiedEntries);

                using (Table.Allocate(out var tvb))
                using (Slice.From(_context.Allocator, _currentChangeVector, out var cv))
                {
                    tvb.Add(SliceHolder.TimeSeriesKeySlice);
                    tvb.Add(Bits.SwapBytes(_currentEtag));
                    tvb.Add(cv);
                    tvb.Add(newValueSegment.Ptr, newValueSegment.NumberOfBytes);
                    tvb.Add(SliceHolder.CollectionSlice);
                    tvb.Add(_context.GetTransactionMarker());

                    Table.Set(tvb);
                }

                EnsureStatsAndDataIntegrity(_context, _docId, _name, newValueSegment, _collection, BaselineDate);

                return count;
            }

            public void AppendDeadSegment(TimeSeriesValuesSegment newValueSegment)
            {
                _currentEtag = _tss._documentsStorage.GenerateNextEtag();
                _currentChangeVector = GetSegmentMergedChangeVector();

                ValidateSegment(newValueSegment);
                MarkSegmentAsPendingDeletion(_context, _collection.Name, _currentEtag);

                ReduceCountBeforeAppend();
                _tss.Stats.UpdateStats(_context, SliceHolder, _collection, newValueSegment, BaselineDate, ReadOnlySegment.NumberOfLiveEntries);
                using (Slice.From(_context.Allocator, _currentChangeVector, out Slice cv))
                using (Table.Allocate(out var tvb))
                {
                    tvb.Add(SliceHolder.TimeSeriesKeySlice);
                    tvb.Add(Bits.SwapBytes(_currentEtag));
                    tvb.Add(cv);
                    tvb.Add(newValueSegment.Ptr, newValueSegment.NumberOfBytes);
                    tvb.Add(SliceHolder.CollectionSlice);
                    tvb.Add(_context.GetTransactionMarker());

                    Table.Set(tvb);
                }

                EnsureStatsAndDataIntegrity(_context, _docId, _name, newValueSegment, _collection, BaselineDate);
            }

            public void AppendToNewSegment(TimeSeriesValuesSegment newSegment, DateTime baseline)
            {
                ValidateSegment(newSegment);

                BaselineDate = EnsureMillisecondsPrecision(baseline);
                SliceHolder.SetBaselineToKey(BaselineDate);

                _currentEtag = _tss._documentsStorage.GenerateNextEtag();
                _currentChangeVector = GetSegmentMergedChangeVector();

                _tss.Stats.UpdateStats(_context, SliceHolder, _collection, newSegment, BaselineDate, newSegment.NumberOfLiveEntries);
                _tss._documentDatabase.TimeSeriesPolicyRunner?.MarkSegmentForPolicy(_context, SliceHolder, baseline, _currentChangeVector, newSegment.NumberOfLiveEntries);

                if (newSegment.NumberOfLiveEntries == 0)
                {
                    MarkSegmentAsPendingDeletion(_context, _collection.Name, _currentEtag);
                }

                using (Slice.From(_context.Allocator, _currentChangeVector, out Slice cv))
                using (Table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(SliceHolder.TimeSeriesKeySlice);
                    tvb.Add(Bits.SwapBytes(_currentEtag));
                    tvb.Add(cv);
                    tvb.Add(newSegment.Ptr, newSegment.NumberOfBytes);
                    tvb.Add(SliceHolder.CollectionSlice);
                    tvb.Add(_context.GetTransactionMarker());

                    Table.Insert(tvb);
                }

                EnsureStatsAndDataIntegrity(_context, _docId, _name, newSegment, _collection, BaselineDate);
            }

            public void AppendToNewSegment(SingleResult item)
            {
                var newSegment = new TimeSeriesValuesSegment(SliceHolder.SegmentBuffer.Ptr, MaxSegmentSize);
                newSegment.Initialize(item.Values.Length);
                newSegment.Append(_context.Allocator, 0, item.Values.Span, SliceHolder.TagAsSpan(item.Tag), item.Status);

                AppendToNewSegment(newSegment, item.Timestamp);
            }

            public void UpdateSegmentChangeVector(TimeSeriesValuesSegment segment)
            {
                if (ChangeVectorFromReplication == null || ChangeVectorUtils.GetConflictStatus(ChangeVectorFromReplication, ReadOnlyChangeVector) != ConflictStatus.Conflict)
                    return;

                _currentEtag = _tss._documentsStorage.GenerateNextEtag();
                _currentChangeVector = ChangeVector.Merge(ReadOnlyChangeVector, ChangeVectorFromReplication, _context);

                if (segment.RecomputeRequired)
                    segment = segment.Recompute(_context.Allocator);

                ValidateSegment(segment);

                using (Slice.From(_context.Allocator, _currentChangeVector, out Slice cv))
                using (Table.Allocate(out var tvb))
                {
                    tvb.Add(SliceHolder.TimeSeriesKeySlice);
                    tvb.Add(Bits.SwapBytes(_currentEtag));
                    tvb.Add(cv);
                    tvb.Add(segment.Ptr, segment.NumberOfBytes);
                    tvb.Add(SliceHolder.CollectionSlice);
                    tvb.Add(_context.GetTransactionMarker());

                    Table.Set(tvb);
                }
            }

            public void AddNewValue(SingleResult result, ref TimeSeriesValuesSegment segment)
            {
                AddNewValue(result.Timestamp, result.Values.Span, SliceHolder.TagAsSpan(result.Tag), ref segment, result.Status);
            }

            public void AddNewValue(DateTime time, Span<double> values, Span<byte> tagSlice, ref TimeSeriesValuesSegment segment, ulong status)
            {
                AddValueInternal(time, values, tagSlice, ref segment, status);
                _context.DocumentDatabase.TimeSeriesPolicyRunner?.MarkForPolicy(_context, SliceHolder, time, status);
            }

            public void AddExistingValue(DateTime time, Span<double> values, Span<byte> tagSlice, ref TimeSeriesValuesSegment segment, ulong status)
            {
                AddValueInternal(time, values, tagSlice, ref segment, status);
            }

            private void AddValueInternal(DateTime time, Span<double> values, Span<byte> tagSlice, ref TimeSeriesValuesSegment segment, ulong status)
            {
                var timestampDiff = ((time - BaselineDate).Ticks / 10_000);
                var inRange = timestampDiff < int.MaxValue;
                if (inRange == false || segment.Append(_context.Allocator, (int)timestampDiff, values, tagSlice, status) == false)
                {
                    var segmentLastTimestamp = segment.GetLastTimestamp(BaselineDate);
                    if (segmentLastTimestamp == BaselineDate && time == segmentLastTimestamp) // all the entries in the segment has the same timestamp
                    {
                        if (FromReplication)
                            RaiseAlertOnMaxCapacityForSingleSegmentReached(values);

                        throw new InvalidDataException(
                            $"Segment reached to capacity and cannot receive more values with {time} timestamp on time series {_name} for {_docId} on database '{_tss._documentDatabase.Name}'. " +
                            "You may choose a different timestamp.");
                    }

                    if (segmentLastTimestamp == time) // special split segment case
                    {
                        TrimSegmentToDate(time, ref segment);
                        segment.Append(_context.Allocator, 0, values, tagSlice, status);
                        return;
                    }

                    FlushCurrentSegment(ref segment, values, tagSlice, status);
                    UpdateBaseline(timestampDiff);
                }
            }

            private void RaiseAlertOnMaxCapacityForSingleSegmentReached(Span<double> values)
            {
                var msg = $"Segment reached capacity (2KB) and open a new segment unavailable at this point." +
                          $"An evict operation has performed for replication on doc: {_docId}, name: {_name} at {BaselineDate}. " +
                          $"The following values has been removed [{string.Join(", ", values.ToArray())}]";


                var alert = AlertRaised.Create(_context.DocumentDatabase.Name, "Time series segment is full - merge operation has performed", msg,
                    AlertType.Replication, NotificationSeverity.Warning);
                _tss._documentDatabase.NotificationCenter.Add(alert);
            }

            //Return true if we have the same key or the closest result to the sliceHolder
            public bool LoadClosestSegment()
            {
                if (Table.SeekOneBackwardByPrimaryKeyPrefix(SliceHolder.TimeSeriesPrefixSlice, SliceHolder.TimeSeriesKeySlice, out _tvr))
                {
                    Initialize();
                    return true;
                }

                return false;
            }

            //Return true only if we have the same key as the sliceHolder
            public bool LoadCurrentSegment()
            {
                if (Table.ReadByKey(SliceHolder.TimeSeriesKeySlice, out _tvr))
                {
                    Initialize();
                    return true;
                }
                return false;
            }

            private void FlushCurrentSegment(
                ref TimeSeriesValuesSegment splitSegment,
                Span<double> currentValues,
                Span<byte> currentTag,
                ulong status)
            {
                AppendExistingSegment(splitSegment);

                splitSegment.Initialize(currentValues.Length);

                var result = splitSegment.Append(_context.Allocator, 0, currentValues, currentTag, status);
                if (result == false)
                    throw new InvalidOperationException($"After renewal of segment, was unable to append a new value. Shouldn't happen. Doc: {_docId}, name: {_name}");
            }

            private void UpdateBaseline(long timestampDiff)
            {
                Debug.Assert(timestampDiff > 0);
                BaselineDate = BaselineDate.AddMilliseconds(timestampDiff);
                SliceHolder.SetBaselineToKey(BaselineDate);
            }

            private void TrimSegmentToDate(DateTime trimTimestamp, ref TimeSeriesValuesSegment timeSeriesValuesSegment)
            {
                // we ran out of space (up to 2KB) and we need to split the segment. 
                // last timestamp of current segment == baseline of next segment

                // it's a rare edge case so we move over the current segment again and add values
                // until encountering a value which its timestamp == segment last timestamp
                // from that point, we flush the segment and add the leftovers to a new segment

                var originalBaseline = BaselineDate;
                var valuesLength = timeSeriesValuesSegment.NumberOfValues;

                SliceHolder.CreateSegmentBuffer();
                var newValueSegment = new TimeSeriesValuesSegment(SliceHolder.SegmentBuffer.Ptr, MaxSegmentSize);
                newValueSegment.Initialize(valuesLength);

                using (_context.Allocator.Allocate(valuesLength * sizeof(double), out var valuesBuffer))
                using (_context.Allocator.Allocate(timeSeriesValuesSegment.NumberOfValues * sizeof(TimestampState), out var stateBuffer))
                {
                    var localValues = new Span<double>(valuesBuffer.Ptr, timeSeriesValuesSegment.NumberOfValues);
                    var localState = new Span<TimestampState>(stateBuffer.Ptr, timeSeriesValuesSegment.NumberOfValues);
                    var localTag = new TimeSeriesValuesSegment.TagPointer();

                    using (var enumerator = timeSeriesValuesSegment.GetEnumerator(_context.Allocator))
                    {
                        while (enumerator.MoveNext(out var localTimestamp, localValues, localState, ref localTag, out var localStatus))
                        {
                            var currentLocalTime = originalBaseline.AddMilliseconds(localTimestamp);

                            if (currentLocalTime < trimTimestamp)
                            {
                                newValueSegment.Append(_context.Allocator, localTimestamp, localValues, localTag.AsSpan(), localStatus);
                                continue;
                            }

                            AppendExistingSegment(newValueSegment);
                            newValueSegment.Initialize(localValues.Length);
                            newValueSegment.Append(_context.Allocator, 0, localValues, localTag.AsSpan(), localStatus);
                            UpdateBaseline(localTimestamp);

                            while (enumerator.MoveNext(out localTimestamp, localValues, localState, ref localTag, out localStatus))
                            {
                                currentLocalTime = originalBaseline.AddMilliseconds(localTimestamp);
                                var timestampDiff = ((currentLocalTime - BaselineDate).Ticks / 10_000);
                                newValueSegment.Append(_context.Allocator, (int)timestampDiff, localValues, localTag.AsSpan(), localStatus);
                            }
                        }
                    }
                }

                timeSeriesValuesSegment = newValueSegment;
            }

            public void Dispose()
            {
                SliceHolder.Dispose();

                if (_clonedReadonlySegment != null)
                {
                    _context.ReturnMemory(_clonedReadonlySegment);
                }
            }
        }

        public string AppendTimestamp(
            DocumentsOperationContext context,
            string documentId,
            string collection,
            string name,
            IEnumerable<TimeSeriesOperation.AppendOperation> toAppend,
            ChangeVector changeVectorFromReplication = null)
        {
            if (TimeSeriesHandlerProcessorForGetTimeSeries.CheckIfIncrementalTs(name))
                throw new InvalidOperationException("Cannot perform append operations on Incremental Time Series");

            var holder = new SingleResult();

            var options = new AppendOptions
            {
                ChangeVectorFromReplication = changeVectorFromReplication
            };

            return AppendTimestamp(context, documentId, collection, name, toAppend.Select(ToResult), options);

            SingleResult ToResult(TimeSeriesOperation.AppendOperation element)
            {
                holder.Values = element.Values;
                holder.Tag = context.GetLazyString(element.Tag);
                holder.Timestamp = element.Timestamp;
                holder.Status = TimeSeriesValuesSegment.Live;
                return holder;
            }
        }

        private Dictionary<string, string[]> _incrementalPrefixByDbId;
        private static readonly byte[] TimedCounterPrefixBuffer = Encoding.UTF8.GetBytes(TimedCounterPrefix);
        private static readonly byte[] IncrementPrefixBuffer = Encoding.UTF8.GetBytes(IncrementPrefix);
        private const string TimedCounterPrefix = "TC:";
        private const string IncrementPrefix = TimedCounterPrefix + "INC-";
        private const string DecrementPrefix = TimedCounterPrefix + "DEC-";

        [ThreadStatic]
        private static double[] _tempHolder;

        public string IncrementTimestamp(
            DocumentsOperationContext context,
            string documentId,
            string collection,
            string name,
            IEnumerable<TimeSeriesOperation.IncrementOperation> toIncrement,
            AppendOptions options = null)
        {
            if (TimeSeriesHandlerProcessorForGetTimeSeries.CheckIfIncrementalTs(name) == false)
                throw new InvalidOperationException("Cannot perform increment operations on Non Incremental Time Series");

            _incrementalPrefixByDbId ??= new Dictionary<string, string[]>();
            DateTime prevTimestamp = DateTime.MinValue;

            var holder = new SingleResult();

            return AppendTimestamp(context, documentId, collection, name, toIncrement.SelectMany(ToResult), options);


            IEnumerable<SingleResult> ToResult(TimeSeriesOperation.IncrementOperation element)
            {
                ValidateTimestamp(prevTimestamp, element.Timestamp);
                prevTimestamp = element.Timestamp;
                var valuesLength = element.ValuesLength ?? element.Values.Length;

                holder.Timestamp = element.Timestamp.EnsureUtc().EnsureMilliseconds();
                holder.Status = TimeSeriesValuesSegment.Live;
                holder.Values = element.Values;
                if (element.ValuesLength != null)
                    holder.Values = holder.Values.Slice(0, (int)element.ValuesLength);

                var firstPositive = element.Values[0] >= 0;
                holder.Tag = TryGetTimedCounterTag(context, _documentDatabase.DbBase64Id, firstPositive);

                for (int i = 1; i < valuesLength; i++)
                {
                    bool currentPositive = element.Values[i] >= 0;
                    if (firstPositive == currentPositive)
                        continue;

                    foreach (var singleResult in RareMixedPositiveAndNegativeValues(context, element, valuesLength, holder))
                    {
                        yield return singleResult;
                    }

                    yield break;
                }
                yield return holder;
            }
        }

        private IEnumerable<SingleResult> RareMixedPositiveAndNegativeValues(DocumentsOperationContext context, TimeSeriesOperation.IncrementOperation element, int valuesLength, SingleResult holder)
        {
            _tempHolder ??= new double[32];
            Array.Copy(element.Values, _tempHolder, valuesLength);

            holder.Tag = TryGetTimedCounterTag(context, _documentDatabase.DbBase64Id, positive: false);

            for (int j = 0; j < valuesLength; j++)
            {
                if (element.Values[j] >= 0)
                {
                    element.Values[j] = 0;
                }
            }

            yield return holder;

            holder.Tag = TryGetTimedCounterTag(context, _documentDatabase.DbBase64Id, positive: true);
            Array.Copy(_tempHolder, element.Values, valuesLength);

            // to avoid replication loop we will return new increment operation just in case we have non zero values
            var nonZeroValues = 0;
            for (int j = 0; j < valuesLength; j++)
            {
                if (element.Values[j] <= 0)
                {
                    element.Values[j] = 0;
                }
                else
                {
                    nonZeroValues++;
                }
            }
            if (nonZeroValues > 0)
                yield return holder;
        }

        private static void ValidateTimestamp(DateTime prevTimestamp, DateTime elementTimestamp)
        {
            if (elementTimestamp < prevTimestamp)
            {
                throw new InvalidDataException(
                    $"The order of increment operations must be sequential, but got previous operation {prevTimestamp} and the current: {elementTimestamp}");
            }
        }

        private LazyStringValue TryGetTimedCounterTag(JsonOperationContext context, string dbId, bool positive)
        {
            if (_incrementalPrefixByDbId.TryGetValue(dbId, out var values) == false)
                _incrementalPrefixByDbId[dbId] = values = new[] { IncrementPrefix + dbId, DecrementPrefix + dbId };

            var value = positive ? values[0] : values[1];

            return context.GetLazyString(value);
        }

        private sealed class IncrementalEnumerator : AppendEnumerator
        {
            public IncrementalEnumerator(string documentId, string name, IEnumerable<SingleResult> toAppend, bool fromReplication) : base(documentId, name, toAppend, fromReplication)
            {
            }

            private readonly int _tagLength = IncrementPrefixBuffer.Length + 22; // TC:XXX-dbid


            private bool MoveNextFromReplication()
            {
                if (ToAppend.MoveNext() == false)
                {
                    if (_current != null)
                        Last = _current.Timestamp;

                    _current = null;
                    return false;
                }

                var time = EnsureMillisecondsPrecision(ToAppend.Current!.Timestamp);
                if (_current == null)
                    First = time;

                _current = ToAppend.Current;
                _current!.Timestamp = time;
                AssertResult(_current);
                return true;
            }

            public override bool MoveNext()
            {
                if (FromReplication)
                    return MoveNextFromReplication();

                return MoveNextWithTagMerging();
            }

            private SingleResult _next;

            private bool MoveNextWithTagMerging()
            {
                if (_next == null)
                {
                    if (ToAppend.MoveNext() == false)
                    {
                        if (_current != null)
                            Last = _current.Timestamp;

                        _current = null;
                        return false;
                    }

                    var time = EnsureMillisecondsPrecision(ToAppend.Current!.Timestamp);
                    if (_current == null)
                        First = time;

                    _current ??= new SingleResult();

                    ToAppend.Current.CopyTo(_current);
                    _current!.Timestamp = time;
                    AssertResult(_current);
                }
                else
                {
                    _current = _next;
                    _next = null;
                }

                while (true)
                {
                    if (ToAppend.MoveNext() == false)
                    {
                        _next = null;
                        break;
                    }

                    _next ??= new SingleResult();

                    ToAppend.Current.CopyTo(_next);
                    _next!.Timestamp = EnsureMillisecondsPrecision(_next.Timestamp);

                    AssertResult(_next);

                    if (_current.Timestamp > _next.Timestamp)
                        throw new InvalidDataException($"The entries of '{Name}' incremental time-series for document '{DocumentId}' must be sorted by their timestamps. " +
                                                       $"Got: current '{_current.Timestamp:O}'.");

                    if (_current.Timestamp == _next.Timestamp)
                    {
                        var tagCompare = _current.Tag.CompareTo(_next.Tag);
                        if (tagCompare > 0)
                            throw new InvalidDataException(
                                $"Entries to append are out of tag order for '{Name}' time-series for document '{DocumentId}' at '{_current.Timestamp:O}' (current:{_current.Tag}, next:{_next.Tag})");

                        // note that we can have a situation like Inc(1,-1) followed by Inc(1,1) at the same timestamp
                        // which will cause out of order that we don't what to handle and will throw
                        if (tagCompare == 0)
                        {

                            if (FromReplication)
                                throw new InvalidDataException(
                                    $"Got an invalid segment from replication for '{Name}' time-series for document '{DocumentId}', segment contain duplicate timestamp and tag '{_current.Tag}' at '{_current.Timestamp:O}'");

                            if (_next.Values.Length != _current.Values.Length)
                                throw new InvalidDataException(
                                    $"Entries for '{Name}' time-series for document '{DocumentId}' with tag '{_current.Tag}' at '{_current.Timestamp:O}' has different number of values ({_next.Values.Length} vs {_current.Values.Length})");

                            for (int i = 0; i < _next.Values.Length; i++)
                            {
                                _current.Values.Span[i] += _next.Values.Span[i];
                            }

                            continue;
                        }
                    }

                    break;
                }

                IteratedValues++;
                return true;
            }

            private void AssertTag(SingleResult next)
            {
                if (FromReplication)
                {
                    if (next.Status == TimeSeriesValuesSegment.Dead)
                        return;
                }

                if (next.Tag?.Length != _tagLength)
                    throw new InvalidDataException($"Tag of '{Name}' time-series for document '{DocumentId}' are illegal (Tag:{next.Tag})");
            }

            private void AssertValues(SingleResult next)
            {
                if (next.Values.Length == 0 && // dead values can have 0 length
                    next.Status == TimeSeriesValuesSegment.Live)
                    throw new InvalidDataException($"The entries of '{Name}' time-series for document '{DocumentId}' must contain at least one value");

                if (FromReplication)
                    return;

                AssertNoNanValue(next);
            }

            private void AssertResult(SingleResult result)
            {
                AssertValues(result);
                AssertTag(result);
            }
        }

        private class AppendEnumerator : IEnumerator<SingleResult>
        {
            protected readonly string DocumentId;
            protected readonly string Name;
            protected readonly bool FromReplication;
            protected readonly IEnumerator<SingleResult> ToAppend;
            protected SingleResult _current;

            public DateTime Last;
            public DateTime First;
            public long IteratedValues;

            public AppendEnumerator(string documentId, string name, IEnumerable<SingleResult> toAppend, bool fromReplication)
            {
                DocumentId = documentId;
                Name = name;
                FromReplication = fromReplication;
                ToAppend = toAppend.GetEnumerator();
            }

            public virtual bool MoveNext()
            {
                var currentTimestamp = _current?.Timestamp;
                if (ToAppend.MoveNext() == false)
                {
                    if (_current != null)
                        Last = _current.Timestamp;

                    _current = null;
                    return false;
                }

                var next = ToAppend.Current;
                next.Timestamp = EnsureMillisecondsPrecision(next.Timestamp);

                if (_current == null)
                    First = next.Timestamp;

                if (currentTimestamp >= next.Timestamp)
                    throw new InvalidDataException(
                        $"The entries of '{Name}' time-series for document '{DocumentId}' must be sorted by their timestamps, and cannot contain duplicate timestamps. " +
                        $"Got: current '{currentTimestamp:O}', next '{next.Timestamp:O}', make sure your measures have at least 1ms interval.");

                if (next.Values.Length == 0 && // dead values can have 0 length
                    next.Status == TimeSeriesValuesSegment.Live)
                    throw new InvalidDataException($"The entries of '{Name}' time-series for document '{DocumentId}' must contain at least one value");

                if (FromReplication == false)
                {
                    AssertNoNanValue(next);
                }

                IteratedValues++;
                _current = next;
                return true;
            }

            public void Reset()
            {
                throw new NotImplementedException();
            }

            object IEnumerator.Current => _current;

            public SingleResult Current => _current;

            public void Dispose()
            {
                ToAppend.Dispose();
            }
        }

        public sealed class AppendOptions
        {
            public ChangeVector ChangeVectorFromReplication = null;
            public bool VerifyName = true;
            public bool AddNewNameToMetadata = true;
            public bool FromSmuggler = false;
        }

        private static readonly AppendOptions DefaultAppendOptions = new AppendOptions();

        public string AppendTimestamp(
            DocumentsOperationContext context,
            string documentId,
            string collection,
            string name,
            IEnumerable<SingleResult> toAppend,
            AppendOptions options = null)
        {
            options ??= DefaultAppendOptions;

            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false); // never hit
            }

            var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            var newSeries = Stats.GetStats(context, documentId, name).Count == 0;
            if (newSeries && options.VerifyName)
            {
                VerifyLegalName(name);
            }

            var appendEnumerator = TimeSeriesHandlerProcessorForGetTimeSeries.CheckIfIncrementalTs(name)
                ? new IncrementalEnumerator(documentId, name, toAppend, options.ChangeVectorFromReplication != null)
                : new AppendEnumerator(documentId, name, toAppend, options.ChangeVectorFromReplication != null);

            using (appendEnumerator)
            {
                while (appendEnumerator.MoveNext())
                {
                    var retry = true;
                    while (retry)
                    {
                        retry = false;
                        var current = appendEnumerator.Current;
                        Debug.Assert(current != null);

                        if (options.ChangeVectorFromReplication == null)
                        {
                            // not from replication
                            AssertNoNanValue(current);
                        }

                        using (var slicer = new TimeSeriesSliceHolder(context, documentId, name, collection).WithBaseline(current.Timestamp))
                        {
                            var segmentHolder = new TimeSeriesSegmentHolder(this, context, slicer, documentId, name, collectionName, options);
                            if (segmentHolder.LoadClosestSegment() == false)
                            {
                                // no matches for this series at all, need to create new segment
                                segmentHolder.AppendToNewSegment(current);
                                break;
                            }

                            if (EnsureNumberOfValues(segmentHolder.ReadOnlySegment.NumberOfValues, ref current))
                            {
                                if (TryAppendToCurrentSegment(context, segmentHolder, appendEnumerator, current, out var newValueFetched))
                                    break;

                                if (newValueFetched)
                                {
                                    retry = true;
                                    continue;
                                }
                            }

                            if (ValueTooFar(segmentHolder, current))
                            {
                                segmentHolder.AppendToNewSegment(appendEnumerator.Current);
                                break;
                            }

                            retry = SplitSegment(context, segmentHolder, appendEnumerator, current);
                        }
                    }
                }

                if (appendEnumerator.IteratedValues > 0)
                {
                    context.Transaction.AddAfterCommitNotification(new TimeSeriesChange
                    {
                        CollectionName = collectionName.Name,
                        ChangeVector = context.LastDatabaseChangeVector,
                        DocumentId = documentId,
                        Name = name,
                        Type = TimeSeriesChangeTypes.Put,
                        From = appendEnumerator.First,
                        To = appendEnumerator.Last
                    });
                }
            }

            if (newSeries && options.AddNewNameToMetadata)
            {
                AddTimeSeriesNameToMetadata(context, documentId, name);
            }

            return context.LastDatabaseChangeVector;
        }

        private static void VerifyLegalName(string name)
        {
            for (int i = 0; i < name.Length; i++)
            {
                if (name[i] == TimeSeriesConfiguration.TimeSeriesRollupSeparator)
                    throw new InvalidOperationException($"Illegal time series name : '{name}'. " +
                                                        $"Time series names cannot contain '{TimeSeriesConfiguration.TimeSeriesRollupSeparator}' character, " +
                                                        "since this character is reserved for time series rollups.");
            }
        }

        private bool ValueTooFar(TimeSeriesSegmentHolder segmentHolder, SingleResult current)
        {
            var deltaInMs = (current.Timestamp.Ticks / 10_000) - segmentHolder.BaselineMilliseconds;
            return deltaInMs >= int.MaxValue;
        }

        private static bool EnsureNumberOfValues(int segmentNumberOfValues, ref SingleResult current)
        {
            if (segmentNumberOfValues > current.Values.Length)
            {
                var updatedValues = new Memory<double>(new double[segmentNumberOfValues]);
                current.Values.CopyTo(updatedValues);

                for (int i = current.Values.Length; i < updatedValues.Length; i++)
                {
                    updatedValues.Span[i] = double.NaN;
                }

                // we create a new instance to avoid having NaN in the case of re-appending
                current = new SingleResult
                {
                    Timestamp = current.Timestamp,
                    Values = updatedValues,
                    Status = current.Status,
                    Tag = current.Tag,
                    Type = current.Type
                };
            }

            return segmentNumberOfValues == current.Values.Length;
        }

        private bool TryAppendToCurrentSegment(DocumentsOperationContext context,
            TimeSeriesSegmentHolder segmentHolder,
            IEnumerator<SingleResult> appendEnumerator,
            SingleResult current,
            out bool newValueFetched)
        {
            var segment = segmentHolder.ReadOnlySegment;
            var slicer = segmentHolder.SliceHolder;

            var lastTimestamp = segment.GetLastTimestamp(segmentHolder.BaselineDate);
            var nextSegmentBaseline = BaselineOfNextSegment(slicer.TimeSeriesKeySlice, slicer.TimeSeriesPrefixSlice, segmentHolder.Table, segmentHolder.BaselineDate) ?? DateTime.MaxValue;

            TimeSeriesValuesSegment newSegment = default;
            newValueFetched = false;
            while (true)
            {
                var canAppend = current.Timestamp > lastTimestamp && segment.NumberOfValues == current.Values.Length;
                var deltaInMs = (current.Timestamp.Ticks / 10_000) - segmentHolder.BaselineMilliseconds;
                var inRange = deltaInMs < int.MaxValue;

                if (canAppend && inRange) // if the range is too big (over 24.85 days, using ms precision), we need a new segment
                {
                    // this is the simplest scenario, we can just add it.
                    if (newValueFetched == false)
                    {
                        segment.CopyTo(slicer.SegmentBuffer.Ptr);
                        newSegment = new TimeSeriesValuesSegment(slicer.SegmentBuffer.Ptr, MaxSegmentSize);
                    }

                    // checking if we run out of space here, in which can we'll create new segment
                    if (newSegment.Append(context.Allocator, (int)deltaInMs, current.Values.Span, slicer.TagAsSpan(current.Tag), current.Status))
                    {
                        _documentDatabase.TimeSeriesPolicyRunner?.MarkForPolicy(context, segmentHolder.SliceHolder, current.Timestamp, current.Status);

                        newValueFetched = true;
                        lastTimestamp = current.Timestamp;
                        if (appendEnumerator.MoveNext() == false)
                        {
                            segmentHolder.AppendExistingSegment(newSegment);
                            return true;
                        }

                        current = appendEnumerator.Current;
                        if (current!.Timestamp < nextSegmentBaseline)
                        {
                            if (EnsureNumberOfValues(newSegment.NumberOfValues, ref current))
                                continue;
                        }

                        canAppend = false;
                    }
                }

                if (newValueFetched)
                {
                    segmentHolder.AppendExistingSegment(newSegment);
                    return false;
                }

                if (canAppend)
                {
                    // either the range is too high to fit in a single segment (~25 days) or the
                    // previous segment is full, we can just create a completely new segment with the
                    // new value
                    segmentHolder.AppendToNewSegment(appendEnumerator.Current);
                    return true;
                }

                return false;
            }
        }

        private bool SplitSegment(
           DocumentsOperationContext context,
            TimeSeriesSegmentHolder timeSeriesSegment,
           IEnumerator<SingleResult> reader,
            SingleResult current)
        {
            // here we have a complex scenario, we need to add it in the middle of the current segment
            // to do that, we have to re-create it from scratch.

            // the first thing to do here it to copy the segment out, because we may be writing it in multiple
            // steps, and move the actual values as we do so

            var originalBaseline = timeSeriesSegment.BaselineDate;
            var slicer = timeSeriesSegment.SliceHolder;
            var nextSegmentBaseline = BaselineOfNextSegment(slicer.TimeSeriesKeySlice, slicer.TimeSeriesPrefixSlice, timeSeriesSegment.Table, originalBaseline) ?? DateTime.MaxValue;

            var segmentToSplit = timeSeriesSegment.ReadOnlySegment;
            var valuesLength = current.Values.Span.Length;
            var additionalValueSize = Math.Max(0, valuesLength - timeSeriesSegment.ReadOnlySegment.NumberOfValues);
            var newNumberOfValues = additionalValueSize + timeSeriesSegment.ReadOnlySegment.NumberOfValues;

            using (context.Allocator.Allocate(segmentToSplit.NumberOfBytes, out var currentSegmentBuffer))
            {
                Memory.Copy(currentSegmentBuffer.Ptr, segmentToSplit.Ptr, segmentToSplit.NumberOfBytes);
                var readOnlySegment = new TimeSeriesValuesSegment(currentSegmentBuffer.Ptr, segmentToSplit.NumberOfBytes);
                var splitSegment = new TimeSeriesValuesSegment(timeSeriesSegment.SliceHolder.SegmentBuffer.Ptr, MaxSegmentSize);
                splitSegment.Initialize(valuesLength);

                using (context.Allocator.Allocate(newNumberOfValues * sizeof(double), out var valuesBuffer))
                using (context.Allocator.Allocate(readOnlySegment.NumberOfValues * sizeof(TimestampState), out var stateBuffer))
                {
                    Memory.Set(valuesBuffer.Ptr, 0, valuesBuffer.Length);
                    Memory.Set(stateBuffer.Ptr, 0, stateBuffer.Length);

                    var currentValues = new Span<double>(valuesBuffer.Ptr, readOnlySegment.NumberOfValues);
                    var state = new Span<TimestampState>(stateBuffer.Ptr, readOnlySegment.NumberOfValues);
                    var currentTag = new TimeSeriesValuesSegment.TagPointer();
                    var updatedValues = new Span<double>(valuesBuffer.Ptr, newNumberOfValues);
                    for (int i = readOnlySegment.NumberOfValues; i < newNumberOfValues; i++)
                    {
                        updatedValues[i] = double.NaN;
                    }

                    bool segmentChanged = false;

                    using (var enumerator = readOnlySegment.GetEnumerator(context.Allocator))
                    {
                        while (enumerator.MoveNext(out var currentTimestamp, currentValues, state, ref currentTag, out var localStatus))
                        {
                            var currentTime = originalBaseline.AddMilliseconds(currentTimestamp);

                            while (true)
                            {
                                var compare = Compare(currentTime, currentValues, currentTag.ContentAsSpan(), localStatus, current, nextSegmentBaseline, timeSeriesSegment);

                                if (compare == CompareResult.Addition)
                                {
                                    ValuesAddition(ref current, currentValues);

                                    compare = CompareResult.Remote;
                                }

                                if (compare.HasFlag(CompareResult.Remote) == false)
                                {
                                    timeSeriesSegment.AddExistingValue(currentTime, updatedValues, currentTag.AsSpan(), ref splitSegment, localStatus);

                                    if (compare.HasFlag(CompareResult.Merge) == false &&
                                        currentTime == current?.Timestamp)
                                    {
                                        reader.MoveNext();
                                        current = reader.Current;
                                    }
                                    break;
                                }

                                Debug.Assert(current != null);

                                segmentChanged = true;

                                if (EnsureNumberOfValues(newNumberOfValues, ref current) == false)
                                {
                                    // the next value to append has a larger number of values.
                                    // we need to append the rest of the open segment and only then we can re-append this value.
                                    timeSeriesSegment.AddNewValue(currentTime, updatedValues, currentTag.AsSpan(), ref splitSegment, localStatus);
                                    while (enumerator.MoveNext(out currentTimestamp, currentValues, state, ref currentTag, out localStatus))
                                    {
                                        currentTime = originalBaseline.AddMilliseconds(currentTimestamp);
                                        timeSeriesSegment.AddNewValue(currentTime, currentValues, currentTag.AsSpan(), ref splitSegment, localStatus);
                                    }

                                    timeSeriesSegment.AppendExistingSegment(splitSegment);
                                    return true;
                                }

                                timeSeriesSegment.AddNewValue(current, ref splitSegment);

                                DateTime previousTimestamp = current.Timestamp;

                                reader.MoveNext();
                                current = reader.Current;

                                if (compare.HasFlag(CompareResult.Merge) == false && currentTime == previousTimestamp)
                                {
                                    break; // the local value was overwritten
                                }
                            }
                        }
                    }

                    var retryAppend = current != null;

                    if (retryAppend &&
                        EnsureNumberOfValues(newNumberOfValues, ref current) &&
                        current.Timestamp < nextSegmentBaseline)
                    {
                        timeSeriesSegment.AddNewValue(current, ref splitSegment);
                        segmentChanged = true;
                        retryAppend = false;
                    }

                    if (segmentChanged)
                        timeSeriesSegment.AppendExistingSegment(splitSegment);
                    else if (timeSeriesSegment.FromReplication)
                        timeSeriesSegment.UpdateSegmentChangeVector(splitSegment);

                    return retryAppend;
                }
            }
        }

        [Flags]
        private enum CompareResult
        {
            Local = 1,
            Equal = 2,
            Remote = 4,
            Addition = 8,
            Merge = 16
        }

        private static CompareResult Compare(DateTime localTime, Span<double> localValues, Span<byte> localTag, ulong localStatus, SingleResult remote, DateTime? nextSegmentBaseline, TimeSeriesSegmentHolder holder)
        {
            if (remote == null)
                return CompareResult.Local;

            if (localTime < remote.Timestamp)
                return CompareResult.Local;

            if (remote.Timestamp >= nextSegmentBaseline)
                return CompareResult.Local;

            if (localTime == remote.Timestamp)
            {
                if (holder.FromReplication && EitherOneIsMarkedAsDead(localStatus, remote, out CompareResult compareResult))
                    return compareResult;

                if (localTag.StartsWith(TimedCounterPrefixBuffer)) // incremental time-series
                {
                    if (remote.Tag == null || remote.Tag.StartsWith(TimedCounterPrefixBuffer) == false)
                        throw new InvalidDataException("Cannot get append operation for Incremental Time Series.");

                    int compareTags = localTag.SequenceCompareTo(remote.Tag.AsSpan());
                    if (compareTags == 0)
                    {
                        if (holder.FromReplication == false && holder.FromSmuggler == false)
                            return CompareResult.Addition;

                        bool isIncrement = localTag.StartsWith(IncrementPrefixBuffer);
                        return isIncrement ? SelectLargestValue(localValues, remote, isIncrement: true) : SelectSmallerValue(localValues, remote);
                    }

                    if (compareTags < 0)
                        return CompareResult.Local | CompareResult.Merge;
                    return CompareResult.Remote | CompareResult.Merge;
                }

                if (holder.FromReplication == false)
                    return CompareResult.Remote; // if not from replication & not incremental time-series, remote value is an update

                if (remote.Tag != null && remote.Tag.StartsWith(TimedCounterPrefixBuffer))
                    throw new InvalidDataException("Cannot get increment operation for Non-Incremental Time Series.");

                return SelectLargestValue(localValues, remote, isIncrement: false);
            }

            return CompareResult.Remote;
        }

        private static bool EitherOneIsMarkedAsDead(ulong localStatus, SingleResult remote, out CompareResult compareResult)
        {
            if (localStatus == TimeSeriesValuesSegment.Dead)
            {
                if (remote.Status == TimeSeriesValuesSegment.Dead)
                {
                    compareResult = CompareResult.Equal;
                    return true;
                }

                compareResult = CompareResult.Local;
                return true;
            }

            if (remote.Status == TimeSeriesValuesSegment.Dead) // deletion wins
            {
                compareResult = CompareResult.Remote;
                return true;
            }

            compareResult = default;
            return false;
        }

        private static CompareResult SelectLargestValue(Span<double> localValues, SingleResult remote, bool isIncrement)
        {
            if (isIncrement && EnsureNumberOfValues(localValues.Length, ref remote) == false)
                return CompareResult.Remote;

            if (localValues.Length != remote.Values.Length)
            {
                // larger number of values wins
                if (localValues.Length > remote.Values.Length)
                    return CompareResult.Local;

                return CompareResult.Remote;
            }

            var compare = localValues.SequenceCompareTo(remote.Values.Span);
            if (compare >= 0)
                return CompareResult.Local;

            return CompareResult.Remote;
        }

        private static CompareResult SelectSmallerValue(Span<double> localValues, SingleResult remote)
        {
            if (EnsureNumberOfValues(localValues.Length, ref remote) == false)
                return CompareResult.Remote;

            if (localValues.Length != remote.Values.Length)
            {
                // smaller number of values wins
                if (localValues.Length < remote.Values.Length)
                    return CompareResult.Local;

                return CompareResult.Remote;
            }

            var compare = localValues.SequenceCompareTo(remote.Values.Span);
            if (compare <= 0)
                return CompareResult.Local;

            return CompareResult.Remote;
        }

        private static void ValuesAddition(ref SingleResult current, Span<double> localValues)
        {
            int length = Math.Min(localValues.Length, current.Values.Length);
            for (int i = 0; i < length; i++)
            {
                if (double.IsNaN(current.Values.Span[i]) == false && double.IsNaN(localValues[i])) // local value == `Nan`, but remote has value
                    continue;

                if (double.IsNaN(current.Values.Span[i]) && double.IsNaN(localValues[i]) == false) // remote value == `Nan`, but local has value
                {
                    current.Values.Span[i] = localValues[i];
                    continue;
                }

                current.Values.Span[i] = current.Values.Span[i] + localValues[i];
            }
        }

        public void ReplaceTimeSeriesNameInMetadata(DocumentsOperationContext ctx, string docId, string oldName, string newName)
        {
            // if the document is in conflict, that's fine
            // we will recreate '@timeseries' in metadata when the conflict is resolved
            var doc = _documentDatabase.DocumentsStorage.Get(ctx, docId, throwOnConflict: false);
            if (doc == null)
                return;

            newName = GetOriginalName(ctx, docId, newName);

            var data = doc.Data;
            if (doc.TryGetMetadata(out var metadata) == false)
                return;

            if (metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray tsNames) == false)
                return;

            if (tsNames == null)
                return;

            if (tsNames.BinarySearch(newName, StringComparison.OrdinalIgnoreCase) < 0)
                return;

            var tsNamesList = new List<string>(tsNames.Length + 1);
            for (var i = 0; i < tsNames.Length; i++)
            {
                var val = tsNames.GetStringByIndex(i);
                if (val == null)
                    continue;
                tsNamesList.Add(val);
            }

            var location = tsNames.BinarySearch(newName, StringComparison.Ordinal);
            if (location < 0)
            {
                tsNamesList.Insert(~location, newName);
            }

            tsNamesList.Remove(oldName);

            metadata.Modifications = new DynamicJsonValue(metadata)
            {
                [Constants.Documents.Metadata.TimeSeries] = tsNamesList
            };

            var flags = doc.Flags.Strip(DocumentFlags.FromClusterTransaction | DocumentFlags.Resolved);
            flags |= DocumentFlags.HasTimeSeries;

            using (data)
            {
                var newDocumentData = ctx.ReadObject(doc.Data, docId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                _documentDatabase.DocumentsStorage.Put(ctx, doc.Id, null, newDocumentData, flags: flags, nonPersistentFlags: NonPersistentDocumentFlags.ByTimeSeriesUpdate);
            }
        }

        public void AddTimeSeriesNameToMetadata(DocumentsOperationContext ctx, string docId, string tsName, NonPersistentDocumentFlags nonPersistentFlags = NonPersistentDocumentFlags.None)
        {
            var tss = _documentDatabase.DocumentsStorage.TimeSeriesStorage;
            if (tss.Stats.GetStats(ctx, docId, tsName).Count == 0)
                return;

            // if the document is in conflict, that's fine
            // we will recreate '@timeseries' in metadata when the conflict is resolved
            var doc = _documentDatabase.DocumentsStorage.Get(ctx, docId, throwOnConflict: false);
            if (doc == null)
                return;

            tsName = GetOriginalName(ctx, docId, tsName);

            var data = doc.Data;
            BlittableJsonReaderArray tsNames = null;
            if (doc.TryGetMetadata(out var metadata))
            {
                metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out tsNames);
            }

            if (tsNames == null)
            {
                if (metadata == null)
                {
                    data.Modifications = new DynamicJsonValue(data)
                    {
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [Constants.Documents.Metadata.TimeSeries] = new[] { tsName }
                        }
                    };
                }
                else
                {
                    metadata.Modifications = new DynamicJsonValue(metadata)
                    {
                        [Constants.Documents.Metadata.TimeSeries] = new[] { tsName }
                    };
                }
            }
            else
            {
                var tsNamesList = new List<string>(tsNames.Length + 1);
                for (var i = 0; i < tsNames.Length; i++)
                {
                    var val = tsNames.GetStringByIndex(i);
                    if (val == null)
                        continue;
                    tsNamesList.Add(val);
                }

                var location = tsNames.BinarySearch(tsName, StringComparison.OrdinalIgnoreCase);
                if (location >= 0)
                    return;

                tsNamesList.Insert(~location, tsName);

                metadata.Modifications = new DynamicJsonValue(metadata)
                {
                    [Constants.Documents.Metadata.TimeSeries] = tsNamesList
                };
            }

            var flags = doc.Flags.Strip(DocumentFlags.FromClusterTransaction | DocumentFlags.Resolved);
            flags |= DocumentFlags.HasTimeSeries;

            using (data)
            {
                var newDocumentData = ctx.ReadObject(doc.Data, docId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                _documentDatabase.DocumentsStorage.Put(ctx, doc.Id, null, newDocumentData, flags: flags, nonPersistentFlags: nonPersistentFlags |= NonPersistentDocumentFlags.ByTimeSeriesUpdate);
            }
        }

        public string GetOriginalName(DocumentsOperationContext ctx, string docId, string tsName)
        {
            try
            {
                return GetOriginalNameInternal(ctx, docId, tsName);
            }
            catch (Exception e)
            {
                var error = $"Unable to locate the original time-series '{tsName}' of document '{docId}'";
                if (_logger.IsInfoEnabled)
                    _logger.Info(error, e);
            }

            return tsName;
        }

        private string GetOriginalNameInternal(DocumentsOperationContext context, string docId, string lowerName)
        {
            var name = Stats.GetTimeSeriesNameOriginalCasing(context, docId, lowerName);
            if (name == null)
                throw new InvalidOperationException($"Can't find the time-series '{lowerName}' of document '{docId}'");

            return name;
        }

        public IEnumerable<TimeSeriesReplicationItem> GetSegmentsFrom(DocumentsOperationContext context, long etag)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(TimeSeriesSchema.FixedSizeIndexes[AllTimeSeriesEtagSlice], etag, 0))
            {
                yield return CreateTimeSeriesSegmentItem(context, ref result.Reader);
            }
        }

        internal TimeSeriesReplicationItem CreateTimeSeriesSegmentItem(DocumentsOperationContext context, ref TableValueReader reader)
        {
            var etag = *(long*)reader.Read((int)TimeSeriesTable.Etag, out _);
            var changeVectorPtr = reader.Read((int)TimeSeriesTable.ChangeVector, out int changeVectorSize);
            var segmentPtr = reader.Read((int)TimeSeriesTable.Segment, out int segmentSize);

            var item = new TimeSeriesReplicationItem
            {
                Type = ReplicationBatchItem.ReplicationItemType.TimeSeriesSegment,
                ChangeVector = Encoding.UTF8.GetString(changeVectorPtr, changeVectorSize),
                Segment = new TimeSeriesValuesSegment(segmentPtr, segmentSize),
                Collection = DocumentsStorage.TableValueToId(context, (int)TimeSeriesTable.Collection, ref reader),
                Etag = Bits.SwapBytes(etag),
                TransactionMarker = DocumentsStorage.TableValueToShort((int)TimeSeriesTable.TransactionMarker, nameof(TimeSeriesTable.TransactionMarker), ref reader)
            };

            var keyPtr = reader.Read((int)TimeSeriesTable.TimeSeriesKey, out int keySize);
            item.ToDispose(Slice.From(context.Allocator, keyPtr, keySize, ByteStringType.Immutable, out item.Key));

            using (TimeSeriesStats.ExtractStatsKeyFromStorageKey(context, item.Key, out var statsKey))
            {
                item.Name = Stats.GetTimeSeriesNameOriginalCasing(context, statsKey);

                if (item.Name == null)
                {
                    // RavenDB-18381 - replace Null in lower-case name to recover from an existing state of broken replication
                    TimeSeriesValuesSegment.ParseTimeSeriesKey(item.Key, context, out _, out item.Name);
                }
            }

            return item;
        }

        internal LazyStringValue GetTimeSeriesNameOriginalCasing(DocumentsOperationContext context, string documentId, string name)
        {
            using (var slicer = new TimeSeriesSliceHolder(context, documentId, name))
            {
                return Stats.GetTimeSeriesNameOriginalCasing(context, slicer.StatsKey);
            }
        }

        public IEnumerable<TimeSeriesDeletedRangeItem> GetDeletedRangesFrom(DocumentsOperationContext context, long etag)
        {
            var table = new Table(DeleteRangesSchema, context.Transaction.InnerTransaction);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DeleteRangesSchema.FixedSizeIndexes[AllDeletedRangesEtagSlice], etag, 0))
            {
                yield return CreateDeletedRangeItem(context, ref result.Reader);
            }
        }

        public IEnumerable<TimeSeriesDeletedRangeItem> GetDeletedRangesFrom(DocumentsOperationContext context, string collection, long fromEtag)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                yield break;
            var table = GetOrCreateDeleteRangesTable(context.Transaction.InnerTransaction, collectionName);
            if (table == null)
                yield break;

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var result in table.SeekForwardFrom(DeleteRangesSchema.FixedSizeIndexes[CollectionDeletedRangesEtagsSlice], fromEtag, 0))
            {
                yield return CreateDeletedRangeItem(context, ref result.Reader);
            }
        }

        public IEnumerable<TimeSeriesDeletedRangeItem> GetDeletedRangesForDoc(DocumentsOperationContext context, string docId)
        {
            var table = new Table(DeleteRangesSchema, context.Transaction.InnerTransaction);
            using var dispose = DocumentIdWorker.GetSliceFromId(context, docId, out var documentKeyPrefix, SpecialChars.RecordSeparator);
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach ((_, Table.TableValueHolder tvh) in table.SeekByPrimaryKeyPrefix(documentKeyPrefix, Slices.Empty, 0))
            {
                var item = CreateDeletedRangeItem(context, ref tvh.Reader);
                yield return item;
            }
        }

        private static ChangeVector ExtractDeletedRangeChangeVector(DocumentsOperationContext context, ref TableValueReader reader)
        {
            var changeVectorPtr = reader.Read((int)DeletedRangeTable.ChangeVector, out int changeVectorSize);
            return context.GetChangeVector(Encoding.UTF8.GetString(changeVectorPtr, changeVectorSize));
        }

        private static TimeSeriesDeletedRangeItem CreateDeletedRangeItem(DocumentsOperationContext context, ref TableValueReader reader)
        {
            var etag = *(long*)reader.Read((int)DeletedRangeTable.Etag, out _);
            var changeVectorPtr = reader.Read((int)DeletedRangeTable.ChangeVector, out int changeVectorSize);

            var item = new TimeSeriesDeletedRangeItem
            {
                Type = ReplicationBatchItem.ReplicationItemType.DeletedTimeSeriesRange,
                ChangeVector = Encoding.UTF8.GetString(changeVectorPtr, changeVectorSize),
                Collection = DocumentsStorage.TableValueToId(context, (int)DeletedRangeTable.Collection, ref reader),
                Etag = Bits.SwapBytes(etag),
                TransactionMarker = DocumentsStorage.TableValueToShort((int)DeletedRangeTable.TransactionMarker, nameof(DeletedRangeTable.TransactionMarker), ref reader),
                From = DocumentsStorage.TableValueToDateTime((int)DeletedRangeTable.From, ref reader),
                To = DocumentsStorage.TableValueToDateTime((int)DeletedRangeTable.To, ref reader),
            };

            var keyPtr = reader.Read((int)DeletedRangeTable.RangeKey, out int keySize);
            item.ToDispose(Slice.From(context.Allocator, keyPtr, keySize - sizeof(long), ByteStringType.Immutable, out item.Key));

            return item;
        }

        public TimeSeriesSegmentEntry GetTimeSeries(DocumentsOperationContext context, Slice key, TimeSeriesSegmentEntryFields fields = TimeSeriesSegmentEntryFields.All)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);

            if (table.ReadByKey(key, out var reader) == false)
                return null;

            return CreateTimeSeriesItem(context, ref reader, fields);
        }

        public TimeSeriesSegmentEntry GetTimeSeries(DocumentsOperationContext context, long etag, TimeSeriesSegmentEntryFields fields = TimeSeriesSegmentEntryFields.All)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);
            var index = TimeSeriesSchema.FixedSizeIndexes[AllTimeSeriesEtagSlice];

            if (table.Read(context.Allocator, index, etag, out var tvr) == false)
                return null;

            return CreateTimeSeriesItem(context, ref tvr, fields);
        }

        public TimeSeriesDeletedRangeEntry GetTimeSeriesDeletedRange(DocumentsOperationContext context, long etag)
        {
            var table = new Table(DeleteRangesSchema, context.Transaction.InnerTransaction);
            var index = DeleteRangesSchema.FixedSizeIndexes[AllDeletedRangesEtagSlice];

            if (table.Read(context.Allocator, index, etag, out var tvr) == false)
                return null;

            return CreateTimeSeriesDeletedRangeItem(context, ref tvr);

            static TimeSeriesDeletedRangeEntry CreateTimeSeriesDeletedRangeItem(DocumentsOperationContext context, ref TableValueReader reader)
            {
                var etag = *(long*)reader.Read((int)DeletedRangeTable.Etag, out _);
                var keyPtr = reader.Read((int)DeletedRangeTable.RangeKey, out int keySize);

                TimeSeriesValuesSegment.ParseTimeSeriesKey(keyPtr, keySize, context, out var docId, out var name);

                return new TimeSeriesDeletedRangeEntry
                {
                    Key = new LazyStringValue(null, keyPtr, keySize, context),
                    DocId = docId,
                    Name = name,
                    Etag = Bits.SwapBytes(etag)
                };
            }
        }

        public IEnumerable<TimeSeriesSegmentEntry> GetTimeSeriesFrom(DocumentsOperationContext context, long etag, long take, TimeSeriesSegmentEntryFields fields = TimeSeriesSegmentEntryFields.All) =>
            GetTimeSeries(context, etag, long.MaxValue, take, fields);

        private IEnumerable<TimeSeriesSegmentEntry> GetTimeSeries(DocumentsOperationContext context, long fromEtag, long toEtag, long take = long.MaxValue, TimeSeriesSegmentEntryFields fields = TimeSeriesSegmentEntryFields.All)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);

            foreach (var result in table.SeekForwardFrom(TimeSeriesSchema.FixedSizeIndexes[AllTimeSeriesEtagSlice], fromEtag, 0))
            {
                if (take-- <= 0)
                    yield break;

                var item = CreateTimeSeriesItem(context, ref result.Reader, fields);
                if (item.Etag > toEtag)
                    yield break;

                yield return item;
            }
        }

        public IEnumerable<TombstoneIndexItem> GetTimeSeriesDeletedRangeIndexItemsFrom(DocumentsOperationContext context, long etag, long take = long.MaxValue) =>
            GetTimeSeriesDeletedRangeIndexItems(context, etag, long.MaxValue, take);

        private IEnumerable<TombstoneIndexItem> GetTimeSeriesDeletedRangeIndexItems(DocumentsOperationContext context, long fromEtag, long toEtag, long take = long.MaxValue)
        {
            var table = new Table(DeleteRangesSchema, context.Transaction.InnerTransaction);

            foreach (var result in table.SeekForwardFrom(DeleteRangesSchema.FixedSizeIndexes[AllDeletedRangesEtagSlice], fromEtag, 0))
            {
                if (take-- <= 0)
                    yield break;

                var item = CreateTimeSeriesDeletedRangeIndexItem(context, ref result.Reader);
                if (item.Etag > toEtag)
                    yield break;

                yield return item;
            }
        }

        internal static TombstoneIndexItem CreateTimeSeriesDeletedRangeIndexItem(DocumentsOperationContext context, ref TableValueReader reader)
        {
            var etag = *(long*)reader.Read((int)DeletedRangeTable.Etag, out _);
            var keyPtr = reader.Read((int)DeletedRangeTable.RangeKey, out int keySize);

            TimeSeriesValuesSegment.ParseTimeSeriesKey(keyPtr, keySize, context, out var docId, out var name);

            return new TombstoneIndexItem
            {
                PrefixKey = GetTimeSeriesDeletedRangePrefixKey(context, docId, name),
                LowerId = docId,
                Name = name,
                Etag = Bits.SwapBytes(etag),
                Type = IndexItemType.TimeSeries,
                From = DocumentsStorage.TableValueToDateTime((int)DeletedRangeTable.From, ref reader),
                To = DocumentsStorage.TableValueToDateTime((int)DeletedRangeTable.To, ref reader)
            };
        }

        public IEnumerable<TimeSeriesSegmentEntry> GetTimeSeriesFrom(DocumentsOperationContext context, string collection, long etag, long take, TimeSeriesSegmentEntryFields fields = TimeSeriesSegmentEntryFields.All) =>
            GetTimeSeriesFrom(context, collection, etag, long.MaxValue, take, fields);

        private IEnumerable<TimeSeriesSegmentEntry> GetTimeSeriesFrom(DocumentsOperationContext context, string collection, long fromEtag, long toEtag, long take, TimeSeriesSegmentEntryFields fields = TimeSeriesSegmentEntryFields.All)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                yield break;

            var table = GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);

            if (table == null)
                yield break;

            foreach (var result in table.SeekForwardFrom(TimeSeriesSchema.FixedSizeIndexes[CollectionTimeSeriesEtagsSlice], fromEtag, skip: 0))
            {
                if (take-- <= 0)
                    yield break;

                var item = CreateTimeSeriesItem(context, ref result.Reader, fields);
                if (item.Etag > toEtag)
                    yield break;

                yield return item;
            }
        }

        public IEnumerable<TombstoneIndexItem> GetTimeSeriesDeletedRangeIndexItemsFrom(DocumentsOperationContext context, string collection, long etag, long take) =>
            GetTimeSeriesDeletedRangeIndexItems(context, collection, etag, long.MaxValue, take);

        private IEnumerable<TombstoneIndexItem> GetTimeSeriesDeletedRangeIndexItems(DocumentsOperationContext context, string collection, long fromEtag, long toEtag, long take)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                yield break;

            var table = GetOrCreateDeleteRangesTable(context.Transaction.InnerTransaction, collectionName);

            if (table == null)
                yield break;

            foreach (var result in table.SeekForwardFrom(DeleteRangesSchema.FixedSizeIndexes[CollectionDeletedRangesEtagsSlice], fromEtag, skip: 0))
            {
                if (take-- <= 0)
                    yield break;

                var item = CreateTimeSeriesDeletedRangeIndexItem(context, ref result.Reader);
                if (item.Etag > toEtag)
                    yield break;

                yield return item;
            }
        }

        internal static TimeSeriesSegmentEntry CreateTimeSeriesItem(JsonOperationContext context, ref TableValueReader reader, TimeSeriesSegmentEntryFields fields = TimeSeriesSegmentEntryFields.All)
        {
            if (fields.Contain(TimeSeriesSegmentEntryFields.All))
            {
                var etag = *(long*)reader.Read((int)TimeSeriesTable.Etag, out _);
                var changeVectorPtr = reader.Read((int)TimeSeriesTable.ChangeVector, out int changeVectorSize);
                var segmentPtr = reader.Read((int)TimeSeriesTable.Segment, out int segmentSize);
                var keyPtr = reader.Read((int)TimeSeriesTable.TimeSeriesKey, out int keySize);

                TimeSeriesValuesSegment.ParseTimeSeriesKey(keyPtr, keySize, context, out var docId, out var lowerName, out var baseline);

                var luceneKey = ToLuceneKey(context, docId, lowerName, baseline);

                return new TimeSeriesSegmentEntry
                {
                    Key = new LazyStringValue(null, keyPtr, keySize, context),
                    LuceneKey = luceneKey,
                    DocId = docId,
                    Name = lowerName,
                    ChangeVector = Encoding.UTF8.GetString(changeVectorPtr, changeVectorSize),
                    Segment = new TimeSeriesValuesSegment(segmentPtr, segmentSize),
                    SegmentSize = segmentSize,
                    Collection = DocumentsStorage.TableValueToId(context, (int)TimeSeriesTable.Collection, ref reader),
                    Start = baseline,
                    Etag = Bits.SwapBytes(etag),
                };
            }

            return CreateTimeSeriesItemPartial(context, ref reader, fields);

            static TimeSeriesSegmentEntry CreateTimeSeriesItemPartial(JsonOperationContext context, ref TableValueReader reader, TimeSeriesSegmentEntryFields fields)
            {
                var result = new TimeSeriesSegmentEntry();

                if (fields.Contain(TimeSeriesSegmentEntryFields.Key))
                {
                    var keyPtr = reader.Read((int)TimeSeriesTable.TimeSeriesKey, out int keySize);

                    result.Key = new LazyStringValue(null, keyPtr, keySize, context);

                    if (fields.Contain(TimeSeriesSegmentEntryFields.DocIdNameAndStart))
                    {
                        TimeSeriesValuesSegment.ParseTimeSeriesKey(keyPtr, keySize, context, out var docId, out var lowerName, out var baseline);

                        result.DocId = docId;
                        result.Name = lowerName;
                        result.Start = baseline;

                        if (fields.Contain(TimeSeriesSegmentEntryFields.LuceneKey))
                        {
                            result.LuceneKey = ToLuceneKey(context, docId, lowerName, baseline);
                        }
                    }
                }

                if (result.DocId == null && fields.Contain(TimeSeriesSegmentEntryFields.DocIdNameAndStart))
                    throw new InvalidOperationException($"Cannot request '{nameof(TimeSeriesSegmentEntryFields.DocIdNameAndStart)}' fields if '{nameof(TimeSeriesSegmentEntryFields.Key)}' field is not requested");

                if (result.LuceneKey == null && fields.Contain(TimeSeriesSegmentEntryFields.LuceneKey))
                    throw new InvalidOperationException($"Cannot request '{nameof(TimeSeriesSegmentEntryFields.LuceneKey)}' fields if '{nameof(TimeSeriesSegmentEntryFields.DocIdNameAndStart)}' field is not requested");

                if (fields.Contain(TimeSeriesSegmentEntryFields.ChangeVector))
                {
                    var changeVectorPtr = reader.Read((int)TimeSeriesTable.ChangeVector, out int changeVectorSize);
                    result.ChangeVector = Encoding.UTF8.GetString(changeVectorPtr, changeVectorSize);
                }

                if (fields.Contain(TimeSeriesSegmentEntryFields.Segment))
                {
                    var segmentPtr = reader.Read((int)TimeSeriesTable.Segment, out int segmentSize);
                    result.Segment = new TimeSeriesValuesSegment(segmentPtr, segmentSize);
                    result.SegmentSize = segmentSize;
                }

                if (fields.Contain(TimeSeriesSegmentEntryFields.Collection))
                    result.Collection = DocumentsStorage.TableValueToId(context, (int)TimeSeriesTable.Collection, ref reader);

                result.Etag = Bits.SwapBytes(*(long*)reader.Read((int)TimeSeriesTable.Etag, out _));

                return result;
            }

            static LazyStringValue ToLuceneKey(JsonOperationContext context, LazyStringValue documentId, LazyStringValue name, DateTime baseline)
            {
                var size = documentId.Size
                           + 1 // separator
                           + name.Size
                           + 1 // separator
                           + FormatD18.Precision; // baseline.Ticks

                var mem = context.GetMemory(size);
                try
                {
                    var bufferSpan = new Span<byte>(mem.Address, size);
                    documentId.AsSpan().CopyTo(bufferSpan);
                    var offset = documentId.Size;
                    bufferSpan[offset++] = SpecialChars.LuceneRecordSeparator;
                    name.AsSpan().CopyTo(bufferSpan.Slice(offset));
                    offset += name.Size;
                    bufferSpan[offset++] = SpecialChars.LuceneRecordSeparator;

                    if (Utf8Formatter.TryFormat(baseline.Ticks, bufferSpan.Slice(offset), out var bytesWritten, FormatD18) == false || bytesWritten != FormatD18.Precision)
                        throw new InvalidOperationException($"Could not write '{baseline.Ticks}' ticks. Bytes written {bytesWritten}, but expected {FormatD18.Precision}.");

                    return context.GetLazyString(mem.Address, size);
                }
                finally
                {
                    context.ReturnMemory(mem);
                }
            }
        }

        private static LazyStringValue GetTimeSeriesDeletedRangePrefixKey(DocumentsOperationContext context, LazyStringValue documentId, LazyStringValue name)
        {
            var size = documentId.Size
                       + 1 // Lucene separator
                       + name.Size;

            var mem = context.GetMemory(size);

            try
            {
                var bufferSpan = new Span<byte>(mem.Address, size);
                documentId.AsSpan().CopyTo(bufferSpan);
                var offset = documentId.Size;
                bufferSpan[offset++] = SpecialChars.LuceneRecordSeparator;
                name.AsSpan().CopyTo(bufferSpan.Slice(offset));
                return context.GetLazyString(mem.Address, size);
            }
            finally
            {
                context.ReturnMemory(mem);
            }
        }

        private static readonly StandardFormat FormatD18 = new StandardFormat('D', 18);

        internal IEnumerable<SegmentSummary> GetSegmentsSummary(DocumentsOperationContext context, string documentId, string name, DateTime from, DateTime to)
        {
            var reader = GetReader(context, documentId, name, from, to);
            return reader.GetSegmentsSummary();
        }

        internal SeriesSummary GetSeriesSummary(DocumentsOperationContext context, string documentId, string name)
        {
            var reader = GetReader(context, documentId, name, DateTime.MinValue, DateTime.MaxValue);
            return reader.GetSummary();
        }

        private static void ValidateSegment(TimeSeriesValuesSegment segment)
        {
            if (segment.NumberOfBytes > MaxSegmentSize)
                throw new ArgumentOutOfRangeException("Attempted to write a time series segment that is larger (" + segment.NumberOfBytes + ") than the maximum size allowed.");

            if (segment.RecomputeRequired)
                throw new InvalidOperationException("Recomputation of this segment is required.");
        }

        public long GetNumberOfTimeSeriesSegments(DocumentsOperationContext context)
        {
            var fstIndex = TimeSeriesSchema.FixedSizeIndexes[AllTimeSeriesEtagSlice];
            var fst = context.Transaction.InnerTransaction.FixedTreeFor(fstIndex.Name, sizeof(long));
            return fst.NumberOfEntries;
        }

        public static void AssertNoNanValue(SingleResult toAppend)
        {
            foreach (var val in toAppend.Values.Span)
            {
                if (double.IsNaN(val))
                    throw new NanValueException("Failed to append TimeSeries entry. TimeSeries entries cannot have 'double.NaN' as one of their values. " +
                                                $"Failed on Timestamp : '{toAppend.Timestamp.GetDefaultRavenFormat()}', Values : [{string.Join(',', toAppend.Values.ToArray())}]. ");
            }
        }

        private Table GetOrCreateTimeSeriesTable(Transaction tx, CollectionName collection)
        {
            return GetOrCreateTable(tx, TimeSeriesSchema, collection, CollectionTableType.TimeSeries);
        }

        private Table GetOrCreateDeleteRangesTable(Transaction tx, CollectionName collection)
        {
            return GetOrCreateTable(tx, DeleteRangesSchema, collection, CollectionTableType.TimeSeriesDeletedRanges);
        }

        private Table GetOrCreateTable(Transaction tx, TableSchema tableSchema, CollectionName collection, CollectionTableType type)
        {
            string tableName = collection.GetTableName(type);

            if (tx.IsWriteTransaction && _tableCreated.Contains(tableName) == false)
            {
                // RavenDB-11705: It is possible that this will revert if the transaction
                // aborts, so we must record this only after the transaction has been committed
                // note that calling the Create() method multiple times is a noop
                tableSchema.Create(tx, tableName, 16);
                tx.LowLevelTransaction.OnDispose += _ =>
                {
                    if (tx.LowLevelTransaction.Committed == false)
                        return;

                    // not sure if we can _rely_ on the tx write lock here, so let's be safe and create
                    // a new instance, just in case
                    _tableCreated = new HashSet<string>(_tableCreated, StringComparer.OrdinalIgnoreCase)
                    {
                        tableName
                    };
                };
            }

            return tx.OpenTable(tableSchema, tableName);
        }

        public DynamicJsonArray GetTimeSeriesNamesForDocument(DocumentsOperationContext context, string docId)
        {
            return new DynamicJsonArray(Stats.GetTimeSeriesNamesForDocumentOriginalCasing(context, docId));
        }


        public long GetLastTimeSeriesEtag(DocumentsOperationContext context)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);

            var result = table.ReadLast(TimeSeriesSchema.FixedSizeIndexes[AllTimeSeriesEtagSlice]);
            if (result == null)
                return 0;

            return DocumentsStorage.TableValueToEtag((int)TimeSeriesTable.Etag, ref result.Reader);
        }

        public long GetLastTimeSeriesEtag(DocumentsOperationContext context, string collection)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return 0;

            var table = GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);

            // ReSharper disable once UseNullPropagation
            if (table == null)
                return 0;

            var result = table.ReadLast(TimeSeriesSchema.FixedSizeIndexes[CollectionTimeSeriesEtagsSlice]);
            if (result == null)
                return 0;

            return DocumentsStorage.TableValueToEtag((int)TimeSeriesTable.Etag, ref result.Reader);
        }

        public long GetLastTimeSeriesDeletedRangesEtag(DocumentsOperationContext context)
        {
            var table = new Table(DeleteRangesSchema, context.Transaction.InnerTransaction);

            var result = table.ReadLast(DeleteRangesSchema.FixedSizeIndexes[AllDeletedRangesEtagSlice]);
            if (result == null)
                return 0;

            return DocumentsStorage.TableValueToEtag((int)DeletedRangeTable.Etag, ref result.Reader);
        }

        public long GetLastTimeSeriesDeletedRangesEtag(DocumentsOperationContext context, string collection)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                return 0;

            var table = GetOrCreateDeleteRangesTable(context.Transaction.InnerTransaction, collectionName);

            // ReSharper disable once UseNullPropagation
            if (table == null)
                return 0;

            var result = table.ReadLast(DeleteRangesSchema.FixedSizeIndexes[CollectionDeletedRangesEtagsSlice]);
            if (result == null)
                return 0;

            return DocumentsStorage.TableValueToEtag((int)DeletedRangeTable.Etag, ref result.Reader);
        }

        private static void MarkSegmentAsPendingDeletion(DocumentsOperationContext context, string collection, long etag)
        {
            var pendingDeletion = context.Transaction.InnerTransaction.CreateTree(PendingDeletionSegments);
            using (context.Allocator.From(Bits.SwapBytes(etag), out var etagSlice))
            {
                pendingDeletion.MultiAdd(collection, new Slice(etagSlice));
            }
        }

        public long GetNumberOfTimeSeriesSegmentsToProcess(DocumentsOperationContext context, string collection, in long afterEtag, out long totalCount, Stopwatch overallDuration)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
            {
                totalCount = 0;
                return 0;
            }

            var table = GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);

            if (table == null)
            {
                totalCount = 0;
                return 0;
            }

            var indexDef = TimeSeriesSchema.FixedSizeIndexes[CollectionTimeSeriesEtagsSlice];

            return table.GetNumberOfEntriesAfter(indexDef, afterEtag, out totalCount, overallDuration);
        }

        public long GetNumberOfTimeSeriesDeletedRangesToProcess(DocumentsOperationContext context, string collection, in long afterEtag, out long totalCount, Stopwatch overallDuration)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
            {
                totalCount = 0;
                return 0;
            }

            var table = GetOrCreateDeleteRangesTable(context.Transaction.InnerTransaction, collectionName);

            if (table == null)
            {
                totalCount = 0;
                return 0;
            }

            var indexDef = DeleteRangesSchema.FixedSizeIndexes[CollectionDeletedRangesEtagsSlice];

            return table.GetNumberOfEntriesAfter(indexDef, afterEtag, out totalCount, overallDuration);
        }

        [Conditional("DEBUG")]
        private static void EnsureStatsAndDataIntegrity(DocumentsOperationContext context, string docId, string name, TimeSeriesValuesSegment segment, CollectionName collectionName, DateTime baseline)
        {
            if (context.Transaction.InnerTransaction.IsWriteTransaction == false)
                return;

            var tss = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage;
            var stats = tss.Stats.GetStats(context, docId, name);
            var reader = tss.GetReader(context, docId, name, DateTime.MinValue, DateTime.MaxValue);

            if (stats.Count == 0)
            {
                Debug.Assert(stats.Start == default);
                Debug.Assert(stats.End == default);
                Debug.Assert(reader.AllValues().Any() == false);
                return;
            }

            var first = reader.First().Timestamp;
            var last = reader.Last().Timestamp;

            Debug.Assert(first == stats.Start && stats.Start.Kind == DateTimeKind.Utc, $"Failed start check: {first} == {stats.Start}");
            Debug.Assert(last == stats.End, $"Failed end check: {last} == {stats.End}");

            if (segment.NumberOfLiveEntries > 0)
            {
                for (var index = 0; index < segment.SegmentValues.Span.Length; index++)
                {
                    var state = segment.SegmentValues.Span[index];
                    if (state.Count == 0)
                    {
                        // all followed dimensions must be zero as well
                        for (; index < segment.SegmentValues.Span.Length; index++)
                        {
                            state = segment.SegmentValues.Span[index];
                            Debug.Assert(state.Count == 0, "Count not zero");
                            Debug.Assert(double.IsNaN(state.First), "First must be NaN");
                            Debug.Assert(double.IsNaN(state.Min), "Min must be NaN");
                            Debug.Assert(double.IsNaN(state.Max), "Max must be NaN");

                            Debug.Assert(state.Last == 0, "Last not zero");
                            Debug.Assert(state.Sum == 0, "Sum not zero");
                        }
                        break;
                    }

                    Debug.Assert(double.IsNaN(state.First) == false, "First is NaN");
                    Debug.Assert(double.IsNaN(state.Last) == false, "Last is NaN");
                    Debug.Assert(double.IsNaN(state.Min) == false, "Min is NaN");
                    Debug.Assert(double.IsNaN(state.Max) == false, "Max is NaN");
                    Debug.Assert(double.IsNaN(state.Sum) == false, "Sum is NaN");
                    Debug.Assert(double.IsNaN(state.Count) == false, "Count is NaN");
                }

                if (name.Contains(TimeSeriesConfiguration.TimeSeriesRollupSeparator))
                {
                    var noNaN = segment.YieldAllValues(context, baseline: default, includeDead: false).All(x => x.Values.ToArray().All(y => double.IsNaN(y) == false));
                    Debug.Assert(noNaN, "Rollup has NaN");
                }

                using (var slicer = new TimeSeriesSliceHolder(context, docId, name).WithBaseline(baseline))
                {
                    Debug.Assert(tss.EnsureNoOverlap(context, slicer.TimeSeriesKeySlice, collectionName, segment, baseline), "Segment is overlapping another segment");
                }
            }
        }
    }

    public sealed class NanValueException : Exception
    {
        public NanValueException()
        {
        }

        public NanValueException(string message) : base(message)
        {
        }

        public NanValueException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}

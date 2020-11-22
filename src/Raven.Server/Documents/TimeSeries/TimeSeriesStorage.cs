using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Documents.Replication.ReplicationItems;
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
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.TimeSeries
{
    public unsafe class TimeSeriesStorage
    {
        public const int MaxSegmentSize = 2048;

        public static readonly Slice AllTimeSeriesEtagSlice;

        private static readonly Slice CollectionTimeSeriesEtagsSlice;

        private static readonly Slice TimeSeriesKeysSlice;
        private static readonly Slice PendingDeletionSegments;
        private static readonly Slice DeletedRangesKey;
        private static readonly Slice AllDeletedRangesEtagSlice;
        private static readonly Slice CollectionDeletedRangesEtagsSlice;

        internal static readonly TableSchema TimeSeriesSchema = new TableSchema
        {
            TableType = (byte)TableType.TimeSeries
        };

        private static readonly TableSchema DeleteRangesSchema = new TableSchema();

        private readonly DocumentDatabase _documentDatabase;
        private readonly DocumentsStorage _documentsStorage;

        private HashSet<string> _tableCreated = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public readonly TimeSeriesStats Stats;
        public readonly TimeSeriesRollups Rollups;

        static TimeSeriesStorage()
        {
            using (StorageEnvironment.GetStaticContext(out ByteStringContext ctx))
            {
                Slice.From(ctx, "AllTimeSeriesEtag", ByteStringType.Immutable, out AllTimeSeriesEtagSlice);
                Slice.From(ctx, "CollectionTimeSeriesEtags", ByteStringType.Immutable, out CollectionTimeSeriesEtagsSlice);
                Slice.From(ctx, "TimeSeriesKeys", ByteStringType.Immutable, out TimeSeriesKeysSlice);
                Slice.From(ctx, "PendingDeletionSegments", ByteStringType.Immutable, out PendingDeletionSegments);
                Slice.From(ctx, "DeletedRangesKey", ByteStringType.Immutable, out DeletedRangesKey);
                Slice.From(ctx, "AllDeletedRangesEtag", ByteStringType.Immutable, out AllDeletedRangesEtagSlice);
                Slice.From(ctx, "CollectionDeletedRangesEtags", ByteStringType.Immutable, out CollectionDeletedRangesEtagsSlice);
            }

            TimeSeriesSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)TimeSeriesTable.TimeSeriesKey,
                Count = 1,
                Name = TimeSeriesKeysSlice,
                IsGlobal = true
            });

            TimeSeriesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)TimeSeriesTable.Etag,
                Name = AllTimeSeriesEtagSlice,
                IsGlobal = true
            });

            TimeSeriesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)TimeSeriesTable.Etag,
                Name = CollectionTimeSeriesEtagsSlice
            });

            DeleteRangesSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)DeletedRangeTable.RangeKey,
                Count = 1,
                Name = DeletedRangesKey,
                IsGlobal = true
            });

            DeleteRangesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)DeletedRangeTable.Etag,
                Name = AllDeletedRangesEtagSlice,
                IsGlobal = true
            });

            DeleteRangesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = (int)DeletedRangeTable.Etag,
                Name = CollectionDeletedRangesEtagsSlice
            });
        }

        private readonly Logger _logger;

        public TimeSeriesStorage(DocumentDatabase documentDatabase, Transaction tx)
        {
            _documentDatabase = documentDatabase;
            _documentsStorage = documentDatabase.DocumentsStorage;

            tx.CreateTree(TimeSeriesKeysSlice);
            tx.CreateTree(DeletedRangesKey);

            Stats = new TimeSeriesStats(this, tx);
            Rollups = new TimeSeriesRollups(_documentDatabase);
            _logger = LoggingSource.Instance.GetLogger<TimeSeriesStorage>(documentDatabase.Name);
        }

        public static DateTime ExtractDateTimeFromKey(Slice key)
        {
            var span = key.AsSpan();
            var timeSlice = span.Slice(span.Length - sizeof(long), sizeof(long));
            var baseline = Bits.SwapBytes(MemoryMarshal.Read<long>(timeSlice));
            return new DateTime(baseline * 10_000);
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
                    if (table.SeekOnePrimaryKeyWithPrefix(tsKey, Slices.BeforeAllKeys, out _) == false)
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

        public class DeletionRangeRequest
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

        private string InsertDeletedRange(DocumentsOperationContext context, DeletionRangeRequest deletionRangeRequest, string remoteChangeVector = null)
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

            string changeVector;
            long etag;
            if (remoteChangeVector != null)
            {
                changeVector = remoteChangeVector;
                etag = _documentsStorage.GenerateNextEtag();
            }
            else
            {
                (changeVector, etag) = GenerateChangeVector(context);
            }

            using (var sliceHolder = new TimeSeriesSliceHolder(context, documentId, name, collectionName.Name).WithEtag(etag))
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

        public string DeleteTimestampRange(DocumentsOperationContext context, DeletionRangeRequest deletionRangeRequest, string remoteChangeVector = null, bool updateMetadata = true)
        {
            deletionRangeRequest.From = EnsureMillisecondsPrecision(deletionRangeRequest.From);
            deletionRangeRequest.To = EnsureMillisecondsPrecision(deletionRangeRequest.To);

            InsertDeletedRange(context, deletionRangeRequest, remoteChangeVector);

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

                slicer.SetBaselineToKey(stats.Start > from ? stats.Start : from);

                // first try to find the previous segment containing from value
                if (table.SeekOneBackwardByPrimaryKeyPrefix(slicer.TimeSeriesPrefixSlice, slicer.TimeSeriesKeySlice, out var segmentValueReader) == false)
                {
                    // or the first segment _after_ the from value
                    if (table.SeekOnePrimaryKeyWithPrefix(slicer.TimeSeriesPrefixSlice, slicer.TimeSeriesKeySlice, out segmentValueReader) == false)
                        return null;
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

                    using (var holder = new TimeSeriesSegmentHolder(this, context, documentId, name, collectionName, baseline))
                    {
                        if (holder.LoadCurrentSegment() == false)
                            return false;

                        // we need to get the next segment before the actual remove, since it might lead to a split
                        next = TryGetNextBaseline();

                        var readOnlySegment = holder.ReadOnlySegment;
                        var end = readOnlySegment.GetLastTimestamp(baseline);

                        if (baseline > end)
                            return false;

                        if (remoteChangeVector != null)
                        {
                            if (ChangeVectorUtils.GetConflictStatus(remoteChangeVector, holder.ReadOnlyChangeVector) == ConflictStatus.AlreadyMerged)
                            {
                                // the deleted range is older than this segment, so we don't touch this segment
                                return false;
                            }
                        }

                        if (readOnlySegment.NumberOfLiveEntries == 0)
                            return true; // nothing to delete here

                        var newSegment = new TimeSeriesValuesSegment(holder.SliceHolder.SegmentBuffer.Ptr, MaxSegmentSize);

                        var canDeleteEntireSegment = baseline >= from && end <= to; // entire segment can be deleted
                        var numberOfValues = canDeleteEntireSegment ? 0 : readOnlySegment.NumberOfValues;

                        newSegment.Initialize(numberOfValues);

                        // TODO: RavenDB-14851 manipulating segments as a whole must be done carefully in a distributed env
                        // if (baseline >= from && end <= to) // entire segment can be deleted
                        // {
                        //     // update properly the start/end of the time-series
                        //     holder.AddNewValue(baseline, new double[readOnlySegment.NumberOfValues], Slices.Empty.AsSpan(), ref newSegment, TimeSeriesValuesSegment.Dead);
                        //     holder.AddNewValue(readOnlySegment.GetLastTimestamp(baseline), new double[readOnlySegment.NumberOfValues], Slices.Empty.AsSpan(), ref newSegment, TimeSeriesValuesSegment.Dead);
                        //     holder.AppendDeadSegment(newSegment);
                        //
                        //     removeOccurred = true;
                        //     return true;
                        // }

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

                        return end < to;
                    }
                }

                DateTime? TryGetNextBaseline()
                {
                    var offset = slicer.TimeSeriesKeySlice.Size - sizeof(long);
                    *(long*)(slicer.TimeSeriesKeySlice.Content.Ptr + offset) = Bits.SwapBytes(baseline.Ticks / 10_000);

                    foreach (var (_, tvh) in table.SeekByPrimaryKeyPrefix(slicer.TimeSeriesPrefixSlice, slicer.TimeSeriesKeySlice, 0))
                    {
                        return GetBaseline(tvh.Reader);
                    }

                    return null;
                }
            }
        }

        private static DateTime GetBaseline(TableValueReader reader)
        {
            var key = reader.Read((int)TimeSeriesTable.TimeSeriesKey, out int keySize);
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

            var doc = storage.Get(ctx, docId, throwOnConflict: false);
            if (doc == null)
                return;

            var flags = doc.Flags;
            var newData = ModifyDocumentMetadata(ctx, docId, namesToAdd: null, 
                namesToRemove: new HashSet<string>(StringComparer.OrdinalIgnoreCase){ tsName }, 
                doc.Data, ref flags);

            if (newData == null)
                return;

            storage.Put(ctx, docId, null, newData, flags: flags,
                nonPersistentFlags: NonPersistentDocumentFlags.ByTimeSeriesUpdate);
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

        public void DeleteAllTimeSeriesForDocument(DocumentsOperationContext context, string documentId, CollectionName collection)
        {
            // this will be called as part of document's delete

            var table = GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collection);
            using (DocumentIdWorker.GetSliceFromId(context, documentId, out Slice documentKeyPrefix, SpecialChars.RecordSeparator))
            {
                table.DeleteByPrimaryKeyPrefix(documentKeyPrefix);
                Stats.DeleteByPrimaryKeyPrefix(context, collection, documentKeyPrefix);
                Rollups.DeleteByPrimaryKeyPrefix(context, documentKeyPrefix);
            }
        }

        public void DeleteTimeSeriesForDocument(DocumentsOperationContext context, string documentId, CollectionName collection, string name)
        {
            var table = GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collection);
            using (var slicer = new TimeSeriesSliceHolder(context, documentId, name, collection.Name))
            {
                table.DeleteByPrimaryKeyPrefix(slicer.TimeSeriesPrefixSlice);
                Stats.DeleteStats(context, collection, slicer.StatsKey);
                Rollups.DeleteByPrimaryKeyPrefix(context, slicer.StatsKey);
                RemoveTimeSeriesNameFromMetadata(context, slicer.DocId, slicer.Name);
            }
        }

        public TimeSeriesReader GetReader(DocumentsOperationContext context, string documentId, string name, DateTime from, DateTime to, TimeSpan? offset = null)
        {
            return new TimeSeriesReader(context, documentId, name, from, to, offset);
        }

        public bool TryAppendEntireSegment(DocumentsOperationContext context, TimeSeriesReplicationItem item, string docId, LazyStringValue name, DateTime baseline)
        {
            var collectionName = _documentsStorage.ExtractCollectionName(context, item.Collection);
            return TryAppendEntireSegment(context, item.Key, docId, name, collectionName, item.ChangeVector, item.Segment, baseline);
        }

        private bool TryAppendEntireSegment(
            DocumentsOperationContext context,
            Slice key,
            string documentId,
            string name,
            CollectionName collectionName,
            string changeVector,
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
                    // TODO: RavenDB-14851 currently this is not working as expected, and cause values to disappear
                    // we can put the segment directly only if the incoming segment doesn't overlap with any existing one
                    // using (Slice.From(context.Allocator, key.Content.Ptr, key.Size - sizeof(long), ByteStringType.Immutable, out var prefix))
                    // {
                    //     if (IsOverlapWithHigherSegment(prefix) == false)
                    //     {
                    //         var segmentReadOnlyBuffer = tvr.Read((int)TimeSeriesTable.Segment, out int size);
                    //         var readOnlySegment = new TimeSeriesValuesSegment(segmentReadOnlyBuffer, size);
                    //         Stats.UpdateCountOfExistingStats(context, documentId, name, collectionName, -readOnlySegment.NumberOfLiveEntries);
                    //
                    //         AppendEntireSegment();
                    //
                    //         return true;
                    //     }
                    // }
                }

                return false;
            }

            return TryPutSegmentDirectly(context, key, documentId, name, collectionName, changeVector, segment, baseline);
        }

        public bool TryAppendEntireSegmentFromSmuggler(DocumentsOperationContext context, Slice key, CollectionName collectionName, TimeSeriesItem item)
        {
            // we will generate new change vector, so we pass null here
            return TryPutSegmentDirectly(context, key, item.DocId, context.GetLazyStringForFieldWithCaching(item.Name), collectionName, null, item.Segment, item.Baseline);
        }

        private bool TryPutSegmentDirectly(
            DocumentsOperationContext context,
            Slice key,
            string documentId,
            string name,
            CollectionName collectionName,
            string changeVector,
            TimeSeriesValuesSegment segment,
            DateTime baseline)
        {

            if (IsOverlapping(context, key, collectionName, segment, baseline))
                return false;

            // if this segment isn't overlap with any other we can put it directly
            ValidateSegment(segment);
            var table = GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);

            using (var slicer = new TimeSeriesSliceHolder(context, documentId, name, collectionName.Name))
            {
                Stats.UpdateStats(context, slicer, collectionName, segment, baseline, segment.NumberOfLiveEntries);

                var newEtag = _documentsStorage.GenerateNextEtag();
                changeVector ??= _documentsStorage.GetNewChangeVector(context, newEtag);

                _documentDatabase.TimeSeriesPolicyRunner?.MarkSegmentForPolicy(context, slicer, baseline, changeVector, segment.NumberOfLiveEntries);

                if (segment.NumberOfLiveEntries == 0)
                {
                    MarkSegmentAsPendingDeletion(context, collectionName.Name, newEtag);
                }

                using (Slice.From(context.Allocator, changeVector, out Slice cv))
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(key);
                    tvb.Add(Bits.SwapBytes(newEtag));
                    tvb.Add(cv);
                    tvb.Add(segment.Ptr, segment.NumberOfBytes);
                    tvb.Add(slicer.CollectionSlice);
                    tvb.Add(context.GetTransactionMarker());

                    table.Set(tvb);
                }

                context.Transaction.AddAfterCommitNotification(new TimeSeriesChange
                {
                    CollectionName = collectionName.Name,
                    ChangeVector = changeVector,
                    DocumentId = documentId,
                    Name = name,
                    Type = TimeSeriesChangeTypes.Put,
                    From = DateTime.MinValue,
                    To = DateTime.MaxValue
                });
            }

            EnsureStatsAndDataIntegrity(context, documentId, name, segment);

            return true;
        }

        private bool IsOverlapping(
            DocumentsOperationContext context,
            Slice key,
            CollectionName collectionName,
            TimeSeriesValuesSegment segment,
            DateTime baseline)
        {
            var table = GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);
            using (Slice.From(context.Allocator, key.Content.Ptr, key.Size - sizeof(long), ByteStringType.Immutable, out var prefix))
            {
                return IsOverlapWithHigherSegment(prefix) || IsOverlapWithLowerSegment(prefix);
            }

            bool IsOverlapWithHigherSegment(Slice prefix)
            {
                var lastTimestamp = segment.GetLastTimestamp(baseline);
                var nextSegmentBaseline = BaselineOfNextSegment(table, prefix, key, baseline);
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
                var last = prevSegment.GetLastTimestamp(prevBaseline);
                return last >= baseline;
            }
        }

        private static DateTime? BaselineOfNextSegment(TimeSeriesSegmentHolder segmentHolder, DateTime myDate)
        {
            var table = segmentHolder.Table;
            var prefix = segmentHolder.SliceHolder.TimeSeriesPrefixSlice;
            var key = segmentHolder.SliceHolder.TimeSeriesKeySlice;

            return BaselineOfNextSegment(table, prefix, key, myDate);
        }

        private static DateTime? BaselineOfNextSegment(Table table, Slice prefix, Slice key, DateTime myDate)
        {
            if (table.SeekOnePrimaryKeyWithPrefix(prefix, key, out var reader))
            {
                var currentKey = reader.Read((int)TimeSeriesTable.TimeSeriesKey, out var keySize);
                var baseline = Bits.SwapBytes(
                    *(long*)(currentKey + keySize - sizeof(long))
                );
                var date = new DateTime(baseline * 10_000);
                if (date > myDate)
                    return date;

                foreach (var (_, holder) in table.SeekByPrimaryKeyPrefix(prefix, key, 0))
                {
                    currentKey = holder.Reader.Read((int)TimeSeriesTable.TimeSeriesKey, out keySize);
                    baseline = Bits.SwapBytes(
                        *(long*)(currentKey + keySize - sizeof(long))
                    );
                    return new DateTime(baseline * 10_000);
                }
            }

            return null;
        }

        public class SegmentSummary
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

        public class TimeSeriesSegmentHolder : IDisposable
        {
            private readonly TimeSeriesStorage _tss;
            private readonly DocumentsOperationContext _context;
            public readonly TimeSeriesSliceHolder SliceHolder;
            public readonly bool FromReplication;
            private readonly string _docId;
            private readonly CollectionName _collection;
            private readonly string _name;

            private TableValueReader _tvr;

            public long BaselineMilliseconds => BaselineDate.Ticks / 10_000;
            public DateTime BaselineDate;
            public TimeSeriesValuesSegment ReadOnlySegment;
            public string ReadOnlyChangeVector;

            private long _currentEtag;

            private string _currentChangeVector;
            public string ChangeVector => _currentChangeVector;

            private AllocatedMemoryData _clonedReadonlySegment;

            public TimeSeriesSegmentHolder(
                TimeSeriesStorage tss,
                DocumentsOperationContext context,
                string docId,
                string name,
                CollectionName collection,
                DateTime timeStamp,
                string fromReplicationChangeVector = null
                )
            {
                _tss = tss;
                _context = context;
                _collection = collection;
                _docId = docId;
                _name = name;

                SliceHolder = new TimeSeriesSliceHolder(_context, docId, name, _collection.Name).WithBaseline(timeStamp);
                SliceHolder.CreateSegmentBuffer();

                FromReplication = fromReplicationChangeVector != null;
                _tss.GenerateChangeVector(_context, fromReplicationChangeVector); // update the database change vector
            }

            public TimeSeriesSegmentHolder(
                TimeSeriesStorage tss,
                DocumentsOperationContext context,
                TimeSeriesSliceHolder allocator,
                string docId,
                string name,
                CollectionName collection,
                string fromReplicationChangeVector)
            {
                _tss = tss;
                _context = context;
                SliceHolder = allocator;
                _collection = collection;
                _docId = docId;
                _name = name;

                FromReplication = fromReplicationChangeVector != null;

                BaselineDate = allocator.CurrentBaseline;
                allocator.CreateSegmentBuffer();
                _tss.GenerateChangeVector(_context, fromReplicationChangeVector); // update the database change vector
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

            public long AppendExistingSegment(TimeSeriesValuesSegment newValueSegment)
            {
                (_currentChangeVector, _currentEtag) = _tss.GenerateChangeVector(_context);

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

                EnsureStatsAndDataIntegrity(_context, _docId, _name, newValueSegment);

                return count;
            }

            public void AppendDeadSegment(TimeSeriesValuesSegment newValueSegment)
            {
                (_currentChangeVector, _currentEtag) = _tss.GenerateChangeVector(_context);

                ValidateSegment(newValueSegment);
                MarkSegmentAsPendingDeletion(_context, _collection.Name, _currentEtag);

                ReduceCountBeforeAppend();
                //TODO: unused code, check if the number of modified entries is correct
                _tss.Stats.UpdateStats(_context, SliceHolder, _collection, newValueSegment, BaselineDate, ReadOnlySegment.NumberOfLiveEntries);

                using (Table.Allocate(out var tvb))
                {
                    tvb.Add(SliceHolder.TimeSeriesKeySlice);
                    tvb.Add(Bits.SwapBytes(_currentEtag));
                    tvb.Add(Slices.Empty); // we put empty slice in the change-vector, so it wouldn't replicate
                    tvb.Add(newValueSegment.Ptr, newValueSegment.NumberOfBytes);
                    tvb.Add(SliceHolder.CollectionSlice);
                    tvb.Add(_context.GetTransactionMarker());

                    Table.Set(tvb);
                }

                EnsureStatsAndDataIntegrity(_context, _docId, _name, newValueSegment);
            }

            public void AppendToNewSegment(SingleResult item)
            {
                BaselineDate = EnsureMillisecondsPrecision(item.Timestamp);
                SliceHolder.SetBaselineToKey(BaselineDate);

                var newSegment = new TimeSeriesValuesSegment(SliceHolder.SegmentBuffer.Ptr, MaxSegmentSize);
                newSegment.Initialize(item.Values.Length);
                newSegment.Append(_context.Allocator, 0, item.Values.Span, SliceHolder.TagAsSpan(item.Tag), item.Status);

                ValidateSegment(newSegment);

                _tss.Stats.UpdateStats(_context, SliceHolder, _collection, newSegment, BaselineDate, 1);
                _tss._documentDatabase.TimeSeriesPolicyRunner?.MarkForPolicy(_context, SliceHolder, BaselineDate, item.Status);

                (_currentChangeVector, _currentEtag) = _tss.GenerateChangeVector(_context);

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

                EnsureStatsAndDataIntegrity(_context, _docId, _name, newSegment);
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
                    FlushCurrentSegment(ref segment, values, tagSlice, status);
                    UpdateBaseline(timestampDiff);
                }
            }

            public bool LoadCurrentSegment()
            {
                if (Table.SeekOneBackwardByPrimaryKeyPrefix(SliceHolder.TimeSeriesPrefixSlice, SliceHolder.TimeSeriesKeySlice, out _tvr))
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

            public void UpdateBaseline(long timestampDiff)
            {
                Debug.Assert(timestampDiff > 0);
                BaselineDate = BaselineDate.AddMilliseconds(timestampDiff);
                SliceHolder.SetBaselineToKey(BaselineDate);
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
            string changeVectorFromReplication = null)
        {
            var holder = new SingleResult();

            return AppendTimestamp(context, documentId, collection, name, toAppend.Select(ToResult), changeVectorFromReplication);

            SingleResult ToResult(TimeSeriesOperation.AppendOperation element)
            {
                holder.Values = element.Values;
                holder.Tag = context.GetLazyString(element.Tag);
                holder.Timestamp = element.Timestamp;
                holder.Status = TimeSeriesValuesSegment.Live;
                return holder;
            }
        }

        private class AppendEnumerator : IEnumerator<SingleResult>
        {
            private readonly string _documentId;
            private readonly string _name;
            private readonly bool _fromReplication;
            private readonly IEnumerator<SingleResult> _toAppend;
            private SingleResult _current;

            public DateTime Last;
            public DateTime First;
            public long IteratedValues;

            public AppendEnumerator(string documentId, string name, IEnumerable<SingleResult> toAppend, bool fromReplication)
            {
                _documentId = documentId;
                _name = name;
                _fromReplication = fromReplication;
                _toAppend = toAppend.GetEnumerator();
            }

            public bool MoveNext()
            {
                var currentTimestamp = _current?.Timestamp;
                if (_toAppend.MoveNext() == false)
                {
                    if (_current != null)
                        Last = _current.Timestamp;

                    _current = null;
                    return false;
                }

                var next = _toAppend.Current;
                next.Timestamp = EnsureMillisecondsPrecision(next.Timestamp);

                if (_current == null)
                    First = next.Timestamp;

                if (currentTimestamp >= next.Timestamp)
                    throw new InvalidDataException($"The entries of '{_name}' time-series for document '{_documentId}' must be sorted by their timestamps, and cannot contain duplicate timestamps. " +
                                                   $"Got: current '{currentTimestamp:O}', next '{next.Timestamp:O}', make sure your measures have at least 1ms interval.");

                if (next.Values.Length == 0 && // dead values can have 0 length
                    next.Status == TimeSeriesValuesSegment.Live)
                    throw new InvalidDataException($"The entries of '{_name}' time-series for document '{_documentId}' must contain at least one value");

                if (_fromReplication == false)
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
                _toAppend.Dispose();
            }
        }

        public string AppendTimestamp(
            DocumentsOperationContext context,
            string documentId,
            string collection,
            string name,
            IEnumerable<SingleResult> toAppend,
            string changeVectorFromReplication = null,
            bool verifyName = true,
            bool addNewNameToMetadata = true)
        {
            if (context.Transaction == null)
            {
                DocumentPutAction.ThrowRequiresTransaction();
                Debug.Assert(false); // never hit
            }

            var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
            var newSeries = Stats.GetStats(context, documentId, name).Count == 0;
            if (newSeries && verifyName)
            {
                VerifyLegalName(name);
            }

            using (var appendEnumerator = new AppendEnumerator(documentId, name, toAppend, changeVectorFromReplication != null))
            {
                while (appendEnumerator.MoveNext())
                {
                    var retry = true;
                    while (retry)
                    {
                        retry = false;
                        var current = appendEnumerator.Current;
                        Debug.Assert(current != null);

                        if (changeVectorFromReplication == null)
                        {
                            // not from replication
                            AssertNoNanValue(current);
                        }

                        using (var slicer = new TimeSeriesSliceHolder(context, documentId, name, collection).WithBaseline(current.Timestamp))
                        {
                            var segmentHolder = new TimeSeriesSegmentHolder(this, context, slicer, documentId, name, collectionName, changeVectorFromReplication);
                            if (segmentHolder.LoadCurrentSegment() == false)
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

            if (newSeries && addNewNameToMetadata)
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
            var nextSegmentBaseline = BaselineOfNextSegment(segmentHolder, current.Timestamp) ?? DateTime.MaxValue;

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
                        if (appendEnumerator.MoveNext() == false)
                        {
                            segmentHolder.AppendExistingSegment(newSegment);
                            return true;
                        }

                        current = appendEnumerator.Current;
                        if (current.Timestamp < nextSegmentBaseline)
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
            var nextSegmentBaseline = BaselineOfNextSegment(timeSeriesSegment, current.Timestamp) ?? DateTime.MaxValue;
            var segmentToSplit = timeSeriesSegment.ReadOnlySegment;
            var segmentChanged = false;
            var additionalValueSize = Math.Max(0, current.Values.Length - timeSeriesSegment.ReadOnlySegment.NumberOfValues);
            var newNumberOfValues = additionalValueSize + timeSeriesSegment.ReadOnlySegment.NumberOfValues;

            using (context.Allocator.Allocate(segmentToSplit.NumberOfBytes, out var currentSegmentBuffer))
            {
                Memory.Copy(currentSegmentBuffer.Ptr, segmentToSplit.Ptr, segmentToSplit.NumberOfBytes);
                var readOnlySegment = new TimeSeriesValuesSegment(currentSegmentBuffer.Ptr, segmentToSplit.NumberOfBytes);

                var splitSegment = new TimeSeriesValuesSegment(timeSeriesSegment.SliceHolder.SegmentBuffer.Ptr, MaxSegmentSize);
                splitSegment.Initialize(current.Values.Span.Length);
                using (context.Allocator.Allocate(newNumberOfValues * sizeof(double), out var valuesBuffer))
                using (context.Allocator.Allocate(readOnlySegment.NumberOfValues * sizeof(TimestampState), out var stateBuffer))
                {
                    Memory.Set(valuesBuffer.Ptr, 0, valuesBuffer.Length);
                    Memory.Set(stateBuffer.Ptr, 0, stateBuffer.Length);

                    var currentValues = new Span<double>(valuesBuffer.Ptr, readOnlySegment.NumberOfValues);
                    var updatedValues = new Span<double>(valuesBuffer.Ptr, newNumberOfValues);
                    var state = new Span<TimestampState>(stateBuffer.Ptr, readOnlySegment.NumberOfValues);
                    var currentTag = new TimeSeriesValuesSegment.TagPointer();

                    for (int i = readOnlySegment.NumberOfValues; i < newNumberOfValues; i++)
                    {
                        updatedValues[i] = double.NaN;
                    }

                    using (var enumerator = readOnlySegment.GetEnumerator(context.Allocator))
                    {
                        while (enumerator.MoveNext(out var currentTimestamp, currentValues, state, ref currentTag, out var localStatus))
                        {
                            var currentTime = originalBaseline.AddMilliseconds(currentTimestamp);
                            while (true)
                            {
                                var compare = Compare(currentTime, currentValues, localStatus, current, nextSegmentBaseline, timeSeriesSegment);
                                if (compare != CompareResult.Remote)
                                {
                                    timeSeriesSegment.AddExistingValue(currentTime, updatedValues, currentTag.AsSpan(), ref splitSegment, localStatus);
                                    if (currentTime == current?.Timestamp)
                                    {
                                        reader.MoveNext();
                                        current = reader.Current;
                                    }
                                    break;
                                }

                                segmentChanged = true;
                                Debug.Assert(current != null);

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

                                if (currentTime == current.Timestamp)
                                {
                                    reader.MoveNext();
                                    current = reader.Current;
                                    break; // the local value was overwritten
                                }
                                reader.MoveNext();
                                current = reader.Current;
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

                    return retryAppend;
                }
            }
        }

        private enum CompareResult
        {
            Local,
            Equal,
            Remote
        }

        private static CompareResult Compare(DateTime localTime, Span<double> localValues, ulong localStatus, SingleResult remote, DateTime? nextSegmentBaseline, TimeSeriesSegmentHolder holder)
        {
            if (remote == null)
                return CompareResult.Local;

            if (localTime < remote.Timestamp)
                return CompareResult.Local;

            if (remote.Timestamp >= nextSegmentBaseline)
                return CompareResult.Local;

            if (localTime == remote.Timestamp)
            {
                if (holder.FromReplication == false)
                    return CompareResult.Remote; // if not from replication, remote value is an update

                // deletion wins
                if (localStatus == TimeSeriesValuesSegment.Dead)
                {
                    if (remote.Status == TimeSeriesValuesSegment.Dead)
                        return CompareResult.Equal;

                    return CompareResult.Local;
                }

                if (remote.Status == TimeSeriesValuesSegment.Dead) // deletion wins
                    return CompareResult.Remote;

                if (localValues.Length != remote.Values.Length)
                {
                    // larger number of values wins
                    if (localValues.Length > remote.Values.Length)
                        return CompareResult.Local;

                    return CompareResult.Remote;
                }

                var compare = localValues.SequenceCompareTo(remote.Values.Span);
                if (compare == 0)
                    return CompareResult.Equal;
                if (compare < 0)
                    return CompareResult.Remote;
                return CompareResult.Local;
            }

            return CompareResult.Remote;
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

        public void AddTimeSeriesNameToMetadata(DocumentsOperationContext ctx, string docId, string tsName)
        {
            if (Stats.GetStats(ctx, docId, tsName).Count == 0)
                return;

            // if the document is in conflict, that's fine
            // we will recreate '@timeseries' in metadata when the conflict is resolved
            var doc = ctx.DocumentDatabase.DocumentsStorage.Get(ctx, docId, throwOnConflict: false);
            if (doc == null)
                return;

            tsName = GetOriginalName(ctx, docId, tsName);

            var flags = doc.Flags;
            var newDocumentData = ModifyDocumentMetadata(ctx, docId, 
                namesToAdd: new SortedSet<string>(StringComparer.OrdinalIgnoreCase) { tsName }, 
                namesToRemove: null, doc.Data, ref flags);

            if (newDocumentData == null)
                return;

            _documentDatabase.DocumentsStorage.Put(ctx, doc.Id, expectedChangeVector: null, newDocumentData, flags: flags, nonPersistentFlags: NonPersistentDocumentFlags.ByTimeSeriesUpdate);
        }

        internal static BlittableJsonReaderObject ModifyDocumentMetadata(JsonOperationContext ctx, string docId, SortedSet<string> namesToAdd, HashSet<string> namesToRemove, BlittableJsonReaderObject data, ref DocumentFlags flags)
        {
            if (data == null || (namesToAdd?.Count ?? 0) == 0 && (namesToRemove?.Count ?? 0) == 0)
                return null;

            BlittableJsonReaderArray existingTsNames = null;
            if (data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
            {
                metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out existingTsNames);
            }

            var tsNames = CountersStorage.UpdateNamesList(existingTsNames, namesToAdd ?? new SortedSet<string>(StringComparer.OrdinalIgnoreCase), 
                namesToRemove, out bool modified);

            if (modified == false)
                return null;

            flags = flags.Strip(DocumentFlags.FromClusterTransaction | DocumentFlags.Resolved);

            if (tsNames.Count == 0)
            {
                flags = flags.Strip(DocumentFlags.HasTimeSeries);
                if (metadata != null)
                {
                    metadata.Modifications = new DynamicJsonValue(metadata);
                    metadata.Modifications.Remove(Constants.Documents.Metadata.TimeSeries);
                }
            }
            else
            {
                flags |= DocumentFlags.HasTimeSeries;
                if (metadata == null)
                {
                    data.Modifications = new DynamicJsonValue
                    {
                        [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                        {
                            [Constants.Documents.Metadata.TimeSeries] = new DynamicJsonArray(tsNames)
                        }
                    };
                }
                else
                {
                    metadata.Modifications = new DynamicJsonValue(metadata)
                    {
                        [Constants.Documents.Metadata.TimeSeries] = new DynamicJsonArray(tsNames)
                    };
                }
            }
            
            using (data)
            {
                return ctx.ReadObject(data, docId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
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
            }

            return item;
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

        public TimeSeriesSegmentEntry GetTimeSeries(DocumentsOperationContext context, Slice key)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);

            if (table.ReadByKey(key, out var reader) == false)
                return null;

            return CreateTimeSeriesItem(context, ref reader);
        }

        public TimeSeriesSegmentEntry GetTimeSeries(DocumentsOperationContext context, long etag)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);
            var index = TimeSeriesSchema.FixedSizeIndexes[AllTimeSeriesEtagSlice];

            if (table.Read(context.Allocator, index, etag, out var tvr) == false)
                return null;

            return CreateTimeSeriesItem(context, ref tvr);
        }

        public IEnumerable<TimeSeriesSegmentEntry> GetTimeSeriesFrom(DocumentsOperationContext context, long etag, long take)
        {
            var table = new Table(TimeSeriesSchema, context.Transaction.InnerTransaction);

            foreach (var result in table.SeekForwardFrom(TimeSeriesSchema.FixedSizeIndexes[AllTimeSeriesEtagSlice], etag, 0))
            {
                if (take-- <= 0)
                    yield break;

                yield return CreateTimeSeriesItem(context, ref result.Reader);
            }
        }

        public IEnumerable<TimeSeriesSegmentEntry> GetTimeSeriesFrom(DocumentsOperationContext context, string collection, long etag, long take)
        {
            var collectionName = _documentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
            if (collectionName == null)
                yield break;

            var table = GetOrCreateTimeSeriesTable(context.Transaction.InnerTransaction, collectionName);

            if (table == null)
                yield break;

            foreach (var result in table.SeekForwardFrom(TimeSeriesSchema.FixedSizeIndexes[CollectionTimeSeriesEtagsSlice], etag, skip: 0))
            {
                if (take-- <= 0)
                    yield break;

                yield return CreateTimeSeriesItem(context, ref result.Reader);
            }
        }

        internal static TimeSeriesSegmentEntry CreateTimeSeriesItem(JsonOperationContext context, ref TableValueReader reader)
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

        private (string ChangeVector, long NewEtag) GenerateChangeVector(DocumentsOperationContext context)
        {
            return GenerateChangeVector(context, null);
        }

        private (string ChangeVector, long NewEtag) GenerateChangeVector(DocumentsOperationContext context, string fromReplicationChangeVector)
        {
            var newEtag = _documentsStorage.GenerateNextEtag();
            string databaseChangeVector = context.LastDatabaseChangeVector ?? DocumentsStorage.GetDatabaseChangeVector(context);
            string changeVector = ChangeVectorUtils.TryUpdateChangeVector(_documentDatabase, databaseChangeVector, newEtag).ChangeVector;

            if (fromReplicationChangeVector != null)
            {
                changeVector = ChangeVectorUtils.MergeVectors(fromReplicationChangeVector, changeVector);
            }

            context.LastDatabaseChangeVector = changeVector;
            return (changeVector, newEtag);
        }

        private static void ValidateSegment(TimeSeriesValuesSegment segment)
        {
            if (segment.NumberOfBytes > MaxSegmentSize)
                throw new ArgumentOutOfRangeException("Attempted to write a time series segment that is larger (" + segment.NumberOfBytes + ") than the maximum size allowed.");
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

        private static void MarkSegmentAsPendingDeletion(DocumentsOperationContext context, string collection, long etag)
        {
            var pendingDeletion = context.Transaction.InnerTransaction.CreateTree(PendingDeletionSegments);
            using (context.Allocator.From(Bits.SwapBytes(etag), out var etagSlice))
            {
                pendingDeletion.MultiAdd(collection, new Slice(etagSlice));
            }
        }

        public long GetNumberOfTimeSeriesSegmentsToProcess(DocumentsOperationContext context, string collection, in long afterEtag, out long totalCount)
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

            return table.GetNumberOfEntriesAfter(indexDef, afterEtag, out totalCount);
        }

        
        [Conditional("DEBUG")]
        private static void EnsureStatsAndDataIntegrity(DocumentsOperationContext context, string docId, string name, TimeSeriesValuesSegment segment)
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

            Debug.Assert(first == stats.Start, $"Failed start check: {first} == {stats.Start}");
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
            }
        }

        internal enum TimeSeriesTable
        {
            // Format of this is:
            // lower document id, record separator, lower time series name, record separator, segment start
            TimeSeriesKey = 0,

            Etag = 1,
            ChangeVector = 2,
            Segment = 3,
            Collection = 4,
            TransactionMarker = 5
        }

        private enum DeletedRangeTable
        {
            // lower document id, record separator, lower time series name, record separator, local etag
            RangeKey = 0,

            Etag = 1,
            ChangeVector = 2,
            Collection = 3,
            TransactionMarker = 4,
            From = 5,
            To = 6,
        }
    }

    public class NanValueException : Exception
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

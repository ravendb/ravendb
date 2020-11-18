using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.TimeSeries
{
    public class TimeSeriesStats
    {
        private readonly TimeSeriesStorage _timeSeriesStorage;
        private static readonly Slice RawPolicySlice;
        private static readonly Slice TimeSeriesStatsKey;
        private static readonly Slice PolicyIndex;
        private static readonly Slice StartTimeIndex;
        private static readonly TableSchema TimeSeriesStatsSchema = new TableSchema();

        private enum StatsColumns
        {
            Key = 0, // documentId, separator, lower-name
            PolicyName = 1, // policy, separator
            Start = 2,
            End = 3,
            Count = 4,
            Name = 5 // original casing
        }

        static TimeSeriesStats()
        {
            using (StorageEnvironment.GetStaticContext(out ByteStringContext ctx))
            {
                Slice.From(ctx, RawTimeSeriesPolicy.PolicyString, SpecialChars.RecordSeparator, ByteStringType.Immutable, out RawPolicySlice);
                Slice.From(ctx, nameof(TimeSeriesStatsKey), ByteStringType.Immutable, out TimeSeriesStatsKey);
                Slice.From(ctx, nameof(PolicyIndex), ByteStringType.Immutable, out PolicyIndex);
                Slice.From(ctx, nameof(StartTimeIndex), ByteStringType.Immutable, out StartTimeIndex);
            }

            TimeSeriesStatsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)StatsColumns.Key,
                Count = 1, 
                Name = TimeSeriesStatsKey,
                IsGlobal = true
            });

            TimeSeriesStatsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)StatsColumns.PolicyName,
                Name = PolicyIndex
            });

            TimeSeriesStatsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                // policy, separator, start
                StartIndex = (int)StatsColumns.PolicyName,
                Count = 2, 
                Name = StartTimeIndex
            });
        }

        public TimeSeriesStats(TimeSeriesStorage timeSeriesStorage, Transaction tx)
        {
            _timeSeriesStorage = timeSeriesStorage;
            tx.CreateTree(TimeSeriesStatsKey);
        }

        private Table GetOrCreateTable(Transaction tx, CollectionName collection)
        {
            var tableName = collection.GetTableName(CollectionTableType.TimeSeriesStats); // TODO: cache the collection and pass Slice
            
            if (tx.IsWriteTransaction)
                TimeSeriesStatsSchema.Create(tx, tableName, 16);

            return tx.OpenTable(TimeSeriesStatsSchema, tableName);
        }

        public void UpdateCountOfExistingStats(DocumentsOperationContext context, string docId, string name, CollectionName collection, long count)
        {
            if (count == 0)
                return;

            using (var slicer = new TimeSeriesSliceHolder(context, docId, name))
            {
                UpdateCountOfExistingStats(context, slicer, collection, count);
            }
        }

        public void UpdateCountOfExistingStats(DocumentsOperationContext context, TimeSeriesSliceHolder slicer, CollectionName collection, long count)
        {
            if (count == 0)
                return;

            var table = GetOrCreateTable(context.Transaction.InnerTransaction, collection);
            using (ReadStats(context, table, slicer, out var oldCount, out var start, out var end, out var name))
            {
                if (oldCount == 0)
                    return;

                using (table.Allocate(out var tvb))
                {
                    tvb.Add(slicer.StatsKey);
                    tvb.Add(GetPolicy(slicer));
                    tvb.Add(Bits.SwapBytes(start.Ticks));
                    tvb.Add(end);
                    tvb.Add(oldCount + count);
                    tvb.Add(name);

                    table.Set(tvb);
                }
            }
        }

        public void UpdateDates(DocumentsOperationContext context, TimeSeriesSliceHolder slicer, CollectionName collection, DateTime start, DateTime end)
        {
            var table = GetOrCreateTable(context.Transaction.InnerTransaction, collection);
            using (ReadStats(context, table, slicer, out var count, out _, out _, out var name))
            using (table.Allocate(out var tvb))
            {
                if (count == 0)
                    return;

                tvb.Add(slicer.StatsKey);
                tvb.Add(GetPolicy(slicer));
                tvb.Add(Bits.SwapBytes(start.Ticks));
                tvb.Add(end);
                tvb.Add(count);
                tvb.Add(name);

                table.Set(tvb);
            }
        }

        private static IDisposable ReadStats(DocumentsOperationContext context, Table table, TimeSeriesSliceHolder slicer, out long count, out DateTime start, out DateTime end, out Slice name)
        {
            count = 0;
            start = DateTime.MaxValue;
            end = DateTime.MinValue;
            name = slicer.NameSlice;

            if (table.ReadByKey(slicer.StatsKey, out var tvr) == false) 
                return null;

            count = DocumentsStorage.TableValueToLong((int)StatsColumns.Count, ref tvr);
            start = new DateTime(Bits.SwapBytes(DocumentsStorage.TableValueToLong((int)StatsColumns.Start, ref tvr)));
            end = DocumentsStorage.TableValueToDateTime((int)StatsColumns.End, ref tvr);

            if (count == 0 && start == default && end == default)
            {
                // this is delete a stats, that we re-create, so we need to treat is as a new one.
                start = DateTime.MaxValue;
                end = DateTime.MinValue;
                return null;
            }

            return DocumentsStorage.TableValueToSlice(context, (int)StatsColumns.Name, ref tvr, out name);

        }

        public long UpdateStats(DocumentsOperationContext context, TimeSeriesSliceHolder slicer, CollectionName collection, TimeSeriesValuesSegment segment, DateTime baseline, int modifiedEntries)
        {
            long previousCount;
            DateTime start, end;

            context.DocumentDatabase.Metrics.TimeSeries.PutsPerSec.MarkSingleThreaded(modifiedEntries);
            context.DocumentDatabase.Metrics.TimeSeries.BytesPutsPerSec.MarkSingleThreaded(segment.NumberOfBytes);

            var table = GetOrCreateTable(context.Transaction.InnerTransaction, collection);

            using (ReadStats(context, table, slicer, out previousCount, out start, out end, out var name))
            {
                var liveEntries = segment.NumberOfLiveEntries;
                if (liveEntries > 0)
                {
                    HandleLiveSegment();
                }

                if (liveEntries == 0) 
                {
                    if (TryHandleDeadSegment() == false)
                    {
                        // this ts was completely deleted
                        //TimeSeriesStorage.RemoveTimeSeriesNameFromMetadata(context, slicer.DocId, slicer.Name);
                        start = end = default;
                    }
                }

                var count = previousCount + liveEntries;

                using (table.Allocate(out var tvb))
                {
                    tvb.Add(slicer.StatsKey);
                    tvb.Add(GetPolicy(slicer));
                    tvb.Add(Bits.SwapBytes(start.Ticks));
                    tvb.Add(end);
                    tvb.Add(count);
                    tvb.Add(name);

                    table.Set(tvb);
                }

                return count;

            }

            void HandleLiveSegment()
            {
                var lastTimestamp = GetLastLiveTimestamp(context, segment, baseline);

                if (lastTimestamp > end)
                {
                    end = lastTimestamp; // found later end
                }
                else
                {
                    var reader = _timeSeriesStorage.GetReader(context, slicer.DocId, slicer.Name, start, DateTime.MaxValue);
                    var last = reader.Last();
               
                    var lastValueInCurrentSegment = reader.ReadBaselineAsDateTime() == baseline;
                    end = lastValueInCurrentSegment ? lastTimestamp : last.Timestamp;
                }

                var first = segment.YieldAllValues(context, baseline, includeDead: false).First().Timestamp;
                if (first < start)
                    start = first; // found earlier start

                if (baseline <= start && first >= start)
                {
                    // start was removed
                    start = first;
                }
            }

            bool TryHandleDeadSegment()
            {
                if (previousCount == 0)
                    return false; // if current and previous are zero it means that this time-series was completely deleted

                var readerOfFirstValue = _timeSeriesStorage.GetReader(context, slicer.DocId, slicer.Name, DateTime.MinValue, DateTime.MaxValue);
                readerOfFirstValue.First();
                var firstValueInCurrentSegment = readerOfFirstValue.ReadBaselineAsDateTime() == baseline;

                var last = segment.GetLastTimestamp(baseline);
                if (baseline <= start && last >= start || firstValueInCurrentSegment)
                {
                    // start was removed, need to find the next start

                    // this segment isn't relevant, so let's get the next one
                    var next = _timeSeriesStorage.GetReader(context, slicer.DocId, slicer.Name, start, DateTime.MaxValue).NextSegmentBaseline();
                    var reader = _timeSeriesStorage.GetReader(context, slicer.DocId, slicer.Name, next, DateTime.MaxValue);

                    var first = reader.First();
                    if (first == default)
                        return false;

                    start = first.Timestamp;
                }

                var readerOfLastValue = _timeSeriesStorage.GetReader(context, slicer.DocId, slicer.Name, start, DateTime.MaxValue);
                readerOfLastValue.Last();

                var lastValueInCurrentSegment = readerOfLastValue.ReadBaselineAsDateTime() == baseline;

                if (baseline <= end && end <= last || lastValueInCurrentSegment)
                {
                    var lastEntry = _timeSeriesStorage.GetReader(context, slicer.DocId, slicer.Name, start, baseline.AddMilliseconds(-1)).Last();
                    if (lastEntry == default)
                        return false;

                    end = lastEntry.Timestamp;
                }

                return true;
            }
        }

        private static DateTime GetLastLiveTimestamp(DocumentsOperationContext context, TimeSeriesValuesSegment segment, DateTime baseline)
        {
            return segment.NumberOfEntries == segment.NumberOfLiveEntries
                ? segment.GetLastTimestamp(baseline) // all values are alive, so we can get the last value fast
                : segment.YieldAllValues(context, baseline, includeDead: false).Last().Timestamp;
        }

        public (long Count, DateTime Start, DateTime End) GetStats(DocumentsOperationContext context, string docId, string name)
        {
            using (var slicer = new TimeSeriesSliceHolder(context, docId, name))
            {
                return GetStats(context, slicer);
            }
        }

        public string GetTimeSeriesNameOriginalCasing(DocumentsOperationContext context, string docId, string name)
        {
            using (var slicer = new TimeSeriesSliceHolder(context, docId, name))
            {
                return GetTimeSeriesNameOriginalCasing(context, slicer.StatsKey);
            }
        }

        public (long Count, DateTime Start, DateTime End) GetStats(DocumentsOperationContext context, TimeSeriesSliceHolder slicer)
        {
            return GetStats(context, slicer.StatsKey);
        }

        public (long Count, DateTime Start, DateTime End) GetStats(DocumentsOperationContext context, Slice statsKey)
        {
            var table = new Table(TimeSeriesStatsSchema, context.Transaction.InnerTransaction);
            if (table.ReadByKey(statsKey, out var tvr) == false)
                return default;

            return GetStats(ref tvr);
        }

        public (long Count, DateTime Start, DateTime End) GetStats(ref TableValueReader tvr)
        {
            var count = DocumentsStorage.TableValueToLong((int)StatsColumns.Count, ref tvr);
            var start = new DateTime(Bits.SwapBytes(DocumentsStorage.TableValueToLong((int)StatsColumns.Start, ref tvr)));
            var end = DocumentsStorage.TableValueToDateTime((int)StatsColumns.End, ref tvr);

            return (count, start, end);
        }

        public IEnumerable<string> GetTimeSeriesNamesForDocumentOriginalCasing(DocumentsOperationContext context, string docId)
        {
            var table = new Table(TimeSeriesStatsSchema, context.Transaction.InnerTransaction);
            using (DocumentIdWorker.GetSliceFromId(context, docId, out var documentKeyPrefix, SpecialChars.RecordSeparator))
            {
                foreach (var result in table.SeekByPrimaryKeyPrefix(documentKeyPrefix, Slices.Empty, 0))
                {
                    var name = DocumentsStorage.TableValueToChangeVector(context, (int)StatsColumns.Name, ref result.Value.Reader);
                    if (GetStats(context, docId, name).Count == 0)
                        continue;

                    yield return name;
                }
            }
        }

        public LazyStringValue GetTimeSeriesNameOriginalCasing(DocumentsOperationContext context, Slice key)
        {
            var table = new Table(TimeSeriesStatsSchema, context.Transaction.InnerTransaction);
            if (table.ReadByKey(key, out var tvr) == false)
                return null;

            return DocumentsStorage.TableValueToString(context, (int)StatsColumns.Name, ref tvr);
        }

        public void UpdateTimeSeriesName(DocumentsOperationContext context, CollectionName collection, TimeSeriesSliceHolder slicer)
        {
            // This method should only be called from IncomingReplicationHandler, 
            // and only when the incoming TS exists locally but under a different casing
            // and local-name > remote-name lexicographically 

            var table = context.Transaction.InnerTransaction.OpenTable(TimeSeriesStatsSchema, collection.GetTableName(CollectionTableType.TimeSeriesStats));
            using (ReadStats(context, table, slicer, out var count, out var start, out var end, out _))
            {
                if (count == 0)
                    return;

                using (table.Allocate(out var tvb))
                {
                    tvb.Add(slicer.StatsKey);
                    tvb.Add(GetPolicy(slicer));
                    tvb.Add(Bits.SwapBytes(start.Ticks));
                    tvb.Add(end);
                    tvb.Add(count);
                    tvb.Add(slicer.NameSlice);

                    table.Set(tvb);
                }
            }
        }

        public IEnumerable<Slice> GetTimeSeriesNameByPolicy(DocumentsOperationContext context, CollectionName collection, string policy, long skip, int take)
        {
            var table = GetOrCreateTable(context.Transaction.InnerTransaction, collection);
            using (Slice.From(context.Allocator, policy.ToLowerInvariant(), SpecialChars.RecordSeparator, ByteStringType.Immutable, out var name))
            {
                foreach (var result in table.SeekForwardFrom(TimeSeriesStatsSchema.Indexes[PolicyIndex], name, skip, startsWith: true))
                {
                    var stats = GetStats(ref result.Result.Reader);
                    if (stats.Count == 0)
                        continue;

                    var reader = result.Result.Reader;
                    DocumentsStorage.TableValueToSlice(context, (int)StatsColumns.Key, ref reader, out var slice);
                    yield return slice;

                    take--;
                    if (take <= 0)
                        yield break;
                }
            }
        }

        public IEnumerable<Slice> GetTimeSeriesByPolicyFromStartDate(DocumentsOperationContext context, CollectionName collection, string policy, DateTime start, long take)
        {
            var table = GetOrCreateTable(context.Transaction.InnerTransaction, collection);
            if (table == null)
                yield break;

            using (CombinePolicyNameAndTicks(context, policy.ToLowerInvariant(), start.Ticks, out var key,out var policySlice))
            {
                foreach (var result in table.SeekBackwardFrom(TimeSeriesStatsSchema.Indexes[StartTimeIndex], policySlice, key))
                {
                    var stats = GetStats(ref result.Result.Reader);
                    if (stats.Count == 0)
                        continue;

                    DocumentsStorage.TableValueToSlice(context, (int)StatsColumns.Key, ref result.Result.Reader, out var slice);
                    var currentStart = new DateTime(Bits.SwapBytes(DocumentsStorage.TableValueToLong((int)StatsColumns.Start, ref result.Result.Reader)));
                    if (currentStart > start)
                        yield break;

                    yield return slice;

                    take--;
                    if (take <= 0)
                        yield break;
                }
            }
        }

        private static unsafe ByteStringContext<ByteStringMemoryCache>.InternalScope CombinePolicyNameAndTicks(DocumentsOperationContext context, string policy, long ticks, out Slice key, out Slice policyNameSlice)
        {
            using (DocumentIdWorker.GetSliceFromId(context, policy, out policyNameSlice, SpecialChars.RecordSeparator))
            {
                var size = policyNameSlice.Size + sizeof(long);
                var scope = context.Allocator.Allocate(size, out var str);
                policyNameSlice.CopyTo(str.Ptr);
                *(long*)(str.Ptr + policyNameSlice.Size) = Bits.SwapBytes(ticks);

                Slice.External(context.Allocator, str, 0, policyNameSlice.Size, out policyNameSlice);
                key = new Slice(str);
                return scope;
            }
        }

        public IEnumerable<Slice> GetAllPolicies(DocumentsOperationContext context, CollectionName collection)
        {
            var table = GetOrCreateTable(context.Transaction.InnerTransaction, collection);
            if (table == null)
                yield break;

            var policies = table.GetTree(TimeSeriesStatsSchema.Indexes[PolicyIndex]);
            using (var it = policies.Iterate(true))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    yield break;

                do
                {
                    yield return it.CurrentKey.Clone(context.Allocator);
                } while (it.MoveNext());
            }
        }

        public bool DeleteStats(DocumentsOperationContext context, CollectionName collection, Slice key)
        {
            var table = GetOrCreateTable(context.Transaction.InnerTransaction, collection);
            return table.DeleteByKey(key);
        }

        public bool DeleteByPrimaryKeyPrefix(DocumentsOperationContext context, CollectionName collection, Slice key)
        {
            var table = GetOrCreateTable(context.Transaction.InnerTransaction, collection);
            return table.DeleteByPrimaryKeyPrefix(key);
        }

        private Slice GetPolicy(TimeSeriesSliceHolder slicer)
        {
            var name = slicer.LowerTimeSeriesName;
            var index = name.Content.IndexOf((byte)TimeSeriesConfiguration.TimeSeriesRollupSeparator);
            if (index < 0)
                return RawPolicySlice;
            var offset = index + 1;
            return slicer.PolicyNameWithSeparator(name, offset, name.Content.Length - offset);
        }

        internal long GetNumberOfEntries(DocumentsOperationContext context)
        {
            var index = TimeSeriesStatsSchema.Key;
            var tree = context.Transaction.InnerTransaction.ReadTree(index.Name);
            return tree.State.NumberOfEntries;
        }

        public static IDisposable ExtractStatsKeyFromStorageKey(DocumentsOperationContext context, Slice storageKey, out Slice statsKey)
        {
            var size = storageKey.Size -
                       sizeof(long) - // baseline
                       1; // record separator
            return Slice.External(context.Allocator, storageKey, size, out statsKey);
        }
    }
}

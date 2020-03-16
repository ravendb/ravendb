using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.TimeSeries
{
    public class TimeSeriesStats
    {
        private static readonly Slice TimeSeriesStatsKey;
        private static readonly Slice PolicyIndex;
        private static readonly Slice StartTimeIndex;
        private static readonly TableSchema TimeSeriesStatsSchema = new TableSchema();

        private enum StatsColumn
        {
            Key = 0, // documentId, separator, name
            PolicyName = 1, // TODO: need separator here?
            Count = 2,
            Start = 3,
            End = 4
        }

        static TimeSeriesStats()
        {
            using (StorageEnvironment.GetStaticContext(out ByteStringContext ctx))
            {
                Slice.From(ctx, "TimeSeriesStats", ByteStringType.Immutable, out TimeSeriesStatsKey);
                Slice.From(ctx, "PolicyIndex", ByteStringType.Immutable, out PolicyIndex);
                Slice.From(ctx, "StartTimeIndex", ByteStringType.Immutable, out StartTimeIndex);
            }

            TimeSeriesStatsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)StatsColumn.Key,
                Count = 1, 
                Name = TimeSeriesStatsKey,
                IsGlobal = true
            });

            TimeSeriesStatsSchema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)StatsColumn.PolicyName,
                Name = PolicyIndex
            });
        }

        public TimeSeriesStats(Transaction tx)
        {
            tx.CreateTree(TimeSeriesStatsKey);
        }

        private Table GetOrCreateTable(Transaction tx, CollectionName collection)
        {
            var tableName = collection.GetTableName(CollectionTableType.TimeSeriesStats); // TODO: cache the collection and pass Slice
            TimeSeriesStatsSchema.Create(tx, tableName, 16);
            return tx.OpenTable(TimeSeriesStatsSchema, tableName);
        }

        public void UpdateCount(DocumentsOperationContext context, string docId, string name, CollectionName collection, long count)
        {
            using (var slicer = new TimeSeriesSliceHolder(context, docId, name))
            {
                UpdateCount(context, slicer, collection, count);
            }
        }

        public void UpdateStats(DocumentsOperationContext context, string docId, string name, CollectionName collection, TimeSeriesValuesSegment segment, DateTime baseline)
        {
            using (var slicer = new TimeSeriesSliceHolder(context, docId, name))
            {
                long previousCount = 0;
                var start = DateTime.MinValue; 
                var end = DateTime.MinValue; 

                var table = GetOrCreateTable(context.Transaction.InnerTransaction, collection);
                if (table.ReadByKey(slicer.StatsKey, out var tvr))
                {
                    previousCount = DocumentsStorage.TableValueToLong((int)StatsColumn.Count, ref tvr);
                    start = DocumentsStorage.TableValueToDateTime((int)StatsColumn.Start, ref tvr);
                    end = DocumentsStorage.TableValueToDateTime((int)StatsColumn.End, ref tvr);
                }

                var count = segment.NumberOfLiveEntries;
                if (count > 0)
                {
                    var last = segment.GetLastTimestamp(baseline);
                    if (last > end)
                        end = last;

                    var first = segment.YieldAllValues(context, context.Allocator, baseline, includeDead: false).First().Timestamp;
                    if (first < start)
                        start = first;
                }

                using (table.Allocate(out var tvb))
                {
                    tvb.Add(slicer.StatsKey);
                    tvb.Add(GetPolicy(slicer));
                    tvb.Add(previousCount + count);
                    tvb.Add(start);
                    tvb.Add(end);
                    table.Set(tvb);
                }
            }
        }

        public void UpdateCount(DocumentsOperationContext context, TimeSeriesSliceHolder slicer, CollectionName collection, long count)
        {
            long previousCount = 0;
            var table = GetOrCreateTable(context.Transaction.InnerTransaction, collection);
            if (table.ReadByKey(slicer.StatsKey, out var tvr))
            {
                previousCount = DocumentsStorage.TableValueToLong((int)StatsColumn.Count, ref tvr);
            }

            using (table.Allocate(out var tvb))
            {
                tvb.Add(slicer.StatsKey);
                tvb.Add(GetPolicy(slicer));
                tvb.Add(previousCount + count);

                table.Set(tvb);
            }
        }


        public long GetCount(DocumentsOperationContext context, string docId, string name)
        {
            var table = new Table(TimeSeriesStatsSchema, context.Transaction.InnerTransaction);
            using (var slicer = new TimeSeriesSliceHolder(context, docId, name))
            {
                return table.ReadByKey(slicer.StatsKey, out var tvr) ? DocumentsStorage.TableValueToLong((int)StatsColumn.Count, ref tvr) : 0;
            }
        }

        public IEnumerable<Slice> GetTimeSeriesNameByPolicy(DocumentsOperationContext context, CollectionName collection, string policy, long skip, int take)
        {
            var table = GetOrCreateTable(context.Transaction.InnerTransaction, collection);
            using (DocumentIdWorker.GetLower(context.Allocator, policy, out var name))
            {
                foreach (var result in table.SeekForwardFrom(TimeSeriesStatsSchema.Indexes[PolicyIndex], name, skip, startsWith: true))
                {
                    var reader = result.Result.Reader;
                    DocumentsStorage.TableValueToSlice(context, (int)StatsColumn.Key, ref reader, out var slice);
                    yield return slice;

                    take--;
                    if (take <= 0)
                        yield break;
                }
            }
        }

        public IEnumerable<Slice> GetAllPolicies(DocumentsOperationContext context, CollectionName collection)
        {
            var table = GetOrCreateTable(context.Transaction.InnerTransaction, collection);

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

        public void DeleteStats(DocumentsOperationContext context, CollectionName collection, Slice key)
        {
            var table = GetOrCreateTable(context.Transaction.InnerTransaction, collection);
            table.DeleteByKey(key);
        }

        private Slice GetPolicy(TimeSeriesSliceHolder slicer)
        {
            var name = slicer.LowerTimeSeriesName;
            var index = name.Content.IndexOf((byte)TimeSeriesConfiguration.TimeSeriesRollupSeparator);
            if (index < 0)
                return TimeSeriesPolicyRunner.RawPolicySlice;
            var offset = index + 1;
            return slicer.External(name, offset, name.Content.Length - offset);
        }
    }
}

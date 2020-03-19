using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.TimeSeries
{
    public class TimeSeriesRollups
    {
        public static readonly Slice RawPolicySlice;

        private static readonly TableSchema RollUpSchema;
        public static readonly Slice TimeSeriesRollUps;
        private static readonly Slice RollUpKey;
        private static readonly Slice NextRollUpIndex;
        private enum RollUpColumns
        {
            // documentId/Name
            Key,
            Collection,
            NextRollUp,
            PolicyToApply,
            Etag,
            ChangeVector
        }

        internal class RollUpState
        {
            public Slice Key;
            public string DocId;
            public string Name;
            public long Etag;
            public string Collection;
            public DateTime NextRollUp;
            public string RollUpPolicy;
            public string ChangeVector;
        }

        static TimeSeriesRollups()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, nameof(TimeSeriesRollUps), ByteStringType.Immutable, out TimeSeriesRollUps);
                Slice.From(ctx, nameof(RollUpKey), ByteStringType.Immutable, out RollUpKey);
                Slice.From(ctx, nameof(NextRollUpIndex), ByteStringType.Immutable, out NextRollUpIndex);
                Slice.From(ctx, RawTimeSeriesPolicy.PolicyString, ByteStringType.Immutable, out RawPolicySlice);
            }

            RollUpSchema = new TableSchema();
            RollUpSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)RollUpColumns.Key,
                Count = 1, 
                Name = RollUpKey
            });

            RollUpSchema.DefineIndex(new TableSchema.SchemaIndexDef // this isn't fixed-size since we expect to have duplicates
            {
                StartIndex = (int)RollUpColumns.NextRollUp, 
                Count = 1,
                Name = NextRollUpIndex
            });
        }

        public unsafe void MarkForPolicy(DocumentsOperationContext context, TimeSeriesSliceHolder slicerHolder, TimeSeriesPolicy nextPolicy, long etag, DateTime baseline, string changeVector)
        {
            var nextRollup = NextRollup(baseline, nextPolicy);

            // mark for rollup
            RollUpSchema.Create(context.Transaction.InnerTransaction, TimeSeriesRollUps, 16);
            var table = context.Transaction.InnerTransaction.OpenTable(RollUpSchema, TimeSeriesRollUps);
            using (table.Allocate(out var tvb))
            using (Slice.From(context.Allocator, nextPolicy.Name, ByteStringType.Immutable, out var policyToApply))
            using (Slice.From(context.Allocator, changeVector, ByteStringType.Immutable, out var changeVectorSlice))
            {
                if (table.ReadByKey(slicerHolder.StatsKey, out var tvr))
                {
                    // check if we need to update this
                    var existingRollup = Bits.SwapBytes(*(long*)tvr.Read((int)RollUpColumns.NextRollUp, out _));
                    if (existingRollup <= nextRollup)
                        return; // we have an earlier date to roll up from
                }

                tvb.Add(slicerHolder.StatsKey);
                tvb.Add(slicerHolder.CollectionSlice);
                tvb.Add(Bits.SwapBytes(nextRollup));
                tvb.Add(policyToApply);
                tvb.Add(etag);
                tvb.Add(changeVectorSlice);

                table.Set(tvb);
            }
        }

        internal void PrepareRollUps(DocumentsOperationContext context, DateTime currentTime, long take, List<RollUpState> states, out Stopwatch duration)
        {
            duration = Stopwatch.StartNew();

            var table = context.Transaction.InnerTransaction.OpenTable(RollUpSchema, TimeSeriesRollUps);
            if (table == null)
                return;

            var currentTicks = currentTime.Ticks;

            foreach (var item in table.SeekForwardFrom(RollUpSchema.Indexes[NextRollUpIndex], Slices.BeforeAllKeys, 0))
            {
                if (take <= 0)
                    return;

                var rollUpTime = DocumentsStorage.TableValueToEtag((int)RollUpColumns.NextRollUp, ref item.Result.Reader);
                if (rollUpTime > currentTicks)
                    return;

                DocumentsStorage.TableValueToSlice(context, (int)RollUpColumns.Key, ref item.Result.Reader,out var key);
                SplitKey(key, out var docId, out var name);

                var state = new RollUpState
                {
                    Key = key,
                    DocId = docId,
                    Name = name,
                    Collection = DocumentsStorage.TableValueToId(context, (int)RollUpColumns.Collection, ref item.Result.Reader),
                    NextRollUp = new DateTime(rollUpTime),
                    RollUpPolicy = DocumentsStorage.TableValueToString(context, (int)RollUpColumns.PolicyToApply, ref item.Result.Reader),
                    Etag = DocumentsStorage.TableValueToLong((int)RollUpColumns.Etag, ref item.Result.Reader),
                    ChangeVector = DocumentsStorage.TableValueToChangeVector(context, (int)RollUpColumns.ChangeVector, ref item.Result.Reader)
                };

                states.Add(state);
                take--;
            }
        }

        public static void SplitKey(Slice key, out string docId, out string name)
        {
            var bytes = key.AsSpan();
            var separatorIndex = key.Content.IndexOf(SpecialChars.RecordSeparator);

            docId = Encoding.UTF8.GetString(bytes.Slice(0, separatorIndex));
            var index = separatorIndex + 1;
            name = Encoding.UTF8.GetString(bytes.Slice(index, bytes.Length - index));
        }

        internal class TimeSeriesRetentionCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public const int BatchSize = 1024;

            private readonly List<Slice> _keys;
            private readonly TimeSeriesStorage.DeletionRangeRequest _request;

            public TimeSeriesRetentionCommand(List<Slice> keys, TimeSeriesStorage.DeletionRangeRequest request)
            {
                _keys = keys;
                _request = request;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                foreach (var key in _keys)
                {
                    SplitKey(key, out var docId, out var name);
                         
                    _request.Name = name;
                    _request.DocumentId = docId;

                    context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage.RemoveTimestampRange(context, _request);
                }

                return _keys.Count;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                throw new NotImplementedException();
            }
        }
        internal class RemovePoliciesCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public const int BatchSize = 1024;

            private readonly CollectionName _collection;
            private readonly string _policy;

            public int Deleted;

            public RemovePoliciesCommand(CollectionName collection, string policy)
            {
                _collection = collection;
                _policy = policy;
            }
            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var tss = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage;
                RollUpSchema.Create(context.Transaction.InnerTransaction, TimeSeriesRollUps, 16);
                var table = context.Transaction.InnerTransaction.OpenTable(RollUpSchema, TimeSeriesRollUps);

                var toDelete = new List<Slice>(tss.Stats.GetTimeSeriesNameByPolicy(context, _collection, _policy, 0, BatchSize));

                var request = new TimeSeriesStorage.DeletionRangeRequest
                {
                    Collection = _collection.Name,
                    From = DateTime.MinValue,
                    To = DateTime.MaxValue,
                };

                foreach (var key in toDelete)
                {
                    SplitKey(key, out var docId, out var name);
                    request.DocumentId = docId;
                    request.Name = name;
                    
                    tss.RemoveTimestampRange(context, request);

                    table.DeleteByKey(key);

                    Deleted++;
                }

                return Deleted;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                throw new NotImplementedException();
            }
        }

        internal class AddedNewRollupPoliciesCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public const int BatchSize = 1024;
            private readonly CollectionName _collection;
            private readonly TimeSeriesPolicy _from;
            private readonly TimeSeriesPolicy _to;
            private readonly int _skip;

            public int Marked;

            public AddedNewRollupPoliciesCommand(CollectionName collection, TimeSeriesPolicy from, TimeSeriesPolicy to, int skip)
            {
                _collection = collection;
                _from = from;
                _to = to;
                _skip = skip;
            }
            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var tss = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage;
                RollUpSchema.Create(context.Transaction.InnerTransaction, TimeSeriesRollUps, 16);
                var table = context.Transaction.InnerTransaction.OpenTable(RollUpSchema, TimeSeriesRollUps);
                foreach (var key in tss.Stats.GetTimeSeriesNameByPolicy(context, _collection, _from.Name, _skip, BatchSize))
                {
                    using (table.Allocate(out var tvb))
                    using (DocumentIdWorker.GetStringPreserveCase(context, _collection.Name, out var collectionSlice))
                    using (Slice.From(context.Allocator, _to.Name, ByteStringType.Immutable, out var policyToApply))
                    using (Slice.From(context.Allocator, string.Empty, ByteStringType.Immutable, out var changeVectorSlice))
                    {
                        tvb.Add(key);
                        tvb.Add(collectionSlice);
                        tvb.Add(Bits.SwapBytes(_to.AggregationTime.Ticks));
                        tvb.Add(policyToApply);
                        tvb.Add(0);
                        tvb.Add(changeVectorSlice);

                        table.Set(tvb);
                    }

                    Marked++;
                }

                return Marked;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                throw new NotImplementedException();
            }
        }

        internal class RollupTimeSeriesCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly TimeSeriesConfiguration _configuration;
            private readonly DateTime _now;
            private readonly List<RollUpState> _states;
            private readonly bool _isFirstInTopology;

            public long RolledUp;

            internal RollupTimeSeriesCommand(TimeSeriesConfiguration configuration, DateTime now, List<RollUpState> states, bool isFirstInTopology)
            {
                _configuration = configuration;
                _now = now;
                _states = states;
                _isFirstInTopology = isFirstInTopology;
            }
            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var tss = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage;
                RollUpSchema.Create(context.Transaction.InnerTransaction, TimeSeriesRollUps, 16);
                var table = context.Transaction.InnerTransaction.OpenTable(RollUpSchema, TimeSeriesRollUps);

                foreach (var item in _states)
                {
                    if (_configuration == null)
                        return RolledUp;

                    if (_configuration.Collections.TryGetValue(item.Collection, out var config) == false)
                        continue;

                    if (config.Disabled)
                        continue;
                        
                    var policy = config.GetPolicyByName(item.RollUpPolicy, out _);
                    if (policy == null)
                        continue;

                    if (table.ReadByKey(item.Key, out var current) == false)
                    {
                        table.DeleteByKey(item.Key);
                        continue;
                    }

                    if (item.Etag != DocumentsStorage.TableValueToLong((int)RollUpColumns.Etag, ref current))
                        continue; // concurrency check

                    var rollupStart = item.NextRollUp.Add(-policy.AggregationTime);
                    var rawTimeSeries = item.Name.Split(TimeSeriesConfiguration.TimeSeriesRollupSeparator)[0];
                    var intoTimeSeries = policy.GetTimeSeriesName(rawTimeSeries);
                    
                    var intoReader = tss.GetReader(context, item.DocId, intoTimeSeries, rollupStart, DateTime.MaxValue);
                    var previouslyAggregated = intoReader.AllValues().Any();
                    if (previouslyAggregated)
                    {
                        var changeVector = intoReader.GetCurrentSegmentChangeVector();
                        if (ChangeVectorUtils.GetConflictStatus(item.ChangeVector, changeVector) == ConflictStatus.AlreadyMerged)
                        {
                            // this rollup is already done
                            table.DeleteByKey(item.Key);
                            continue;
                        }
                    }

                    if (_isFirstInTopology == false)
                        continue; // we execute the actual rollup only on the primary node to avoid conflicts

                    var rollupEnd = new DateTime(NextRollup(_now.Add(-policy.AggregationTime), policy));
                    var reader = tss.GetReader(context, item.DocId, item.Name, rollupStart, rollupEnd);
                    var values = tss.GetAggregatedValues(reader, DateTime.MinValue, policy.AggregationTime, policy.Type);

                    if (previouslyAggregated)
                    {
                        // if we need to re-aggregate we need to delete everything we have from that point on.  
                        var removeRequest = new TimeSeriesStorage.DeletionRangeRequest
                        {
                            Collection = item.Collection,
                            DocumentId = item.DocId,
                            Name = intoTimeSeries,
                            From = rollupStart,
                            To = DateTime.MaxValue
                        };

                        tss.RemoveTimestampRange(context, removeRequest);
                    }
                    
                    tss.AppendTimestamp(context, item.DocId, item.Collection, intoTimeSeries, values);
                    RolledUp++;
                    table.DeleteByKey(item.Key);

                    var stats = tss.Stats.GetStats(context, item.DocId, item.Name);
                    if (stats.End > rollupEnd)
                    {
                        // we know that we have values after the current rollup and we need to mark them
                        var nextRollup = new DateTime(NextRollup(rollupEnd, policy));
                        intoReader = tss.GetReader(context, item.DocId, intoTimeSeries, nextRollup, DateTime.MaxValue);
                        if (intoReader.Init() == false)
                        {
                            Debug.Assert(false,"We have values but no segment?");
                            continue;
                        }

                        using (var slicer = new TimeSeriesSliceHolder(context, item.DocId, item.Name, item.Collection))
                        {
                            var info = intoReader.GetSegmentInfo();
                            var nextStart = info.Baseline > rollupStart ? info.Baseline : nextRollup;

                            tss.Rollups.MarkForPolicy(context, slicer, policy, info.Etag, nextStart, info.ChangeVector);
                        }
                    }
                }

                return RolledUp;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                throw new NotImplementedException();
            }
        }

        private static long NextRollup(DateTime baseline, TimeSeriesPolicy nextPolicy)
        {
            var integerPart = baseline.Ticks / nextPolicy.AggregationTime.Ticks;
            var nextRollup = nextPolicy.AggregationTime.Ticks * (integerPart + 1);
            return nextRollup;
        }
    }
}

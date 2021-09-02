using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.TimeSeries
{
    public class TimeSeriesRollups
    {
        private readonly DocumentDatabase _database;
        private static readonly TableSchema RollupSchema;
        public static readonly Slice TimeSeriesRollupTable;
        private static readonly Slice RollupKey;
        private static readonly Slice NextRollupIndex;
        private enum RollupColumns
        {
            // documentId/Name
            Key,
            Collection,
            NextRollup,
            PolicyToApply,
            Etag,
            ChangeVector
        }

        internal class RollupState
        {
            public Slice Key;
            public string DocId;
            public string Name;
            public long Etag;
            public string Collection;
            public DateTime NextRollup;
            public string RollupPolicy;
            public string ChangeVector;

            public override string ToString()
            {
                return $"Rollup for time-series '{Name}' in document '{DocId}' of policy {RollupPolicy} at {NextRollup}";
            }
        }

        static TimeSeriesRollups()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, nameof(TimeSeriesRollupTable), ByteStringType.Immutable, out TimeSeriesRollupTable);
                Slice.From(ctx, nameof(RollupKey), ByteStringType.Immutable, out RollupKey);
                Slice.From(ctx, nameof(NextRollupIndex), ByteStringType.Immutable, out NextRollupIndex);
            }

            RollupSchema = new TableSchema();
            RollupSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)RollupColumns.Key,
                Count = 1, 
                Name = RollupKey
            });

            RollupSchema.DefineIndex(new TableSchema.SchemaIndexDef // this isn't fixed-size since we expect to have duplicates
            {
                StartIndex = (int)RollupColumns.NextRollup, 
                Count = 1,
                Name = NextRollupIndex
            });
        }

        private readonly Logger _logger;

        public TimeSeriesRollups(DocumentDatabase database)
        {
            _database = database;
            _logger = LoggingSource.Instance.GetLogger<TimeSeriesPolicyRunner>(database.Name);
        }

        public unsafe void MarkForPolicy(DocumentsOperationContext context, TimeSeriesSliceHolder slicerHolder, TimeSeriesPolicy nextPolicy, DateTime timestamp)
        {
            var nextRollup = NextRollup(timestamp, nextPolicy);

            // mark for rollup
            RollupSchema.Create(context.Transaction.InnerTransaction, TimeSeriesRollupTable, 16);
            var table = context.Transaction.InnerTransaction.OpenTable(RollupSchema, TimeSeriesRollupTable);
            using (table.Allocate(out var tvb))
            using (Slice.From(context.Allocator, nextPolicy.Name, ByteStringType.Immutable, out var policyToApply))
            {
                if (table.ReadByKey(slicerHolder.StatsKey, out var tvr))
                {
                    // check if we need to update this
                    var existingRollup = Bits.SwapBytes(*(long*)tvr.Read((int)RollupColumns.NextRollup, out _));
                    if (existingRollup <= nextRollup)
                        return; // we have an earlier date to roll up from
                }

                if (_logger.IsInfoEnabled)
                    _logger.Info(
                        $"Marking {slicerHolder.Name} in document {slicerHolder.DocId} for policy {nextPolicy.Name} to rollup at {new DateTime(nextRollup)} (ticks:{nextRollup})");

                var etag = context.DocumentDatabase.DocumentsStorage.GenerateNextEtag();
                var changeVector = context.DocumentDatabase.DocumentsStorage.GetNewChangeVector(context, etag);
                using (Slice.From(context.Allocator, changeVector, ByteStringType.Immutable, out var changeVectorSlice))
                {
                    tvb.Add(slicerHolder.StatsKey);
                    tvb.Add(slicerHolder.CollectionSlice);
                    tvb.Add(Bits.SwapBytes(nextRollup));
                    tvb.Add(policyToApply);
                    tvb.Add(etag);
                    tvb.Add(changeVectorSlice);

                    table.Set(tvb);
                }
            }
        }

        public unsafe void MarkSegmentForPolicy(DocumentsOperationContext context, TimeSeriesSliceHolder slicerHolder, TimeSeriesPolicy nextPolicy, DateTime timestamp, string changeVector)
        {
            var nextRollup = NextRollup(timestamp, nextPolicy);
           
            // mark for rollup
            RollupSchema.Create(context.Transaction.InnerTransaction, TimeSeriesRollupTable, 16);
            var table = context.Transaction.InnerTransaction.OpenTable(RollupSchema, TimeSeriesRollupTable);
            using (table.Allocate(out var tvb))
            using (Slice.From(context.Allocator, nextPolicy.Name, ByteStringType.Immutable, out var policyToApply))
            {
                if (table.ReadByKey(slicerHolder.StatsKey, out var tvr))
                {
                    // check if we need to update this
                    var existingRollup = Bits.SwapBytes(*(long*)tvr.Read((int)RollupColumns.NextRollup, out _));
                    if (existingRollup < nextRollup)
                        return; // we have an earlier date to roll up from
                }

                if (_logger.IsInfoEnabled)
                    _logger.Info(
                        $"Marking segment of {slicerHolder.Name} in document {slicerHolder.DocId} for policy {nextPolicy.Name} to rollup at {new DateTime(nextRollup)} (ticks:{nextRollup})");

                var etag = context.DocumentDatabase.DocumentsStorage.GenerateNextEtag();
                using (Slice.From(context.Allocator, changeVector, ByteStringType.Immutable, out var changeVectorSlice))
                {
                    tvb.Add(slicerHolder.StatsKey);
                    tvb.Add(slicerHolder.CollectionSlice);
                    tvb.Add(Bits.SwapBytes(nextRollup));
                    tvb.Add(policyToApply);
                    tvb.Add(etag);
                    tvb.Add(changeVectorSlice);

                    table.Set(tvb);
                }
            }
        }

        public unsafe bool HasPendingRollupFrom(DocumentsOperationContext context, Slice key, DateTime time)
        {
            var t = TimeSeriesStorage.EnsureMillisecondsPrecision(time);

            var table = context.Transaction.InnerTransaction.OpenTable(RollupSchema, TimeSeriesRollupTable);
            if (table == null)
                return false;

            if (table.ReadByKey(key, out var tvr) == false)
                return false;

            var existingRollup = Bits.SwapBytes(*(long*)tvr.Read((int)RollupColumns.NextRollup, out _));
            return existingRollup <= t.Ticks;
        }

        internal void PrepareRollups(DocumentsOperationContext context, DateTime currentTime, long take, long start, List<RollupState> states, out Stopwatch duration)
        {
            duration = Stopwatch.StartNew();

            var table = context.Transaction.InnerTransaction.OpenTable(RollupSchema, TimeSeriesRollupTable);
            if (table == null)
                return;

            var currentTicks = currentTime.Ticks;

            using (DocumentsStorage.GetEtagAsSlice(context, start, out var startSlice))
            {
                foreach (var item in table.SeekForwardFrom(RollupSchema.Indexes[NextRollupIndex], startSlice, 0))
                {
                    if (take <= 0)
                        return;
                    
                    var rollUpTime = DocumentsStorage.TableValueToEtag((int)RollupColumns.NextRollup, ref item.Result.Reader);
                    if (rollUpTime > currentTicks)
                        return;

                    DocumentsStorage.TableValueToSlice(context, (int)RollupColumns.Key, ref item.Result.Reader, out var key);
                    SplitKey(key, out var docId, out var name);
                    name = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage.GetOriginalName(context, docId, name);

                    var state = new RollupState
                    {
                        Key = key,
                        DocId = docId,
                        Name = name,
                        Collection = DocumentsStorage.TableValueToId(context, (int)RollupColumns.Collection, ref item.Result.Reader),
                        NextRollup = new DateTime(rollUpTime),
                        RollupPolicy = DocumentsStorage.TableValueToString(context, (int)RollupColumns.PolicyToApply, ref item.Result.Reader),
                        Etag = DocumentsStorage.TableValueToLong((int)RollupColumns.Etag, ref item.Result.Reader),
                        ChangeVector = DocumentsStorage.TableValueToChangeVector(context, (int)RollupColumns.ChangeVector, ref item.Result.Reader)
                    };

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"{state} is prepared.");

                    states.Add(state);
                    take--;
                }
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
            private readonly string _collection;
            private readonly DateTime _to;

            public TimeSeriesRetentionCommand(List<Slice> keys, string collection, DateTime to)
            {
                _keys = keys;
                _collection = collection;
                _to = to;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var logger = LoggingSource.Instance.GetLogger<TimeSeriesPolicyRunner>(context.DocumentDatabase.Name);
                var request = new TimeSeriesStorage.DeletionRangeRequest
                {
                    From = DateTime.MinValue,
                    To = _to,
                    Collection = _collection
                };

                var retained = 0;
                foreach (var key in _keys)
                {
                    SplitKey(key, out var docId, out var name);
                         
                    request.Name = name;
                    request.DocumentId = docId;

                    var done = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage.DeleteTimestampRange(context, request) != null;
                    if (done)
                        retained++;

                    if (logger.IsInfoEnabled)
                        logger.Info($"{request} was executed (successfully: {done})");
                }

                return retained;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new TimeSeriesRetentionCommandDto(_keys, _collection, _to);
            }

            public class TimeSeriesRetentionCommandDto : TransactionOperationsMerger.IReplayableCommandDto<TimeSeriesRetentionCommand>
            {
                public List<Slice> _keys;
                public string _collection;
                public DateTime _to;

                public TimeSeriesRetentionCommandDto(List<Slice> keys, string collection, DateTime to)
                {
                    _keys = keys;
                    _collection = collection;
                    _to = to;
                }
                public TimeSeriesRetentionCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
                {
                    var keys = new List<Slice>();
                    foreach (var key in _keys)
                    {
                        keys.Add(key.Clone(context.Allocator));
                    }

                    return new TimeSeriesRetentionCommand(keys, _collection, _to);
                }
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
                RollupSchema.Create(context.Transaction.InnerTransaction, TimeSeriesRollupTable, 16);
                var table = context.Transaction.InnerTransaction.OpenTable(RollupSchema, TimeSeriesRollupTable);
                foreach (var key in tss.Stats.GetTimeSeriesNameByPolicy(context, _collection, _from.Name, _skip, BatchSize))
                {
                    using (table.Allocate(out var tvb))
                    using (DocumentIdWorker.GetStringPreserveCase(context, _collection.Name, out var collectionSlice))
                    using (Slice.From(context.Allocator, _to.Name, ByteStringType.Immutable, out var policyToApply))
                    using (Slice.From(context.Allocator, string.Empty, ByteStringType.Immutable, out var changeVectorSlice))
                    {
                        tvb.Add(key);
                        tvb.Add(collectionSlice);
                        tvb.Add(Bits.SwapBytes(NextRollup(DateTime.MinValue, _to)));
                        tvb.Add(policyToApply);
                        tvb.Add(0L);
                        tvb.Add(changeVectorSlice);

                        table.Set(tvb);
                    }

                    Marked++;
                }

                return Marked;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new AddedNewRollupPoliciesCommandDto(_collection, _from, _to, _skip);
            }

            public class AddedNewRollupPoliciesCommandDto : TransactionOperationsMerger.IReplayableCommandDto<AddedNewRollupPoliciesCommand>
            {
                public CollectionName _name;
                public TimeSeriesPolicy _from;
                public TimeSeriesPolicy _to;
                public int _skip;

                public AddedNewRollupPoliciesCommandDto(CollectionName name, TimeSeriesPolicy from, TimeSeriesPolicy to, int skip)
                {
                    _name = name;
                    _from = @from;
                    _to = to;
                    _skip = skip;
                }

                public AddedNewRollupPoliciesCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
                {
                    return new AddedNewRollupPoliciesCommand(_name, _from, _to, _skip);
                }
            }
        }

        public void DeleteByPrimaryKeyPrefix(DocumentsOperationContext context, Slice prefix)
        {
            RollupSchema.Create(context.Transaction.InnerTransaction, TimeSeriesRollupTable, 16);
            var table = context.Transaction.InnerTransaction.OpenTable(RollupSchema, TimeSeriesRollupTable);
            table.DeleteByPrimaryKeyPrefix(prefix);
        }

        internal class RollupTimeSeriesCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly TimeSeriesConfiguration _configuration;
            private readonly DateTime _now;
            private readonly List<RollupState> _states;
            private readonly bool _isFirstInTopology;
            private readonly Logger _logger;

            public long RolledUp;

            internal RollupTimeSeriesCommand(TimeSeriesConfiguration configuration, DateTime now, List<RollupState> states, bool isFirstInTopology)
            {
                _configuration = configuration;
                _now = now;
                _states = states;
                _isFirstInTopology = isFirstInTopology;
                _logger = LoggingSource.Instance.GetLogger<TimeSeriesRollups>(nameof(RollupTimeSeriesCommand));
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var storage = context.DocumentDatabase.DocumentsStorage;
                RollupSchema.Create(context.Transaction.InnerTransaction, TimeSeriesRollupTable, 16);
                var table = context.Transaction.InnerTransaction.OpenTable(RollupSchema, TimeSeriesRollupTable);
                foreach (var item in _states)
                {
                    if (_configuration == null)
                        return RolledUp;

                    if (_configuration.Collections.TryGetValue(item.Collection, out var config) == false)
                        continue;

                    if (config.Disabled)
                        continue;
                        
                    if (table.ReadByKey(item.Key, out var current) == false)
                        continue;

                    var policy = config.GetPolicyByName(item.RollupPolicy, out _);
                    if (policy == null)
                    {
                        table.DeleteByKey(item.Key);
                        continue;
                    }

                    if (item.Etag != DocumentsStorage.TableValueToLong((int)RollupColumns.Etag, ref current))
                        continue; // concurrency check

                    try
                    {
                        RollupOne(context, table, item, policy, config);
                    }
                    catch (NanValueException e)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"{item} failed", e);

                        if (table.VerifyKeyExists(item.Key) == false)
                        {
                            // we should re-add it, in case we already removed this rollup
                            using (var slicer = new TimeSeriesSliceHolder(context, item.DocId, item.Name, item.Collection))
                            using (Slice.From(context.Allocator, item.ChangeVector, ByteStringType.Immutable, out var cv))
                            using (Slice.From(context.Allocator, item.RollupPolicy, ByteStringType.Immutable, out var policySlice))
                            using (table.Allocate(out var tvb))
                            {
                                tvb.Add(slicer.StatsKey);
                                tvb.Add(slicer.CollectionSlice);
                                tvb.Add(Bits.SwapBytes(item.NextRollup.Ticks));
                                tvb.Add(policySlice);
                                tvb.Add(item.Etag);
                                tvb.Add(cv);

                                table.Set(tvb);
                            }
                        }
                    }
                    catch (RollupExceedNumberOfValuesException e)
                    {
                        var name = item.Name;
                        var docId = item.DocId;
                        try
                        {
                            var document = storage.Get(context, item.DocId, throwOnConflict: false);
                            docId = document?.Id ?? docId;
                            name = storage.TimeSeriesStorage.GetOriginalName(context, docId, name);
                        }
                        catch
                        {
                            // ignore
                        }

                        var msg = $"Rollup '{item.RollupPolicy}' for time-series '{name}' in document '{docId}' failed.";
                        if (_logger.IsInfoEnabled)
                            _logger.Info(msg, e);

                        var alert = AlertRaised.Create(context.DocumentDatabase.Name, "Failed to perform rollup because the time-series has more than 5 values", msg,
                            AlertType.RollupExceedNumberOfValues, NotificationSeverity.Warning, $"{item.Collection}/{item.Name}", new ExceptionDetails(e));

                        context.DocumentDatabase.NotificationCenter.Add(alert);
                    }
                }

                return RolledUp;
            }

            private void RollupOne(DocumentsOperationContext context, Table table, RollupState item, TimeSeriesPolicy policy, TimeSeriesCollectionConfiguration config)
            {
                var tss = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage;

                var rawTimeSeries = item.Name.Split(TimeSeriesConfiguration.TimeSeriesRollupSeparator)[0];
                var intoTimeSeries = policy.GetTimeSeriesName(rawTimeSeries);
                var rollupStart = item.NextRollup.Add(-policy.AggregationTime);

                if (config.MaxRetention < TimeValue.MaxValue)
                {
                    var next = new DateTime(NextRollup(_now.Add(-config.MaxRetention), policy)).Add(-policy.AggregationTime);
                    var rollupStartTicks = Math.Max(rollupStart.Ticks, next.Ticks);
                    rollupStart = new DateTime(rollupStartTicks);
                }

                var intoReader = tss.GetReader(context, item.DocId, intoTimeSeries, rollupStart, DateTime.MaxValue);
                var lastAggregated = intoReader.Last();
                var previouslyAggregated = lastAggregated != null;
                DateTime rollupEnd;
                if (previouslyAggregated)
                {
                    var changeVector = intoReader.GetCurrentSegmentChangeVector();

                    if (ChangeVectorUtils.GetConflictStatus(item.ChangeVector, changeVector) == ConflictStatus.AlreadyMerged)
                    {
                        // this rollup is already done
                        rollupEnd = new DateTime(NextRollup(lastAggregated.Timestamp, policy)).AddMilliseconds(-1);
                        MarkForNextPolicyAfterRollup(context, table, item, policy, tss, rollupEnd);
                        return;
                    }
                }

                if (_isFirstInTopology == false)
                    return;
                rollupEnd = new DateTime(NextRollup(_now, policy)).Add(-policy.AggregationTime).AddMilliseconds(-1);
                var reader = tss.GetReader(context, item.DocId, item.Name, rollupStart, rollupEnd);

                if (previouslyAggregated)
                {
                    var hasPriorValues = tss.GetReader(context, item.DocId, item.Name, DateTime.MinValue, rollupStart).AllValues().Any();
                    if (hasPriorValues == false)
                    {
                        table.DeleteByKey(item.Key);
                        var first = tss.GetReader(context, item.DocId, item.Name, rollupStart, DateTime.MaxValue).First();

                        if (first == default)
                            return;
                        if (first.Timestamp > item.NextRollup)
                        {
                            // if the 'source' time-series doesn't have any values it is retained.
                            // so we need to aggregate only from the next time frame
                            using (var slicer = new TimeSeriesSliceHolder(context, item.DocId, item.Name, item.Collection))
                            {
                                tss.Rollups.MarkForPolicy(context, slicer, policy, first.Timestamp);
                            }
                            return;
                        }
                    }
                }

                // rollup from the the raw data will generate 6-value roll up of (first, last, min, max, sum, count)
                // other rollups will aggregate each of those values by the type
                var mode = item.Name.Contains(TimeSeriesConfiguration.TimeSeriesRollupSeparator) ? AggregationMode.FromAggregated : AggregationMode.FromRaw;
                var rangeSpec = new RangeGroup();
                switch (policy.AggregationTime.Unit)
                {
                    case TimeValueUnit.Second:
                        rangeSpec.Ticks = TimeSpan.FromSeconds(policy.AggregationTime.Value).Ticks;
                        rangeSpec.TicksAlignment = RangeGroup.Alignment.Second;
                        break;
                    case TimeValueUnit.Month:
                        rangeSpec.Months = policy.AggregationTime.Value;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(policy.AggregationTime.Unit), $"Not supported time value unit '{policy.AggregationTime.Unit}'");
                }

                rangeSpec.InitializeRange(rollupStart);

                var values = GetAggregatedValues(reader, rangeSpec, mode);

                if (previouslyAggregated)
                {
                    // if we need to re-aggregate we need to delete everything we have from that point on.  
                    var removeRequest = new TimeSeriesStorage.DeletionRangeRequest
                    {
                        Collection = item.Collection,
                        DocumentId = item.DocId,
                        Name = intoTimeSeries,
                        From = rollupStart,
                        To = DateTime.MaxValue,
                    };
                    tss.DeleteTimestampRange(context, removeRequest);
                }

                var before = context.LastDatabaseChangeVector;
                var after = tss.AppendTimestamp(context, item.DocId, item.Collection, intoTimeSeries, values, verifyName: false);
                if (before != after)
                    RolledUp++;
                MarkForNextPolicyAfterRollup(context, table, item, policy, tss, rollupEnd);
            }

            private static void MarkForNextPolicyAfterRollup(DocumentsOperationContext context, Table table, RollupState item, TimeSeriesPolicy policy, TimeSeriesStorage tss,
                DateTime rollupEnd)
            {
                table.DeleteByKey(item.Key);
                (long Count, DateTime Start, DateTime End) stats = tss.Stats.GetStats(context, item.DocId, item.Name);

                if (stats.End > rollupEnd)
                {
                    // we know that we have values after the current rollup and we need to mark them
                    var nextRollup = rollupEnd.AddMilliseconds(1);
                    TimeSeriesReader intoReader = tss.GetReader(context, item.DocId, item.Name, nextRollup, DateTime.MaxValue);
                    if (intoReader.Init() == false)
                    {
                        Debug.Assert(false, "We have values but no segment?");
                        return;
                    }

                    using (var slicer = new TimeSeriesSliceHolder(context, item.DocId, item.Name, item.Collection))
                    {
                        tss.Rollups.MarkForPolicy(context, slicer, policy, intoReader.First().Timestamp);
                    }
                }
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new RollupTimeSeriesCommandDto(_configuration, _now, _states, _isFirstInTopology);
            }

            public class RollupTimeSeriesCommandDto : TransactionOperationsMerger.IReplayableCommandDto<RollupTimeSeriesCommand>
            {
                public TimeSeriesConfiguration _configuration;
                public DateTime _now;
                public List<RollupState> _states;
                public bool _isFirstInTopology;

                public RollupTimeSeriesCommandDto(TimeSeriesConfiguration configuration, DateTime now, List<RollupState> states, bool isFirstInTopology)
                {
                    _configuration = configuration;
                    _now = now;
                    _states = states;
                    _isFirstInTopology = isFirstInTopology;
                }

                public RollupTimeSeriesCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
                {
                    return new RollupTimeSeriesCommand(_configuration, _now, _states, _isFirstInTopology);
                }
            }
        }

        public enum AggregationMode
        {
            FromRaw,
            FromAggregated
        }

        private static readonly AggregationType[] Aggregations = 
        {
            // the order here matters, and should match TimeSeriesAggregation.AggregationType
            AggregationType.First,
            AggregationType.Last,
            AggregationType.Min,
            AggregationType.Max,
            AggregationType.Sum,
            AggregationType.Count
        };

        public readonly struct TimeSeriesAggregation
        {
            private readonly AggregationMode _mode;
            public bool Any => Values.Count > 0;

            public readonly List<double> Values;

            public TimeSeriesAggregation(AggregationMode mode)
            {
                _mode = mode;
                Values = new List<double>();
            }

            public void Init()
            {
                Values.Clear();
            }

            public void Segment(Span<StatefulTimestampValue> values)
            {
                EnsureNumberOfValues(values.Length);

                for (int i = 0; i < values.Length; i++)
                {
                    var val = values[i];
                    switch (_mode)
                    {
                        case AggregationMode.FromRaw:
                            for (var index = 0; index < Aggregations.Length; index++)
                            {
                                var aggregation = Aggregations[index];
                                var aggIndex = index + (i * Aggregations.Length);
                                AggregateOnceBySegment(aggregation, aggIndex, val, _mode);
                            }

                            break;
                        case AggregationMode.FromAggregated:
                            {
                                var aggIndex = i % Aggregations.Length;
                                var aggType = Aggregations[aggIndex];
                                AggregateOnceBySegment(aggType, i, val, _mode);
                            }
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }

            private void AggregateOnceBySegment(AggregationType aggregation, int i, StatefulTimestampValue val, AggregationMode mode)
            {
                switch (aggregation)
                {
                    case AggregationType.Min:
                        if (double.IsNaN(Values[i]))
                            Values[i] = val.Min;
                        else
                            Values[i] = Math.Min(Values[i], val.Min);
                        break;
                    case AggregationType.Max:
                        if (double.IsNaN(Values[i]))
                            Values[i] = val.Max;
                        else
                            Values[i] = Math.Max(Values[i], val.Max);
                        break;
                    case AggregationType.Sum:
                    case AggregationType.Average:
                        if (double.IsNaN(Values[i]))
                            Values[i] = 0;
                        Values[i] = Values[i] + val.Sum;
                        break;
                    case AggregationType.First:
                        if (double.IsNaN(Values[i]))
                            Values[i] = val.First;
                        break;
                    case AggregationType.Last:
                        Values[i] = val.Last;
                        break;
                    case AggregationType.Count:
                        if (double.IsNaN(Values[i]))
                            Values[i] = 0;
                        if (mode == AggregationMode.FromAggregated)
                            Values[i] += val.Sum;
                        else
                            Values[i] += val.Count;
                        
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("Unknown aggregation operation: " + aggregation);
                }
            }

            private void AggregateOnceByItem(AggregationType aggregation, int i, double val, AggregationMode mode)
            {
                switch (aggregation)
                {
                    case AggregationType.Min:
                        if (double.IsNaN(Values[i]))
                            Values[i] = val;
                        else
                            Values[i] = Math.Min(Values[i], val);
                        break;
                    case AggregationType.Max:
                        if (double.IsNaN(Values[i]))
                            Values[i] = val;
                        else
                            Values[i] = Math.Max(Values[i], val);
                        break;
                    case AggregationType.Sum:
                    case AggregationType.Average:
                        if (double.IsNaN(Values[i]))
                            Values[i] = 0;
                        Values[i] = Values[i] + val;
                        break;
                    case AggregationType.First:
                        if (double.IsNaN(Values[i]))
                            Values[i] = val;
                        break;
                    case AggregationType.Last:
                        Values[i] = val;
                        break;
                    case AggregationType.Count:
                        if (double.IsNaN(Values[i]))
                            Values[i] = 0;

                        if (mode == AggregationMode.FromAggregated)
                            Values[i] += val;
                        else
                            Values[i]++;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException("Unknown aggregation operation: " + aggregation);
                }
            }

            public void Step(Span<double> values)
            {
                EnsureNumberOfValues(values.Length);
                
                for (int i = 0; i < values.Length; i++)
                {
                    var val = values[i];
                    switch (_mode)
                    {
                        case AggregationMode.FromRaw:
                            for (var index = 0; index < Aggregations.Length; index++)
                            {
                                var aggregation = Aggregations[index];
                                var aggIndex = index + (i * Aggregations.Length);
                                AggregateOnceByItem(aggregation, aggIndex, val, _mode);
                            }

                            break;
                        case AggregationMode.FromAggregated:
                            {
                                var aggIndex = i % Aggregations.Length;
                                var type = Aggregations[aggIndex];
                                AggregateOnceByItem(type, i, val, _mode);
                            }
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }

            private void EnsureNumberOfValues(int numberOfValues)
            {
                
                switch (_mode)
                {
                    case AggregationMode.FromRaw:
                        
                        if (numberOfValues > 5)
                            throw new RollupExceedNumberOfValuesException(
                                $"Rollup more than 5 values is not supported.{Environment.NewLine}" +
                                $"The number of values is {numberOfValues}, so an aggregated entry will contain {numberOfValues * 6} values, which will exceed the allowed 32 values per time-series entry.{Environment.NewLine}");

                        var entries = numberOfValues * Aggregations.Length;
                        for (int i = Values.Count; i < entries; i++)
                        {
                            Values.Add(double.NaN);
                        }

                        break;
                    case AggregationMode.FromAggregated:
                        for (int i = Values.Count; i < numberOfValues; i++)
                        {
                            Values.Add(double.NaN);
                        }

                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }

        public static List<SingleResult> GetAggregatedValues(TimeSeriesReader reader, RangeGroup rangeSpec, AggregationMode mode)
        {
            var aggStates = new TimeSeriesAggregation(mode); // we always will aggregate here by Min, Max, First, Last, Sum, Count, Mean
            var results = new List<SingleResult>();
           
            foreach (var it in reader.SegmentsOrValues())
            {
                if (it.IndividualValues != null)
                {
                    AggregateIndividualItems(it.IndividualValues);
                }
                else
                {
                    //We might need to close the old aggregation range and start a new one
                    MaybeMoveToNextRange(it.Segment.Start);

                    // now we need to see if we can consume the whole segment, or 
                    // if the range it cover needs to be broken up to multiple ranges.
                    // For example, if the segment covers 3 days, but we have group by 1 hour,
                    // we still have to deal with the individual values
                    if (it.Segment.End > rangeSpec.End)
                    {
                        AggregateIndividualItems(it.Segment.Values);
                    }
                    else
                    {
                        var span = it.Segment.Summary.SegmentValues.Span;
                        aggStates.Segment(span);
                    }
                }
            }

            if (aggStates.Any)
            {
                var result = new SingleResult
                {
                    Timestamp = rangeSpec.Start,
                    Values = new Memory<double>(aggStates.Values.ToArray()),
                    Status = TimeSeriesValuesSegment.Live,
                    Type = SingleResultType.RolledUp
                    // TODO: Tag = ""
                };
                TimeSeriesStorage.AssertNoNanValue(result);
                results.Add(result);
            }

            return results;

            void MaybeMoveToNextRange(DateTime ts)
            {
                if (rangeSpec.WithinRange(ts))
                    return;

                if (aggStates.Any)
                {
                    var result = new SingleResult
                    {
                        Timestamp = rangeSpec.Start,
                        Values = new Memory<double>(aggStates.Values.ToArray()),
                        Status = TimeSeriesValuesSegment.Live,
                        Type = SingleResultType.RolledUp
                        // TODO: Tag = ""
                    };
                    TimeSeriesStorage.AssertNoNanValue(result);
                    results.Add(result);
                }

                rangeSpec.MoveToNextRange(ts);
                aggStates.Init();
            }

            void AggregateIndividualItems(IEnumerable<SingleResult> items)
            {
                foreach (var cur in items)
                {
                    if (cur.Status == TimeSeriesValuesSegment.Dead)
                        continue;

                    MaybeMoveToNextRange(cur.Timestamp);
                    
                    aggStates.Step(cur.Values.Span);
                }
            }
        }
        public static long NextRollup(DateTime time, TimeSeriesPolicy nextPolicy)
        {
            if (time == DateTime.MinValue)
                return time.Add(nextPolicy.AggregationTime).Ticks;

            switch (nextPolicy.AggregationTime.Unit)
            {
                case TimeValueUnit.Second:
                    // align by seconds
                    var timespan = TimeSpan.FromSeconds(nextPolicy.AggregationTime.Value);
                    var integerPart = time.Ticks / timespan.Ticks;
                    var nextRollup = timespan.Ticks * (integerPart + 1);
                    return nextRollup;

                case TimeValueUnit.Month:
                    // align by months
                    var totalMonths = time.Year * 12 + time.Month - 1;
                    var integerAggPart = totalMonths / nextPolicy.AggregationTime.Value;
                    var nextInMonths = nextPolicy.AggregationTime.Value * (integerAggPart + 1);
                    var years = nextInMonths / 12;
                    var months = nextInMonths % 12;
                    return new DateTime(years, months + 1, 1).Ticks;

                default:
                    throw new ArgumentOutOfRangeException(nameof(nextPolicy.AggregationTime.Unit), $"Not supported time value unit '{nextPolicy.AggregationTime.Unit}'");
            }
        }

        public class RollupExceedNumberOfValuesException : Exception
        {
            public RollupExceedNumberOfValuesException()
            {
            }

            public RollupExceedNumberOfValuesException(string message) : base(message)
            {
            }

            public RollupExceedNumberOfValuesException(string message, Exception innerException) : base(message, innerException)
            {
            }
        }
    }
}

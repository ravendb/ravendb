using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.ServerWide;
using Raven.Server.Background;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.TimeSeries
{
    public class TimeSeriesPolicyRunner : BackgroundWorkBase
    {
        private readonly DocumentDatabase _database;

        public TimeSeriesConfiguration Configuration { get; }

        public TimeSeriesPolicyRunner(DocumentDatabase database, TimeSeriesConfiguration configuration) : base(database.Name, database.DatabaseShutdown)
        {
            _database = database;
            Configuration = configuration;

            if (configuration.Collections != null)
                Configuration.Collections =
                    new Dictionary<string, TimeSeriesCollectionConfiguration>(Configuration.Collections, StringComparer.InvariantCultureIgnoreCase);
        }

        public static TimeSeriesPolicyRunner LoadConfigurations(DocumentDatabase database, DatabaseRecord dbRecord, TimeSeriesPolicyRunner policyRunner)
        {
            try
            {
                if (dbRecord.TimeSeries?.Collections == null || dbRecord.TimeSeries.Collections.Count == 0)
                {
                    policyRunner?.Dispose();
                    return null;
                }

                if (policyRunner != null)
                {
                    // no changes
                    if (Equals(policyRunner.Configuration, dbRecord.TimeSeries))
                        return policyRunner;
                }
                policyRunner?.Dispose();
                var runner = new TimeSeriesPolicyRunner(database, dbRecord.TimeSeries);
                runner.Start();
                return runner;
            }
            catch (Exception e)
            {
                
                const string msg = "Cannot enable retention policy runner as the configuration record is not valid.";
                database.NotificationCenter.Add(AlertRaised.Create(
                    database.Name,
                    $"retention policy runner error in {database.Name}", msg,
                    AlertType.RevisionsConfigurationNotValid, NotificationSeverity.Error, database.Name));

                var logger = LoggingSource.Instance.GetLogger<TimeSeriesPolicyRunner>(database.Name);
                if (logger.IsOperationsEnabled)
                    logger.Operations(msg, e);

                try
                {
                    policyRunner?.Dispose();
                }
                catch (Exception ex)
                {
                    if (logger.IsOperationsEnabled)
                        logger.Operations("Failed to dispose previous time-series policy runner", ex);
                }

                return null;
            }
        }
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

        static TimeSeriesPolicyRunner()
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

        public unsafe void MarkForPolicy(DocumentsOperationContext context,TimeSeriesSliceHolder slicerHolder, string collection, string name, long etag, DateTime baseline, string changeVector)
        {
            if (Configuration.Collections.TryGetValue(collection, out var config) == false)
                return;

            if (config.Disabled)
                return;

            var current = config.GetPolicyIndexByTimeSeries(name);
            if (current == -1) // policy not found
                return;

            var nextPolicy = config.GetNextPolicy(current);
            if (nextPolicy == null)
                return;
            
            if (nextPolicy == TimeSeriesPolicy.AfterAllPolices)
                return; // this is the last policy

            var integerPart = baseline.Ticks / nextPolicy.AggregationTime.Ticks;
            var nextRollup = nextPolicy.AggregationTime.Ticks * (integerPart + 1);

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
       
        protected override async Task DoWork()
        {
            // this is explicitly outside the loop
            await HandleChanges();

            while (Cts.IsCancellationRequested == false)
            {
                await WaitOrThrowOperationCanceled(TimeSpan.FromSeconds(60));

                await RunRollUps();

                await DoRetention();
            }
        }

        internal async Task HandleChanges()
        {
            var policies = new List<(TimeSeriesPolicy Policy, int Index)>();

            foreach (var config in Configuration.Collections)
            {
                var collection = config.Key;
                var collectionName = _database.DocumentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
                if (collectionName == null)
                    continue;

                policies.Clear();
                for (int i = 0; i < config.Value.Policies.Count; i++)
                {
                    var p = config.Value.Policies[i];
                    policies.Add((p, i + 1));
                }

                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    List<Slice> currentPolicies;
                    using (context.OpenReadTransaction())
                    {
                        currentPolicies = _database.DocumentsStorage.TimeSeriesStorage.Stats.GetAllPolicies(context, collectionName).ToList();
                    }

                    foreach (var policySlice in currentPolicies)
                    {
                        var policyName = policySlice.ToString();
                        var policy = config.Value.GetPolicyByName(policyName, out var index);
                        if (policy == null)
                        {
                            await RemoveTimeSeriesByPolicy(collectionName, policyName);
                            continue;
                        }

                        policies.Remove((policy, index));
                    }

                    foreach (var policy in policies)
                    {
                        var prev = config.Value.GetPreviousPolicy(policy.Index);
                        if (prev == null || prev == TimeSeriesPolicy.BeforeAllPolices)
                            continue;

                        await AddNewPolicy(collectionName, prev, policy.Policy);
                    }
                }
            }
        }

        private async Task RemoveTimeSeriesByPolicy(CollectionName collectionName, string policyName)
        {
            while (true)
            {
                Cts.Token.ThrowIfCancellationRequested();

                var cmd = new RemovePoliciesCommand(collectionName, policyName);
                await _database.TxMerger.Enqueue(cmd);

                if (cmd.Deleted < RemovePoliciesCommand.BatchSize)
                    break;
            }
        }

        private async Task AddNewPolicy(CollectionName collectionName, TimeSeriesPolicy prev, TimeSeriesPolicy policy)
        {
            var skip = 0;
            while (true)
            {
                Cts.Token.ThrowIfCancellationRequested();

                var cmd = new AddedNewRollupPoliciesCommand(collectionName, prev, policy, skip);
                await _database.TxMerger.Enqueue(cmd);

                if (cmd.Marked < AddedNewRollupPoliciesCommand.BatchSize)
                    break;

                skip += cmd.Marked;
            }
        }

        internal async Task RunRollUps()
        {
            var now = DateTime.UtcNow;
            try
            {
                var states = new List<RollUpState>();
                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    while (true)
                    {
                        states.Clear();

                        context.Reset();
                        context.Renew();

                        Stopwatch duration;
                        using (context.OpenReadTransaction())
                        {
                            PrepareRollUps(context, now, 1024, states, out duration);
                            if (states.Count == 0)
                                return;
                        }

                        Cts.Token.ThrowIfCancellationRequested();

                        var topology = _database.ServerStore.LoadDatabaseTopology(_database.Name);
                        var isFirstInTopology = string.Equals(topology.Members.FirstOrDefault(), _database.ServerStore.NodeTag, StringComparison.OrdinalIgnoreCase);

                        var command = new RollupTimeSeriesCommand(Configuration, states, isFirstInTopology);
                        await _database.TxMerger.Enqueue(command);
                        if (command.RolledUp == 0)
                            break;

                        if (Logger.IsInfoEnabled)
                            Logger.Info($"Successfully aggregated {command.RolledUp:#,#;;0} time-series within {duration.ElapsedMilliseconds:#,#;;0} ms.");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // this will stop processing
                throw;
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                        Logger.Operations($"Failed to roll-up time series for '{_database.Name}' which are older than {now}", e);
            }
        }

        internal async Task DoRetention()
        {
            var topology = _database.ServerStore.LoadDatabaseTopology(_database.Name);
            var isFirstInTopology = string.Equals(topology.Members.FirstOrDefault(), _database.ServerStore.NodeTag, StringComparison.OrdinalIgnoreCase);
            if (isFirstInTopology == false)
                return;

            var now = DateTime.UtcNow;
            var configuration = Configuration.Collections;

            try
            {
                foreach (var collectionConfig in configuration)
                {
                    var collection = collectionConfig.Key;

                    var config = collectionConfig.Value;
                    if (config.Disabled)
                        continue;
                
                    using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        CollectionName collectionName;
                        using (context.OpenReadTransaction())
                        {
                            collectionName = _database.DocumentsStorage.ExtractCollectionName(context, collection);
                        }

                        var request = new TimeSeriesStorage.DeletionRangeRequest
                        {
                            Collection = collection,
                            From = DateTime.MinValue
                        };

                        await ApplyRetention(context, collectionName, config.RawPolicy, now, request);

                        foreach (var policy in config.Policies)
                        {
                            await ApplyRetention(context, collectionName, policy, now, request);
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // this will stop processing
                throw;
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Failed to execute time series retention for database '{_database.Name}'", e);
            }
        }

        private async Task ApplyRetention(DocumentsOperationContext context, CollectionName collectionName, TimeSeriesPolicy policy, DateTime now, TimeSeriesStorage.DeletionRangeRequest request)
        {
            var tss = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage;
            if (policy.RetentionTime == TimeSpan.MaxValue)
                return;

            var deleteFrom = now.Add(-policy.RetentionTime);

            while (true)
            {
                Cts.Token.ThrowIfCancellationRequested();

                context.Reset();
                context.Renew();

                using (context.OpenReadTransaction())
                {
                    var list = tss.Stats.GetTimeSeriesByPolicyFromStartDate(context, collectionName, policy.Name, deleteFrom, TimeSeriesRetentionCommand.BatchSize).ToList();
                    if (list.Count == 0)
                        return;

                    request.To = deleteFrom;
                    var cmd = new TimeSeriesRetentionCommand(list, request);
                    await _database.TxMerger.Enqueue(cmd);
                }
            }
        }

        private void PrepareRollUps(DocumentsOperationContext context, DateTime currentTime, long take, List<RollUpState> states, out Stopwatch duration)
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
            private readonly List<RollUpState> _states;
            private readonly bool _isFirstInTopology;

            public long RolledUp;

            internal RollupTimeSeriesCommand(TimeSeriesConfiguration configuration, List<RollUpState> states, bool isFirstInTopology)
            {
                _configuration = configuration;
                _states = states;
                _isFirstInTopology = isFirstInTopology;
            }
            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var tss = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage;
                RollUpSchema.Create(context.Transaction.InnerTransaction, TimeSeriesRollUps, 16);
                var table = context.Transaction.InnerTransaction.OpenTable(RollUpSchema, TimeSeriesRollUps);
                var toRemove = new List<Slice>();

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
                        toRemove.Add(item.Key);
                        continue;
                    }

                    if (item.Etag != DocumentsStorage.TableValueToLong((int)RollUpColumns.Etag, ref current))
                        continue; // concurrency check

                    var startFrom = item.NextRollUp.Add(-policy.AggregationTime);
                    var rawTimeSeries = item.Name.Split(TimeSeriesConfiguration.TimeSeriesRollupSeparator)[0];
                    var intoTimeSeries = policy.GetTimeSeriesName(rawTimeSeries);
                    
                    var intoReader = tss.GetReader(context, item.DocId, intoTimeSeries, startFrom, startFrom);
                    var previouslyAggregated = intoReader.AllValues().Any();
                    if (previouslyAggregated)
                    {
                        var changeVector = intoReader.GetCurrentSegmentChangeVector();
                        if (ChangeVectorUtils.GetConflictStatus(item.ChangeVector, changeVector) == ConflictStatus.AlreadyMerged)
                        {
                            // this rollup is already done
                            toRemove.Add(item.Key);
                            continue;
                        }
                    }

                    if (_isFirstInTopology == false)
                        continue; // we execute the actual rollup only on the primary node to avoid conflicts

                    var reader = tss.GetReader(context, item.DocId, item.Name, startFrom, DateTime.MaxValue);
                    var values = tss.GetAggregatedValues(reader, DateTime.MinValue, policy.AggregationTime, policy.Type);

                    if (previouslyAggregated)
                    {
                        // if we need to re-aggregate we need to delete everything we have from that point on.  
                        var removeRequest = new TimeSeriesStorage.DeletionRangeRequest
                        {
                            Collection = item.Collection,
                            DocumentId = item.DocId,
                            Name = intoTimeSeries,
                            From = startFrom,
                            To = DateTime.MaxValue
                        };

                        tss.RemoveTimestampRange(context, removeRequest);
                    }
                    
                    tss.AppendTimestamp(context, item.DocId, item.Collection, intoTimeSeries, values);

                    toRemove.Add(item.Key);
                    RolledUp++;
                }

                foreach (var item in toRemove)
                {
                    table.DeleteByKey(item);
                }

                return RolledUp;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                throw new NotImplementedException();
            }
        }
    }
}

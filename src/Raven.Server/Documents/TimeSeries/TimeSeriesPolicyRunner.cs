using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.ServerWide;
using Raven.Server.Background;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
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
        }

        public static TimeSeriesPolicyRunner LoadConfigurations(DocumentDatabase database, DatabaseRecord dbRecord, TimeSeriesPolicyRunner policyRunner)
        {
            try
            {
                if (dbRecord.TimeSeries == null)
                {
                    policyRunner?.Dispose();
                    return null;
                }

                if (policyRunner != null)
                {
                    // no changes
                    if (Equals(policyRunner.Configuration, dbRecord.TimeSeries))
                        return policyRunner;
                    //TODO: when the policy changed, we need to delete the not relevant ones
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

                return null;
            }
        }

        private static readonly TableSchema RollUpSchema;
        private static readonly Slice TimeSeriesRollUps;
        private static readonly Slice Key;
        private static readonly Slice NextRollUp;
        private enum RollUpColumn
        {
            // documentId/Name
            Key,
            Collection,
            NextRollUp,
            PolicyToApply,
            Etag
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
        }

        static TimeSeriesPolicyRunner()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "TimeSeriesRollUps", ByteStringType.Immutable, out TimeSeriesRollUps);
                Slice.From(ctx, "Key", ByteStringType.Immutable, out Key);
                Slice.From(ctx, "NextRollUp", ByteStringType.Immutable, out NextRollUp);
            }

            RollUpSchema = new TableSchema();
            RollUpSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = (int)RollUpColumn.Key,
                Count = 1, 
                Name = Key
            });

            RollUpSchema.DefineIndex(new TableSchema.SchemaIndexDef // this isn't fixed-size since we expect to have duplicates
            {
                StartIndex = (int)RollUpColumn.NextRollUp, 
                Count = 1,
                Name = NextRollUp
            });
        }

        public unsafe void MarkForPolicy(DocumentsOperationContext context, Slice key, string collection, string name, long etag, DateTime baseline)
        {
            var config = Configuration.Collections[collection];

            if (config.Disabled)
                return;

            var current = config.GetPolicy(name);
            if (current == null) // policy not found
                // TODO: delete this timeseries if not the raw one?
                return;

            var nextPolicy = config.GetNextPolicy(current);
            if (nextPolicy == null)
            {
                Debug.Assert(false,"shouldn't happened, this mean the current policy doesn't exists");
                return;
            }
            
            if (nextPolicy == RollupPolicy.AfterAllPolices)
                return; // this is the last policy

            var integerPart = baseline.Ticks / nextPolicy.AggregationTime.Ticks;
            var nextRollup = nextPolicy.AggregationTime.Ticks * (integerPart + 1);

            var raw = name.Split(TimeSeriesConfiguration.TimeSeriesRollupSeparator)[0];

            // mark for rollup
            RollUpSchema.Create(context.Transaction.InnerTransaction, TimeSeriesRollUps, 16);
            var table = context.Transaction.InnerTransaction.OpenTable(RollUpSchema, TimeSeriesRollUps);
            using (table.Allocate(out var tvb))
            using (Slice.From(context.Allocator, key.Content.Ptr, key.Size - sizeof(long), ByteStringType.Immutable, out var prefix))
            using (Slice.From(context.Allocator,collection, ByteStringType.Immutable,out var collectionSlice))
            using (Slice.From(context.Allocator, nextPolicy.GetTimeSeriesName(raw), ByteStringType.Immutable, out var policyToApply))
            {
                if (table.ReadByKey(prefix, out var tvr))
                {
                    // check if we need to update this
                    var existingRollup = Bits.SwapBytes(*(long*)tvr.Read((int)RollUpColumn.NextRollUp, out _));
                    if (existingRollup <= nextRollup)
                        return; // we have an earlier date to roll up from
                }

                tvb.Add(prefix);
                tvb.Add(collectionSlice);
                tvb.Add(Bits.SwapBytes(nextRollup));
                tvb.Add(policyToApply);
                tvb.Add(etag);

                table.Set(tvb);
            }
        }
       
        protected override async Task DoWork()
        {
            while (Cts.IsCancellationRequested == false)
            {
                await WaitOrThrowOperationCanceled(TimeSpan.FromSeconds(60));

                await RunRollUps();
            }
        }

        internal async Task RunRollUps()
        {
            var now = DateTime.UtcNow;
            try
            {
                DatabaseTopology topology;
                using (_database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
                using (serverContext.OpenReadTransaction())
                {
                    topology = _database.ServerStore.Cluster.ReadDatabaseTopology(serverContext, _database.Name);
                }
                var isFirstInTopology = string.Equals(topology.AllNodes.FirstOrDefault(), _database.ServerStore.NodeTag, StringComparison.OrdinalIgnoreCase);

                var states = new List<RollUpState>();

                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    while (true)
                    {
                        context.Reset();
                        context.Renew();

                        using (context.OpenReadTransaction())
                        {
                            GetRollUps(context, now, 1024, states, out var duration);
                            if (states.Count == 0)
                                return;

                            var command = new RollupTimeSeriesCommand(Configuration, states);
                            await _database.TxMerger.Enqueue(command);
                            if (command.RolledUp == 0)
                                break;

                            states.Clear();

                            if (Logger.IsInfoEnabled)
                                    Logger.Info($"Successfully aggregated {command.RolledUp:#,#;;0} time-series within {duration.ElapsedMilliseconds:#,#;;0} ms.");
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
                        Logger.Operations($"Failed to roll-up time series on {_database.Name} which are older than {now}", e);
            }
        }

        private void GetRollUps(DocumentsOperationContext context, DateTime currentTime, long take, List<RollUpState> states, out Stopwatch duration)
        {
            RollUpSchema.Create(context.Transaction.InnerTransaction, TimeSeriesRollUps, 16);
            var table = context.Transaction.InnerTransaction.OpenTable(RollUpSchema, TimeSeriesRollUps);
            var currentTicks = currentTime.Ticks;

            duration = Stopwatch.StartNew();
            var keys = new Dictionary<Slice, long>();
            foreach (var item in table.SeekForwardFrom(RollUpSchema.Indexes[NextRollUp], Slices.BeforeAllKeys, 0))
            {
                if (take <= 0)
                    return;

                var rollUpTime = DocumentsStorage.TableValueToEtag((int)RollUpColumn.NextRollUp, ref item.Result.Reader);
                if (rollUpTime > currentTicks)
                    return;

                DocumentsStorage.TableValueToSlice(context, (int)RollUpColumn.Key, ref item.Result.Reader,out var key);
                TimeSeriesValuesSegment.ParseTimeSeriesKey(key, context, out var docId, out var name);
                var state = new RollUpState
                {
                    Key = key,
                    DocId = docId,
                    Name = name,
                    Collection = DocumentsStorage.TableValueToString(context, (int)RollUpColumn.Collection, ref item.Result.Reader),
                    NextRollUp = new DateTime(rollUpTime),
                    RollUpPolicy = DocumentsStorage.TableValueToString(context, (int)RollUpColumn.PolicyToApply, ref item.Result.Reader),
                    Etag = DocumentsStorage.TableValueToLong((int)RollUpColumn.Etag, ref item.Result.Reader)
                };

                states.Add(state);
                take--;
            }
        }

        internal class RollupTimeSeriesCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly TimeSeriesConfiguration _configuration;
            private readonly List<RollUpState> _states;

            public long RolledUp;

            internal RollupTimeSeriesCommand(TimeSeriesConfiguration configuration, List<RollUpState> states)
            {
                _configuration = configuration;
                _states = states;
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

                    var config = _configuration.Collections[item.Collection];
                    if (config.Disabled)
                        continue;
                        
                    var policy = config.GetPolicy(item.RollUpPolicy);
                    if (policy == null)
                        continue;

                    if (table.ReadByKey(item.Key, out var current) == false)
                    {
                        toRemove.Add(item.Key);
                        continue;
                    }

                    if (item.Etag != DocumentsStorage.TableValueToLong((int)RollUpColumn.Etag, ref current))
                        continue; // concurrency check

                    RolledUp++;
                    var startFrom = item.NextRollUp.Add(-policy.AggregationTime);
                    var reader = tss.GetReader(context, item.DocId, item.Name, startFrom, DateTime.MaxValue);
                    var values = tss.GetAggregatedValues(reader, DateTime.MinValue, policy.AggregationTime, policy.Type);
                    var rawName = item.Name.Split(TimeSeriesConfiguration.TimeSeriesRollupSeparator)[0];
                    // TODO: remove everything after values
                    tss.AppendTimestamp(context, item.DocId, item.Collection, policy.GetTimeSeriesName(rawName), values);

                    toRemove.Add(item.Key);
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

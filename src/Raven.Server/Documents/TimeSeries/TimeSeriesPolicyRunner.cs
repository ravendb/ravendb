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
using Sparrow.Logging;

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

            Configuration.Initialize();
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

        protected override async Task DoWork()
        {
            // this is explicitly outside the loop
            await HandleChanges();

            while (Cts.IsCancellationRequested == false)
            {
                await WaitOrThrowOperationCanceled(TimeSpan.FromSeconds(60));

                await RunRollups();

                await DoRetention();
            }
        }

        public void MarkForPolicy(DocumentsOperationContext context, TimeSeriesSliceHolder slicerHolder, DateTime timestamp)
        {
            if (Configuration.Collections.TryGetValue(slicerHolder.Collection, out var config) == false)
                return;

            if (config.Disabled)
                return;

            var current = config.GetPolicyIndexByTimeSeries(slicerHolder.Name);
            if (current == -1) // policy not found
                return;

            var nextPolicy = config.GetNextPolicy(current);
            if (nextPolicy == null)
                return;

            if (ReferenceEquals(nextPolicy, TimeSeriesPolicy.AfterAllPolices))
                return; // this is the last policy

            _database.DocumentsStorage.TimeSeriesStorage.Rollups.MarkForPolicy(context, slicerHolder, nextPolicy, timestamp);
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
                    List<string> currentPolicies;
                    using (context.OpenReadTransaction())
                    {
                        currentPolicies = _database.DocumentsStorage.TimeSeriesStorage.Stats.GetAllPolicies(context, collectionName).Select(p => p.ToString()).ToList();
                    }

                    foreach (var policy in policies)
                    {
                        if (currentPolicies.Contains(policy.Policy.Name))
                            continue;

                        var prev = config.Value.GetPreviousPolicy(policy.Index);
                        if (prev == null || ReferenceEquals(prev, TimeSeriesPolicy.BeforeAllPolices))
                            continue;

                        await AddNewPolicy(collectionName, prev, policy.Policy);
                    }
                }
            }
        }

        private async Task AddNewPolicy(CollectionName collectionName, TimeSeriesPolicy prev, TimeSeriesPolicy policy)
        {
            var skip = 0;
            while (true)
            {
                Cts.Token.ThrowIfCancellationRequested();

                var cmd = new TimeSeriesRollups.AddedNewRollupPoliciesCommand(collectionName, prev, policy, skip);
                await _database.TxMerger.Enqueue(cmd);

                if (cmd.Marked < TimeSeriesRollups.AddedNewRollupPoliciesCommand.BatchSize)
                    break;

                skip += cmd.Marked;
            }
        }

        internal async Task RunRollups()
        {
            var now = _database.Time.GetUtcNow();
            try
            {
                var states = new List<TimeSeriesRollups.RollupState>();
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
                            _database.DocumentsStorage.TimeSeriesStorage.Rollups.PrepareRollups(context, now, 1024, states, out duration);
                            if (states.Count == 0)
                                return;
                        }

                        Cts.Token.ThrowIfCancellationRequested();

                        var topology = _database.ServerStore.LoadDatabaseTopology(_database.Name);
                        var isFirstInTopology = string.Equals(topology.Members.FirstOrDefault(), _database.ServerStore.NodeTag, StringComparison.OrdinalIgnoreCase);

                        var command = new TimeSeriesRollups.RollupTimeSeriesCommand(Configuration, now, states, isFirstInTopology);
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

            var now = _database.Time.GetUtcNow();
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
                    var list = tss.Stats.GetTimeSeriesByPolicyFromStartDate(context, collectionName, policy.Name, deleteFrom, TimeSeriesRollups.TimeSeriesRetentionCommand.BatchSize).ToList();
                    if (list.Count == 0)
                        return;

                    request.To = deleteFrom;
                    var cmd = new TimeSeriesRollups.TimeSeriesRetentionCommand(list, request);
                    await _database.TxMerger.Enqueue(cmd);
                }
            }
        }
    }
}

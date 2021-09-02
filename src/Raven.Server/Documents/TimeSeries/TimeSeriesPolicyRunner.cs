using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Server.Background;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Documents.TimeSeries
{
    public class TimeSeriesPolicyRunner : BackgroundWorkBase
    {
        private readonly DocumentDatabase _database;

        public TimeSeriesConfiguration Configuration { get; }

        private readonly TimeSpan _checkFrequency;

        public TimeSeriesPolicyRunner(DocumentDatabase database, TimeSeriesConfiguration configuration) : base(database.Name, database.DatabaseShutdown)
        {
            _database = database;
            Configuration = configuration;
            if (configuration.Collections != null)
                Configuration.Collections =
                    new Dictionary<string, TimeSeriesCollectionConfiguration>(Configuration.Collections, StringComparer.OrdinalIgnoreCase);

            Configuration.InitializeRollupAndRetention();

            _checkFrequency = Configuration.PolicyCheckFrequency ?? TimeSeriesConfiguration.DefaultPolicyCheckFrequency;
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

                database.ServerStore.LicenseManager.AssertCanAddTimeSeriesRollupsAndRetention(dbRecord.TimeSeries);

                if (policyRunner != null)
                {
                    // no changes
                    if (policyRunner.Configuration.PolicyConfigurationChanged(dbRecord.TimeSeries) == false)
                        return policyRunner;
                }

                policyRunner?.Dispose();

                var runner = new TimeSeriesPolicyRunner(database, dbRecord.TimeSeries);
                runner.Start();
                return runner;
            }
            catch (Exception e)
            {
                const string msg = "Cannot enable policy runner as the configuration record is not valid.";

                if (e is LicenseLimitException lle)
                {
                    LicenseLimitWarning.AddLicenseLimitNotification(database.ServerStore.NotificationCenter, lle);
                }
                else
                {
                    database.NotificationCenter.Add(AlertRaised.Create(
                        database.Name,
                        $"Time series policy runner for database '{database.Name}' encountered an error", msg,
                        AlertType.RevisionsConfigurationNotValid, NotificationSeverity.Error, database.Name));
                }

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
                await WaitOrThrowOperationCanceled(_checkFrequency);

                await RunRollups(propagateException: false);

                await DoRetention(propagateException: false);
            }
        }

        public void MarkSegmentForPolicy(DocumentsOperationContext context, TimeSeriesSliceHolder slicerHolder, DateTime timestamp, 
            string changeVector,
            int numberOfEntries)
        {
            if (Configuration.Collections.TryGetValue(slicerHolder.Collection, out var config) == false)
                return;

            var currentIndex = config.GetPolicyIndexByTimeSeries(slicerHolder.Name);
            if (currentIndex == -1) // policy not found
                return;

            var nextPolicy = config.GetNextPolicy(currentIndex);
            if (nextPolicy == null)
                return;

            if (ReferenceEquals(nextPolicy, TimeSeriesPolicy.AfterAllPolices))
                return; // this is the last policy

            if (numberOfEntries == 0)
            {
                var currentPolicy = config.GetPolicy(currentIndex);
                var now = context.DocumentDatabase.Time.GetUtcNow();
                var nextRollup = new DateTime(TimeSeriesRollups.NextRollup(timestamp, nextPolicy));
                var startRollup = nextRollup.Add(-currentPolicy.RetentionTime);
                if (now - startRollup > currentPolicy.RetentionTime)
                    return; // ignore this segment since it is outside our retention frame
            }

            _database.DocumentsStorage.TimeSeriesStorage.Rollups.MarkSegmentForPolicy(context, slicerHolder, nextPolicy, timestamp, changeVector);
        }

        public void MarkForPolicy(DocumentsOperationContext context, TimeSeriesSliceHolder slicerHolder, DateTime timestamp, ulong status)
        {
            if (Configuration.Collections.TryGetValue(slicerHolder.Collection, out var config) == false)
                return;

            if (config.Disabled)
                return;

            var currentIndex = config.GetPolicyIndexByTimeSeries(slicerHolder.Name);
            if (currentIndex == -1) // policy not found
                return;
            
            var nextPolicy = config.GetNextPolicy(currentIndex);
            if (nextPolicy == null)
                return;

            if (ReferenceEquals(nextPolicy, TimeSeriesPolicy.AfterAllPolices))
                return; // this is the last policy

            if (status == TimeSeriesValuesSegment.Dead)
            {
                var currentPolicy = config.GetPolicy(currentIndex);
                if (currentPolicy.RetentionTime < TimeValue.MaxValue)
                {
                    var now = context.DocumentDatabase.Time.GetUtcNow();
                    var startRollup = new DateTime(TimeSeriesRollups.NextRollup(timestamp, nextPolicy)).Add(-currentPolicy.RetentionTime);
                    if (startRollup.Add(currentPolicy.RetentionTime) < now)
                        return; // ignore this value since it is outside our retention frame
                }
            }

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

                policies.Add((config.Value.RawPolicy, 0));
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
                        currentPolicies = _database.DocumentsStorage.TimeSeriesStorage.Stats.GetAllPolicies(context, collectionName)
                            .Select(p =>
                            {
                                using (Slice.External(context.Allocator, p, p.Content.Length - 1, out var ex))
                                {
                                    return ex.ToString();
                                }
                            }).ToList();
                    }

                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Found {currentPolicies.Count} policies in collection '{collection}': ({string.Join(',', currentPolicies)})");

                    foreach (var policy in policies)
                    {
                        if (SkipExisting(policy.Policy.Name))
                            continue;

                        var prev = config.Value.GetPreviousPolicy(policy.Index);
                        if (prev == null || ReferenceEquals(prev, TimeSeriesPolicy.BeforeAllPolices))
                            continue;

                        if (Logger.IsInfoEnabled)
                            Logger.Info($"Adding new policy '{policy.Policy.Name}' for collection '{collection}'");

                        await AddNewPolicy(collectionName, prev, policy.Policy);
                    }

                    bool SkipExisting(string name)
                    {
                        for (int i = 0; i < currentPolicies.Count; i++)
                        {
                            var currentPolicy = currentPolicies[i];
                            if (string.Equals(currentPolicy, name, StringComparison.OrdinalIgnoreCase))
                            {
                                currentPolicies.RemoveAt(i);
                                return true;
                            }
                        }

                        return false;
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

                if (Logger.IsInfoEnabled)
                    Logger.Info($"New policy '{policy.Name}' marked {cmd.Marked} time-series");

                if (cmd.Marked < TimeSeriesRollups.AddedNewRollupPoliciesCommand.BatchSize)
                    break;

                skip += cmd.Marked;
            }
        }

        internal async Task<long> RunRollups(bool propagateException = true, List<string> explanations = null)
        {
            var now = _database.Time.GetUtcNow();
            var total = 0L;
            try
            {
                var states = new List<TimeSeriesRollups.RollupState>();
                var start = 0L;
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
                            explanations?.Add($"Preparing rollups at '{now.GetDefaultRavenFormat()}' with '{0}' start.");

                            _database.DocumentsStorage.TimeSeriesStorage.Rollups.PrepareRollups(context, now, 1024, start, states, out duration);
                            if (states.Count == 0)
                            {
                                explanations?.Add($"Cannot run rollups at '{now.GetDefaultRavenFormat()}' because there are no rollup states.");
                                return total;
                            }
                        }

                        Cts.Token.ThrowIfCancellationRequested();

                        var topology = _database.ServerStore.LoadDatabaseTopology(_database.Name);
                        var isFirstInTopology = string.Equals(topology.Members.FirstOrDefault(), _database.ServerStore.NodeTag, StringComparison.OrdinalIgnoreCase);

                        explanations?.Add($"RollupTimeSeriesCommand({now.GetDefaultRavenFormat()}, {isFirstInTopology})");

                        var command = new TimeSeriesRollups.RollupTimeSeriesCommand(Configuration, now, states, isFirstInTopology);
                        await _database.TxMerger.Enqueue(command);
                        
                        if (command.RolledUp > 0)
                        {
                            explanations?.Add($"RollupTimeSeriesCommand({now.GetDefaultRavenFormat()}, {isFirstInTopology}) = {command.RolledUp}");

                            total += command.RolledUp;

                            if (Logger.IsInfoEnabled)
                                Logger.Info($"Successfully aggregated {command.RolledUp:#,#;;0} time-series within {duration.ElapsedMilliseconds:#,#;;0} ms.");
                        }

                        start = states.Last().NextRollup.Ticks + 1;
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

                if (propagateException)
                    throw;

                return total;
            }
        }

        internal async Task DoRetention(bool propagateException = true)
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
                        var collectionName = _database.DocumentsStorage.GetCollection(collection, throwIfDoesNotExist: false);
                        if (collectionName == null)
                            continue;
                        
                        await ApplyRetention(context, config, collectionName, config.RawPolicy, now);

                        foreach (var policy in config.Policies)
                        {
                            await ApplyRetention(context, config, collectionName, policy, now.Add(-policy.AggregationTime));
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

                if (propagateException)
                    throw;
            }
        }

        private async Task ApplyRetention(
            DocumentsOperationContext context, 
            TimeSeriesCollectionConfiguration config,
            CollectionName collectionName, 
            TimeSeriesPolicy policy, 
            DateTime now)
        {
            var tss = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage;
            if (policy.RetentionTime == TimeValue.MaxValue)
                return;

            var to = now.Add(-policy.RetentionTime);
            var list = new List<Slice>();

            while (true)
            {
                Cts.Token.ThrowIfCancellationRequested();

                context.Reset();
                context.Renew();
                list.Clear();

                using (context.OpenReadTransaction())
                {
                    foreach (var item in tss.Stats.GetTimeSeriesByPolicyFromStartDate(context, collectionName, policy.Name, to, TimeSeriesRollups.TimeSeriesRetentionCommand.BatchSize))
                    {
                        if (RequiredForNextPolicy(context, config, policy, item, to)) 
                            continue;

                        if (tss.Rollups.HasPendingRollupFrom(context, item, to) == false)
                            list.Add(item);
                    }

                    if (list.Count == 0)
                        return;

                    if (Logger.IsInfoEnabled)
                    {
                        Logger.Info($"Found {list.Count} time-series for retention in policy {policy.Name} with collection '{collectionName.Name}' up-to {to}"
#if DEBUG

                                    + $"{Environment.NewLine}{string.Join(Environment.NewLine, list)}"
#endif

                                    );
                    }

                    var cmd = new TimeSeriesRollups.TimeSeriesRetentionCommand(list, collectionName.Name, to);
                    await _database.TxMerger.Enqueue(cmd);
                }
            }
        }

        private static bool RequiredForNextPolicy(DocumentsOperationContext context, TimeSeriesCollectionConfiguration config, TimeSeriesPolicy policy, Slice item, DateTime to)
        {
            var tss = context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage;
            var next = config.GetNextPolicy(policy);
            if (ReferenceEquals(next, TimeSeriesPolicy.AfterAllPolices) == false)
            {
                TimeSeriesRollups.SplitKey(item, out var docId, out var name);
                var raw = name.Split(TimeSeriesConfiguration.TimeSeriesRollupSeparator)[0];
                var currentStats = tss.Stats.GetStats(context, docId, policy.GetTimeSeriesName(raw));
                var nextStats = tss.Stats.GetStats(context, docId, next.GetTimeSeriesName(raw));

                var nextEnd = nextStats.End.Add(next.AggregationTime).AddMilliseconds(-1);
                if (nextEnd >= currentStats.End)
                    return false;

                if (nextEnd < to)
                    return true;
            }

            return false;
        }
    }
}

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NCrontab.Advanced;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Maintenance.Sharding;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Server.Utils;

namespace Raven.Server.ServerWide.Maintenance
{
    internal partial class ClusterObserver : IDisposable
    {
        private readonly PoolOfThreads.LongRunningWork _observe;
        private readonly DatabaseTopologyUpdater _databaseTopologyUpdater;
        private readonly OrchestratorTopologyUpdater _orchestratorTopologyUpdater;
        private readonly CancellationTokenSource _cts;
        private readonly ClusterMaintenanceSupervisor _maintenance;
        private readonly string _nodeTag;
        private readonly RachisConsensus<ClusterStateMachine> _engine;
        private readonly ClusterContextPool _contextPool;
        private readonly ObserverLogger _observerLogger;

        private readonly TimeSpan _supervisorSamplePeriod;
        private readonly ServerStore _server;
        private readonly TimeSpan _stabilizationTime;
        private readonly long _stabilizationTimeMs;

        public SystemTime Time = new SystemTime();

        private ServerNotificationCenter NotificationCenter => _server.NotificationCenter;

        internal ClusterMaintenanceSupervisor Maintenance => _maintenance;

        public ClusterObserver(
            ServerStore server,
            ClusterMaintenanceSupervisor maintenance,
            RachisConsensus<ClusterStateMachine> engine,
            long term,
            ClusterContextPool contextPool,
            CancellationToken token)
        {
            _maintenance = maintenance;
            _nodeTag = server.NodeTag;
            _server = server;
            _engine = engine;
            _term = term;
            _contextPool = contextPool;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(token);
            _observerLogger = new ObserverLogger(_nodeTag);

            var config = server.Configuration.Cluster;
            _supervisorSamplePeriod = config.SupervisorSamplePeriod.AsTimeSpan;
            _stabilizationTime = config.StabilizationTime.AsTimeSpan;
            _stabilizationTimeMs = (long)config.StabilizationTime.AsTimeSpan.TotalMilliseconds;

            var now = DateTime.UtcNow;
            _databaseTopologyUpdater = new DatabaseTopologyUpdater(server, engine, config, clusterObserverStartTime: now, _observerLogger);
            _orchestratorTopologyUpdater = new OrchestratorTopologyUpdater(server, engine, config, clusterObserverStartTime: now, _observerLogger);

            _observe = PoolOfThreads.GlobalRavenThreadPool.LongRunning(_ =>
            {
                try
                {
                    Run(_cts.Token);
                }
                catch
                {
                    // nothing we can do here
                }
            }, null, ThreadNames.ForClusterObserver($"Cluster observer for term {_term}", _term));
        }

        public bool Suspended = false; // don't really care about concurrency here
        internal long _iteration;
        private readonly long _term;
        private long _lastIndexCleanupTimeInTicks;
        internal long _lastTombstonesCleanupTimeInTicks;
        internal long _lastExpiredCompareExchangeCleanupTimeInTicks;
        private bool _hasMoreTombstones = false;

        public (ClusterObserverLogEntry[] List, long Iteration) ReadDecisionsForDatabase()
        {
            return (_observerLogger.DecisionsLog.ToArray(), _iteration);
        }

        public void Run(CancellationToken token)
        {
            // we give some time to populate the stats.
            if (token.WaitHandle.WaitOne(_stabilizationTime))
                return;

            var prevStats = _maintenance.GetStats();

            // wait before collecting the stats again.
            if (token.WaitHandle.WaitOne(_supervisorSamplePeriod))
                return;

            while (_term == _engine.CurrentTerm && token.IsCancellationRequested == false)
            {
                try
                {
                    if (Suspended == false)
                    {
                        _iteration++;
                        var newStats = _maintenance.GetStats();

                        // ReSharper disable once MethodSupportsCancellation
                        // we explicitly not passing a token here, since it will throw operation cancelled,
                        // but the original task might continue to run (with an open tx)

                        AnalyzeLatestStats(newStats, prevStats).Wait();
                        prevStats = newStats;
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception e)
                {
                    Debug.Assert(e.InnerException is not KeyNotFoundException,
                        $"Got a '{nameof(KeyNotFoundException)}' while analyzing maintenance stats on node {_nodeTag} : {e}");

                    _observerLogger.Log($"An error occurred while analyzing maintenance stats on node {_nodeTag}.", _iteration, e);
                }
                finally
                {
                    token.WaitHandle.WaitOne(_supervisorSamplePeriod);
                }
            }
        }

        private async Task AnalyzeLatestStats(
            Dictionary<string, ClusterNodeStatusReport> newStats,
            Dictionary<string, ClusterNodeStatusReport> prevStats)
        {
            var currentLeader = _engine.CurrentLeader;
            if (currentLeader == null)
                return;

            var updateCommands = new List<(UpdateTopologyCommand Update, string Reason)>();
            var responsibleNodePerDatabase = new Dictionary<string, List<ResponsibleNodeInfo>>();
            var cleanUnusedAutoIndexesCommands = new List<(UpdateDatabaseCommand Update, string Reason)>();
            var cleanCompareExchangeTombstonesCommands = new List<CleanCompareExchangeTombstonesCommand>();

            Dictionary<string, long> cleanUpState = null;
            List<DeleteDatabaseCommand> deletions = null;
            List<DestinationMigrationConfirmCommand> confirmCommands = null;
            List<string> databases;

            using (_contextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                databases = _engine.StateMachine.GetDatabaseNames(context).ToList();
            }

            var now = Time.GetUtcNow();
            var cleanupIndexes = now.Ticks - _lastIndexCleanupTimeInTicks >= _server.Configuration.Indexing.CleanupInterval.AsTimeSpan.Ticks;
            var cleanupTombstones = now.Ticks - _lastTombstonesCleanupTimeInTicks >= _server.Configuration.Cluster.CompareExchangeTombstonesCleanupInterval.AsTimeSpan.Ticks;
            var cleanupExpiredCompareExchange = now.Ticks - _lastExpiredCompareExchangeCleanupTimeInTicks >= _server.Configuration.Cluster.CompareExchangeExpiredCleanupInterval.AsTimeSpan.Ticks;

            foreach (var database in databases)
            {
                using (_contextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var clusterTopology = _server.GetClusterTopology(context);

                    _cts.Token.ThrowIfCancellationRequested();

                    using (var rawRecord = _engine.StateMachine.ReadRawDatabaseRecord(context, database, out long etag))
                    {
                        if (rawRecord == null)
                        {
                            _observerLogger.Log($"Can't analyze the stats of database the {database}, because the database record is null.", iteration: _iteration, database: database);
                            continue;
                        }

                        if (rawRecord.IsSharded)
                        {
                            var databaseName = rawRecord.DatabaseName;
                            var topology = rawRecord.Sharding.Orchestrator.Topology;
                            var state = new DatabaseObservationState(databaseName, rawRecord, topology, clusterTopology, newStats, prevStats, etag, _iteration);

                            if (SkipAnalyzingDatabaseGroup(state, currentLeader, now))
                                continue;

                            List<DeleteDatabaseCommand> unneededDeletions = null; // database deletions are irrelevant in orchestrator topology changes
                            var updateReason = _orchestratorTopologyUpdater.Update(context, state, ref unneededDeletions);

                            if (updateReason != null)
                            {
                                _observerLogger.AddToDecisionLog(databaseName, updateReason, _iteration);

                                var cmd = new UpdateTopologyCommand(databaseName, now, RaftIdGenerator.NewId())
                                {
                                    Topology = topology,
                                    RaftCommandIndex = etag,
                                };

                                updateCommands.Add((cmd, updateReason));
                            }

                            UpdateReshardingStatus(context, rawRecord, newStats, ref confirmCommands);

                            //if orchestrator topology was changed, we skip the checks for the shard topologies to avoid concurrency exception
                            if (updateReason != null)
                                continue;
                        }

                        var mergedState = new MergedDatabaseObservationState(rawRecord);

                        foreach (var topology in rawRecord.Topologies)
                        {
                            var state = new DatabaseObservationState(topology.Name, rawRecord, topology.Topology, clusterTopology, newStats, prevStats, etag, _iteration);

                            try
                            {
                                mergedState.AddState(state);

                                if (SkipAnalyzingDatabaseGroup(state, currentLeader, now))
                                    continue;

                                var updateReason = _databaseTopologyUpdater.Update(context, state, ref deletions);
                                if (updateReason != null)
                                {
                                    _observerLogger.AddToDecisionLog(state.Name, updateReason, state.ObserverIteration);

                                    var cmd = new UpdateTopologyCommand(state.Name, now, RaftIdGenerator.NewId())
                                    {
                                        Topology = state.DatabaseTopology, RaftCommandIndex = state.LastIndexModification,
                                    };

                                    updateCommands.Add((cmd, updateReason));
                                    //breaking here to only change the db record once in order to avoid concurrency exception
                                    break;
                                }
                            }
                            finally
                            {
                                var responsibleNodeCommands = GetResponsibleNodesForBackupTasks(currentLeader, rawRecord, topology.Name, state.DatabaseTopology, state.ObserverIteration, context);
                                if (responsibleNodeCommands is { Count: > 0 })
                                    responsibleNodePerDatabase[topology.Name] = responsibleNodeCommands;
                            }
                        }

                        var cleanUp = mergedState.States.Min(s => CleanUpDatabaseValues(s.Value) ?? -1);
                        if (cleanUp > 0)
                        {
                            cleanUpState ??= new Dictionary<string, long>();
                            cleanUpState.Add(database, cleanUp);
                        }

                        if (cleanupIndexes)
                        {
                            var cleanupCommandsForDatabase = GetUnusedAutoIndexes(mergedState);
                            cleanUnusedAutoIndexesCommands.AddRange(cleanupCommandsForDatabase);
                        }

                        if (cleanupTombstones)
                        {
                            var cmd = GetCleanCompareExchangeTombstonesCommand(database, mergedState, context, out var cleanupState);
                            switch (cleanupState)
                            {
                                case CompareExchangeTombstonesCleanupState.InvalidDatabaseObservationState:
                                    _hasMoreTombstones = true;
                                    break;
                                case CompareExchangeTombstonesCleanupState.HasMoreTombstones:
                                    Debug.Assert(cmd != null, $"Expected to get command {nameof(CleanCompareExchangeTombstonesCommand)} but it was null");
                                    cleanCompareExchangeTombstonesCommands.Add(cmd);
                                    break;
                                case CompareExchangeTombstonesCleanupState.InvalidPeriodicBackupStatus:
                                case CompareExchangeTombstonesCleanupState.NoMoreTombstones:
                                    break;

                                default:
                                    throw new NotSupportedException($"Not supported state: '{cleanupState}'.");
                            }
                        }
                    }
                }
            }

            if (cleanupIndexes)
            {
                foreach (var (cmd, updateReason) in cleanUnusedAutoIndexesCommands)
                {
                    await _engine.SendToLeaderAsync(cmd);
                    _observerLogger.AddToDecisionLog(cmd.DatabaseName, updateReason, _iteration);
                }

                _lastIndexCleanupTimeInTicks = now.Ticks;
            }

            if (cleanupTombstones)
            {
                _hasMoreTombstones = false;
                foreach (var cmd in cleanCompareExchangeTombstonesCommands)
                {
                    var result = await _server.SendToLeaderAsync(cmd);
                    await _server.Cluster.WaitForIndexNotification(result.Index);
                    _hasMoreTombstones |= (bool)result.Result;
                }

                if (_hasMoreTombstones == false)
                    _lastTombstonesCleanupTimeInTicks = now.Ticks;
            }

            if (cleanupExpiredCompareExchange)
            {
                if (await RemoveExpiredCompareExchange(now.Ticks) == false)
                    _lastExpiredCompareExchangeCleanupTimeInTicks = now.Ticks;
            }

            foreach (var command in updateCommands)
            {
                try
                {
                    await UpdateTopology(command.Update);
                    var alert = AlertRaised.Create(
                        command.Update.DatabaseName,
                        $"Topology of database '{command.Update.DatabaseName}' was changed",
                        command.Reason,
                        AlertType.DatabaseTopologyWarning,
                        NotificationSeverity.Warning
                    );
                    NotificationCenter.Add(alert);
                }
                catch (Exception e) when (e.ExtractSingleInnerException() is ConcurrencyException or RachisConcurrencyException)
                {
                    // this is sort of expected, if the database was
                    // modified by someone else, we'll avoid changing
                    // it and run the logic again on the next round
                    _observerLogger.AddToDecisionLog(command.Update.DatabaseName,
                        $"Topology of database '{command.Update.DatabaseName}' was not changed, reason: {nameof(ConcurrencyException)}", _iteration);
                }
            }

            if (responsibleNodePerDatabase.Count > 0)
            {
                if (_engine.LeaderTag != _server.NodeTag)
                {
                    throw new NotLeadingException("This node is no longer the leader, so we abort updating the responsible node for backup tasks");
                }

                var command = new UpdateResponsibleNodeForTasksCommand(new UpdateResponsibleNodeForTasksCommand.Parameters
                {
                    ResponsibleNodePerDatabase = responsibleNodePerDatabase
                }, RaftIdGenerator.NewId());

                await _engine.SendToLeaderAsync(command);
            }
            if (deletions != null)
            {
                foreach (var command in deletions)
                {
                    _observerLogger.AddToDecisionLog(command.DatabaseName,
                        $"We reached the replication factor on '{command.DatabaseName}', so we try to remove promotables/rehabs from: {string.Join(", ", command.FromNodes)}", _iteration);

                    await Delete(command);
                }
            }

            if (cleanUpState != null)
            {
                var guid = "cleanup/" + GetCommandId(cleanUpState);
                if (_engine.ContainsCommandId(guid) == false)
                {
                    foreach (var kvp in cleanUpState)
                    {
                        _observerLogger.AddToDecisionLog(kvp.Key, $"Should clean up values up to raft index {kvp.Value}.", _iteration);
                    }

                    var cmd = new CleanUpClusterStateCommand(guid) { ClusterTransactionsCleanup = cleanUpState };

                    if (_engine.LeaderTag != _server.NodeTag)
                    {
                        throw new NotLeadingException("This node is no longer the leader, so abort the cleaning.");
                    }

                    await _engine.SendToLeaderAsync(cmd);
                }
            }

            if (confirmCommands != null)
            {
                foreach (var confirmCommand in confirmCommands)
                {
                    await _engine.SendToLeaderAsync(confirmCommand);
                }
            }
        }

        private void UpdateReshardingStatus(ClusterOperationContext context, RawDatabaseRecord rawRecord, Dictionary<string, ClusterNodeStatusReport> newStats, ref List<DestinationMigrationConfirmCommand> confirmCommands)
        {
            if (_server.Server.ServerStore.Sharding.ManualMigration)
                return;

            var databaseName = rawRecord.DatabaseName;
            var sharding = rawRecord.Sharding;
            var currentMigration = sharding.BucketMigrations.Values.FirstOrDefault(x => x.Status == MigrationStatus.Moved);
            if (currentMigration == null)
                return;

            var destination = ShardHelper.ToShardName(databaseName, currentMigration.DestinationShard);
            foreach (var node in newStats)
            {
                var tag = node.Key;
                var nodeReport = node.Value;

                if (currentMigration.ConfirmedDestinations.Contains(tag))
                    continue;

                if (nodeReport.Report.TryGetValue(destination, out var destinationReport))
                {
                    var raftId = ShardingStore.GenerateDestinationMigrationConfirmRaftId(currentMigration.Bucket, currentMigration.MigrationIndex, tag);
                    string lastChangeVector = null;
                    if (destinationReport.ReportPerBucket.TryGetValue(currentMigration.Bucket, out var bucketReport))
                    {
                        lastChangeVector = bucketReport.LastChangeVector;
                    }

                    var lastFromSrc = context.GetChangeVector(currentMigration.LastSourceChangeVector);
                    var currentFromDest = context.GetChangeVector(lastChangeVector);
                    var status = ChangeVector.GetConflictStatusForBucket(lastFromSrc, currentFromDest, rawRecord.UnusedDatabaseIds);
                    if (status == ConflictStatus.AlreadyMerged)
                    {
                        confirmCommands ??= new List<DestinationMigrationConfirmCommand>();
                        confirmCommands.Add(new DestinationMigrationConfirmCommand(currentMigration.Bucket,
                            currentMigration.MigrationIndex, tag, databaseName, raftId));
                    }
                }
            }
        }

        private bool SkipAnalyzingDatabaseGroup(DatabaseObservationState state, Leader currentLeader, DateTime now)
        {
            var databaseTopology = state.DatabaseTopology;
            var databaseName = state.Name;

            if (databaseTopology == null)
            {
                _observerLogger.Log($"Can't analyze the stats of database the {databaseName}, because the database topology is null.", _iteration, database: databaseName);
                return true;
            }

            if (databaseTopology.Count == 0)
            {
                // database being deleted
                _observerLogger.Log($"Skip analyze the stats of database the {databaseName}, because it being deleted", _iteration, database: databaseName);
                return true;
            }

            var topologyStamp = databaseTopology.Stamp;
            var graceIfLeaderChanged = _term > topologyStamp.Term && currentLeader.LeaderShipDuration < _stabilizationTimeMs;
            var letStatsBecomeStable = _term == topologyStamp.Term &&
                                       ((now - (databaseTopology.NodesModifiedAt ?? DateTime.MinValue)).TotalMilliseconds < _stabilizationTimeMs);

            if (graceIfLeaderChanged || letStatsBecomeStable)
            {
                _observerLogger.Log($"We give more time for the '{databaseName}' stats to become stable, so we skip analyzing it for now.", _iteration, database: databaseName);
                return true;
            }

            if (state.ReadDatabaseDisabled())
                return true;

            return false;
        }

        private static string GetCommandId(Dictionary<string, long> dic)
        {
            if (dic == null)
                return Guid.Empty.ToString();

            var hash = 0UL;
            foreach (var kvp in dic)
            {
                hash = Hashing.XXHash64.CalculateRaw(kvp.Key) ^ (ulong)kvp.Value ^ hash;
            }

            return hash.ToString("X");
        }

        internal List<(UpdateDatabaseCommand Update, string Reason)> GetUnusedAutoIndexes(MergedDatabaseObservationState mergedStates)
        {
            const string autoIndexPrefix = "Auto/";
            var cleanupCommands = new List<(UpdateDatabaseCommand Update, string Reason)>();

            var indexes = new Dictionary<string, TimeSpan>();


            foreach (var shardToState in mergedStates.States)
            {
                var databaseState = shardToState.Value;

                if (AllDatabaseNodesHasReport(databaseState) == false)
                    return cleanupCommands;

                foreach (var node in databaseState.DatabaseTopology.AllNodes)
                {
                    if (databaseState.Current.TryGetValue(node, out var nodeReport) == false)
                        return cleanupCommands;

                    if (nodeReport.Report.TryGetValue(databaseState.Name, out var report) == false)
                        return cleanupCommands;
                    
                    foreach (var kvp in report.LastIndexStats)
                    {
                        var lastQueried = kvp.Value.LastQueried;
                        if (lastQueried.HasValue == false)
                            continue;

                        var indexName = kvp.Key;
                        if (indexName.StartsWith(autoIndexPrefix, StringComparison.OrdinalIgnoreCase) == false)
                            continue;

                        if (indexes.TryGetValue(indexName, out var lq) == false || lq > lastQueried)
                        {
                            indexes[indexName] = lastQueried.Value;
                        }
                    }
                }
            }

            if (indexes.Count == 0)
                return cleanupCommands;

            var settings = mergedStates.RawDatabase.Settings;
            var timeToWaitBeforeMarkingAutoIndexAsIdle = (TimeSetting)RavenConfiguration.GetValue(x => x.Indexing.TimeToWaitBeforeMarkingAutoIndexAsIdle, _server.Configuration, settings);
            var timeToWaitBeforeDeletingAutoIndexMarkedAsIdle = (TimeSetting)RavenConfiguration.GetValue(x => x.Indexing.TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle, _server.Configuration, settings);

            foreach (var kvp in indexes)
            {
                // This value is actually the result of the Min function of values from all nodes for an index.
                var smallestTimeElapsedInCluster = kvp.Value;

                var state = IndexState.Normal;
                if (mergedStates.RawDatabase.AutoIndexes.TryGetValue(kvp.Key, out var definition) && definition.State.HasValue)
                    state = definition.State.Value;

                var shardedDatabaseName = ShardHelper.ToDatabaseName(mergedStates.RawDatabase.DatabaseName);

                if (state == IndexState.Idle && smallestTimeElapsedInCluster >= timeToWaitBeforeDeletingAutoIndexMarkedAsIdle.AsTimeSpan)
                {
                    var deleteIndexCommand = new DeleteIndexCommand(kvp.Key, shardedDatabaseName, RaftIdGenerator.NewId());
                    var updateReason = $"Deleting idle auto-index '{kvp.Key}' because last query time value is '{smallestTimeElapsedInCluster}' and threshold is set to '{timeToWaitBeforeDeletingAutoIndexMarkedAsIdle.AsTimeSpan}'.";

                    cleanupCommands.Add((deleteIndexCommand, updateReason));
                    continue;
                }

                if (state == IndexState.Normal && smallestTimeElapsedInCluster >= timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan)
                {
                    var setIndexStateCommand = new SetIndexStateCommand(kvp.Key, IndexState.Idle, shardedDatabaseName, RaftIdGenerator.NewId());
                    var updateReason = $"Marking auto-index '{kvp.Key}' as idle because last query time value is '{smallestTimeElapsedInCluster}' and threshold is set to '{timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan}'.";

                    cleanupCommands.Add((setIndexStateCommand, updateReason));
                    continue;
                }

                if (state == IndexState.Idle && smallestTimeElapsedInCluster < timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan)
                {
                    var setIndexStateCommand = new SetIndexStateCommand(kvp.Key, IndexState.Normal, shardedDatabaseName, Guid.NewGuid().ToString());
                    var updateReason = $"Marking idle auto-index '{kvp.Key}' as normal because last query time value is '{smallestTimeElapsedInCluster}' and threshold is set to '{timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan}'.";

                    cleanupCommands.Add((setIndexStateCommand, updateReason));
                }
            }

            return cleanupCommands;
        }

        internal CleanCompareExchangeTombstonesCommand GetCleanCompareExchangeTombstonesCommand(string databaseName, MergedDatabaseObservationState mergedState, ClusterOperationContext context, out CompareExchangeTombstonesCleanupState cleanupState)
        {
            var onDiagnosticLog = ForTestingPurposes?.OnDiagnosticLog;

            Debug.Assert(ShardHelper.IsShardName(databaseName) == false, $"Compare exchanges are put in cluster under sharded database name, so can't delete them from under shard name {databaseName}");
            const int amountToDelete = 8192;

            onDiagnosticLog?.Invoke($"Starting {nameof(GetCleanCompareExchangeTombstonesCommand)} for " +
                                    $"{(mergedState.RawDatabase?.IsSharded == true ? "Sharded" : "Not Sharded")} database '{databaseName}'" +
                                    $" with PeriodicBackupTaskIds: {(mergedState.RawDatabase?.PeriodicBackupsTaskIds is { Count: > 0 } ? string.Join(", ", mergedState.RawDatabase.PeriodicBackupsTaskIds) : "none")}");

            if (_server.Cluster.HasCompareExchangeTombstones(context, databaseName) == false)
            {
                onDiagnosticLog?.Invoke("No tombstones found in the cluster.");

                cleanupState = CompareExchangeTombstonesCleanupState.NoMoreTombstones;
                return null;
            }

            cleanupState = GetMaxCompareExchangeTombstonesEtagToDelete(mergedState, out long maxRaftIndex);

            onDiagnosticLog?.Invoke($"Computed maxRaftIndex: `{maxRaftIndex}`");

            if (cleanupState != CompareExchangeTombstonesCleanupState.HasMoreTombstones)
            {
                onDiagnosticLog?.Invoke($"Exiting early, cleanupState: `{cleanupState}`");

                return null;
            }

            onDiagnosticLog?.Invoke($"Sending cleanup command for database '{databaseName}' with maxRaftIndex: `{maxRaftIndex}` and amountToDelete: `{amountToDelete}`");

            return new CleanCompareExchangeTombstonesCommand(databaseName, maxRaftIndex, amountToDelete, RaftIdGenerator.NewId());
        }

        public enum CompareExchangeTombstonesCleanupState
        {
            HasMoreTombstones,
            InvalidDatabaseObservationState,
            InvalidPeriodicBackupStatus,
            NoMoreTombstones
        }

        private CompareExchangeTombstonesCleanupState GetMaxCompareExchangeTombstonesEtagToDelete(MergedDatabaseObservationState mergedState, out long maxEtag)
        {
            maxEtag = -1;
            var onDiagnosticLog = ForTestingPurposes?.OnDiagnosticLog;
            var utcNow = DateTime.UtcNow;

            onDiagnosticLog?.Invoke($"Executing {nameof(GetMaxCompareExchangeTombstonesEtagToDelete)} for '{mergedState.RawDatabase.DatabaseName}' at {utcNow:O}.");

            if (mergedState.IsValid(onDiagnosticLog) == false)
            {
                onDiagnosticLog?.Invoke("Aborting: MergedDatabaseObservationState is invalid. Reports may be missing or malformed.");

                return CompareExchangeTombstonesCleanupState.InvalidDatabaseObservationState;
            }

            var minEtagFromAllTasks = long.MaxValue;

            var periodicBackups = mergedState.RawDatabase.PeriodicBackups;
            if (periodicBackups is { Count: > 0 })
            {
                if (periodicBackups.Any(backupConfiguration => backupConfiguration.Disabled))
                {
                    onDiagnosticLog?.Invoke("Found a disabled periodic backup task. It's a blocking condition for tombstone cleanup.");

                    return CompareExchangeTombstonesCleanupState.NoMoreTombstones;
                }

                foreach (var backupConfig in periodicBackups)
                {
                    if (string.IsNullOrWhiteSpace(backupConfig.FullBackupFrequency))
                    {
                        onDiagnosticLog?.Invoke($"[Task {backupConfig.TaskId}] Found a task without FullBackupFrequency. This implies only incremental backups can be made, so we should never clean tombstones");

                        return CompareExchangeTombstonesCleanupState.NoMoreTombstones;
                    }

                    // Step 1: Find the MOST RECENT full backup for this task to use as our anchor
                    var allHistoricalFullBackups = new List<(DateTime Time, long RaftIndex)>();
                    foreach (var databaseState in mergedState.States.Values)
                    {
                        foreach (var nodeTag in databaseState.DatabaseTopology.AllNodes)
                        {
                            if (databaseState.GetCurrentDatabaseReport(nodeTag)?.BackupStatuses.TryGetValue(backupConfig.TaskId, out var statusReport) == true &&
                                statusReport.LastSuccessfulFullBackupTime.HasValue &&
                                statusReport.LastFullBackupRaftIndexEtag.HasValue)
                            {
                                allHistoricalFullBackups.Add((statusReport.LastSuccessfulFullBackupTime.Value, statusReport.LastFullBackupRaftIndexEtag.Value));
                            }
                        }
                    }

                    if (allHistoricalFullBackups.Count == 0)
                    {
                        onDiagnosticLog?.Invoke($"[Task {backupConfig.TaskId}] No historical full backups found for this task. Tombstone cleanup is not constrained by current backup task.");

                        allHistoricalFullBackups.Add((Time: utcNow, RaftIndex: long.MaxValue));
                    }

                    var latestFullBackup = allHistoricalFullBackups.OrderByDescending(b => b.Time).First();

                    onDiagnosticLog?.Invoke($"[Task {backupConfig.TaskId}] Anchor backup identified: Time=`{latestFullBackup.Time:O}`, Etag=`{latestFullBackup.RaftIndex}`. This backup defines the relevant cycle.");

                    // Step 2: Based on the anchor, determine the start of its cycle
                    var schedule = CrontabSchedule.Parse(backupConfig.FullBackupFrequency);
                    var cycleStartTime = schedule.GetPreviousOccurrence(latestFullBackup.Time);

                    onDiagnosticLog?.Invoke($"[Task {backupConfig.TaskId}] The cycle for the anchor backup started at: `{cycleStartTime:O}`.");

                    // Step 3: Find all full backups that fall within THIS evidence-based cycle
                    var etagsInCycle = allHistoricalFullBackups
                        .Where(b => b.Time >= cycleStartTime)
                        .Select(b => b.RaftIndex)
                        .ToList();

                    Debug.Assert(etagsInCycle.Count > 0, "The anchor backup must be present in its own cycle.");

                    // Step 4: The limit for this task is the EARLIEST backup from this cycle
                    var limitForThisTask = etagsInCycle.Min();

                    onDiagnosticLog?.Invoke($"[Task {backupConfig.TaskId}] Found {etagsInCycle.Count} backups in this cycle. The limit (earliest etag) for this task is: `{limitForThisTask}`.");

                    // Step 5: Update the overall minimum etag across all tasks
                    if (limitForThisTask < minEtagFromAllTasks)
                        minEtagFromAllTasks = limitForThisTask;
                }
            }
            else
            {
                onDiagnosticLog?.Invoke("No periodic backup tasks configured. Tombstone cleanup is not constrained by backups.");
            }

            maxEtag = minEtagFromAllTasks == long.MaxValue
                ? -1
                : minEtagFromAllTasks;

            // Apply final constraints
            onDiagnosticLog?.Invoke($"Backup-based Etag limit is `{(maxEtag == -1 ? "unconstrained" : maxEtag.ToString())}`. Applying constraints from cluster transactions and indexes.");

            long minClusterConstraintEtag = -1;

            if (_server?.ForTestingPurposes?.IgnoreClusterTransactionIndexInCompareExchangeCleaner == true)
            {
                minClusterConstraintEtag = long.MaxValue;

                onDiagnosticLog?.Invoke($"Ignoring cluster transaction index in compare exchange cleaner, setting minClusterConstraintEtag to {minClusterConstraintEtag}.");
            }

            foreach (var databaseState in mergedState.States.Values)
            {
                foreach (var nodeTag in databaseState.DatabaseTopology.AllNodes)
                {
                    var report = databaseState.GetCurrentDatabaseReport(nodeTag);
                    if (report == null)
                        continue;

                    var clusterTxIndex = report.LastClusterWideTransactionRaftIndex;
                    if (minClusterConstraintEtag == -1 || clusterTxIndex < minClusterConstraintEtag)
                    {
                        onDiagnosticLog?.Invoke($"[Node {nodeTag}] Found new minimum constraint from cluster-wide tx: `{clusterTxIndex}`.");

                        minClusterConstraintEtag = clusterTxIndex;
                    }

                    foreach ((string indexName, DatabaseStatusReport.ObservedIndexStatus indexStats) in report.LastIndexStats)
                    {
                        if (indexStats.LastIndexedCompareExchangeReferenceTombstoneEtag.HasValue == false)
                            continue;

                        long indexEtag = indexStats.LastIndexedCompareExchangeReferenceTombstoneEtag.Value;
                        if (minClusterConstraintEtag != -1 && indexEtag >= minClusterConstraintEtag)
                            continue;

                        onDiagnosticLog?.Invoke($"[Node {nodeTag}][Index {indexName}] Found new minimum constraint from index: `{indexEtag}`.");

                        minClusterConstraintEtag = indexEtag;
                    }
                }
            }

            if (maxEtag == -1 || (minClusterConstraintEtag != -1 && minClusterConstraintEtag < maxEtag))
            {
                onDiagnosticLog?.Invoke($"Cluster-wide constraint ({minClusterConstraintEtag}) is stricter than backup-based limit (`{(maxEtag == -1 ? "none" : maxEtag.ToString())}`). Applying stricter limit.");

                maxEtag = minClusterConstraintEtag;
            }

            if (maxEtag <= 0)
            {
                onDiagnosticLog?.Invoke($"Final cleanup Etag is {maxEtag}. No tombstones will be cleaned this time.");

                return CompareExchangeTombstonesCleanupState.NoMoreTombstones;
            }

            onDiagnosticLog?.Invoke($"Final decision: tombstones can be cleaned up to Etag `{maxEtag}`.");

            return CompareExchangeTombstonesCleanupState.HasMoreTombstones;
        }

        private async Task<bool> RemoveExpiredCompareExchange(long nowTicks)
        {
            const int batchSize = 1024;
            using (_contextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                if (CompareExchangeExpirationStorage.HasExpired(context, nowTicks) == false)
                    return false;
            }

            var result = await _server.SendToLeaderAsync(new DeleteExpiredCompareExchangeCommand(nowTicks, batchSize, RaftIdGenerator.NewId()));
            await _server.Cluster.WaitForIndexNotification(result.Index);
            return (bool)result.Result;
        }

        private long? CleanUpDatabaseValues(DatabaseObservationState state)
        {
            if (_server.Engine.CommandsVersionManager.CurrentClusterMinimalVersion <
                ClusterCommandsVersionManager.ClusterCommandsVersions[nameof(CleanUpClusterStateCommand)])
            {
                return null;
            }

            if (AllDatabaseNodesHasReport(state) == false)
                return null;

            long commandCount = long.MaxValue;
            foreach (var node in state.DatabaseTopology.AllNodes)
            {
                if (state.Current.TryGetValue(node, out var nodeReport) == false)
                    return null;

                if (nodeReport.Report.TryGetValue(state.Name, out var report) == false)
                    return null;

                commandCount = Math.Min(commandCount, report.LastCompletedClusterTransaction);
            }

            if (commandCount <= state.ReadTruncatedClusterTransactionCommandsCount())
                return null;

            return commandCount;
        }

        private static bool AllDatabaseNodesHasReport(DatabaseObservationState state)
        {
            if (state == null)
                return false;

            if (state.DatabaseTopology.Count == 0)
                return false; // database is being deleted, so no need to cleanup values

            foreach (var node in state.DatabaseTopology.AllNodes)
            {
                if (state.Current.ContainsKey(node) == false)
                    return false;
            }

            return true;
        }

        private Task<(long Index, object Result)> UpdateTopology(UpdateTopologyCommand cmd)
        {
            if (_engine.LeaderTag != _server.NodeTag)
            {
                throw new NotLeadingException("This node is no longer the leader, so we abort updating the database databaseTopology");
            }

            return _engine.SendToLeaderAsync(cmd);
        }

        private Task<(long Index, object Result)> Delete(DeleteDatabaseCommand cmd)
        {
            if (_engine.LeaderTag != _server.NodeTag)
            {
                throw new NotLeadingException("This node is no longer the leader, so we abort the deletion command");
            }
            return _engine.SendToLeaderAsync(cmd);
        }

        public void Dispose()
        {
            _cts.Cancel();

            try
            {
                if (_observe.Join((int)TimeSpan.FromSeconds(30).TotalMilliseconds) == false)
                {
                    throw new ObjectDisposedException($"Cluster observer on node {_nodeTag} still running and can't be closed");
                }
            }
            finally
            {
                _cts.Dispose();
            }
        }

        internal sealed class MergedDatabaseObservationState
        {
            public static MergedDatabaseObservationState Empty = new MergedDatabaseObservationState();
            private readonly bool _isShardedState;
            public readonly Dictionary<int, DatabaseObservationState> States;
            public readonly RawDatabaseRecord RawDatabase;

            public MergedDatabaseObservationState(RawDatabaseRecord record)
            {
                RawDatabase = record;
                _isShardedState = RawDatabase.IsSharded;

                var length = _isShardedState ? RawDatabase.Sharding.Shards.Count : 1;
                States = new Dictionary<int, DatabaseObservationState>(length);
            }

            public MergedDatabaseObservationState(RawDatabaseRecord record, DatabaseObservationState state) : this(record)
            {
                AddState(state);
            }

            private MergedDatabaseObservationState()
            {
                States = new Dictionary<int, DatabaseObservationState>(1);
            }

            public void AddState(DatabaseObservationState state)
            {
                if (ShardHelper.TryGetShardNumberFromDatabaseName(state.Name, out var shardNumber) == false)
                {
                    // handle not sharded database
                    if (_isShardedState)
                        throw new InvalidOperationException($"The database {state.Name} isn't sharded, but was initialized as one.");

                    States[0] = state;
                    return;
                }

                if (_isShardedState == false)
                    throw new InvalidOperationException($"The database {state.Name} is sharded (shard: {shardNumber}), but was initialized as a regular one.");

                States[shardNumber] = state;
            }

            internal bool IsValid(Action<string> onDiagnosticLog)
            {
                foreach (DatabaseObservationState databaseObservationState in States.Values)
                {
                    foreach (var nodeTag in databaseObservationState.DatabaseTopology.AllNodes)
                    {
                        var hasClusterNodeStatusReport = databaseObservationState.Current.TryGetValue(nodeTag, out var clusterNodeStatusReport);
                        if (hasClusterNodeStatusReport == false)
                        {
                            onDiagnosticLog?.Invoke($"[Node {nodeTag}] Missing cluster node status report for database '{databaseObservationState.Name}'.");

                            return false;
                        }

                        var hasDatabaseStatusReport = clusterNodeStatusReport.Report.TryGetValue(databaseObservationState.Name, out var databaseStatusReport);
                        if (hasDatabaseStatusReport == false)
                        {
                            onDiagnosticLog?.Invoke($"[Node {nodeTag}] Missing database status report for database '{databaseObservationState.Name}'.");

                            return false;
                        }

                        if (databaseStatusReport.BackupStatuses == null)
                        {
                            onDiagnosticLog?.Invoke($"[Node {nodeTag}] BackupStatuses is null for database '{databaseObservationState.Name}'.");

                            return false;
                        }

                        onDiagnosticLog?.Invoke($"[Node {nodeTag}] {nameof(DatabaseStatusReport.LastClusterWideTransactionRaftIndex)} = {(databaseStatusReport.LastClusterWideTransactionRaftIndex == long.MaxValue ? "long.MaxValue" : databaseStatusReport.LastClusterWideTransactionRaftIndex.ToString())}");

                        foreach ((long taskId, PeriodicBackupStatusReport backupStatusReport) in databaseStatusReport.BackupStatuses)
                        {
                            if (backupStatusReport?.LastSuccessfulFullBackupTime == null)
                                onDiagnosticLog?.Invoke($"[Node {nodeTag}] Did not find a successful full backup for taskId '{taskId}' in database '{databaseObservationState.Name}'.");

                            var backupConfiguration = RawDatabase.GetPeriodicBackupConfiguration(taskId);
                            if (backupConfiguration == null)
                            {
                                onDiagnosticLog?.Invoke($"[Node {nodeTag}] Missing backup configuration for taskId '{taskId}' in database '{databaseObservationState.Name}'. Should not happen, probably a bug.");

                                return false;
                            }

                            if (backupConfiguration.FullBackupFrequency == null && backupConfiguration.IncrementalBackupFrequency == null)
                            {
                                onDiagnosticLog?.Invoke($"[Node {nodeTag}] Backup configuration for taskId '{taskId}' has no FullBackupFrequency and no IncrementalBackupFrequency. Should not happen, probably a bug.");

                                return false;
                            }
                        }
                    }
                }

                return true;
            }
        }

        internal sealed class DatabaseObservationState
        {
            public readonly string Name;
            public readonly DatabaseTopology DatabaseTopology;
            public readonly Dictionary<string, ClusterNodeStatusReport> Current;
            public readonly Dictionary<string, ClusterNodeStatusReport> Previous;
            public readonly ClusterTopology ClusterTopology;

            public readonly RawDatabaseRecord RawDatabase;
            public readonly long LastIndexModification;
            public readonly long ObserverIteration;

            public DatabaseObservationState(
                [NotNull] string name,
                [NotNull] RawDatabaseRecord databaseRecord,
                [NotNull] DatabaseTopology databaseTopology,
                [NotNull] ClusterTopology clusterTopology,
                Dictionary<string, ClusterNodeStatusReport> current,
                Dictionary<string, ClusterNodeStatusReport> previous,
                long lastIndexModification,
                long observerIteration)
            {
                Name = name ?? throw new ArgumentNullException(nameof(name));
                RawDatabase = databaseRecord ?? throw new ArgumentNullException(nameof(databaseRecord));
                DatabaseTopology = databaseTopology ?? throw new ArgumentNullException(nameof(databaseTopology));
                ClusterTopology = clusterTopology ?? throw new ArgumentNullException(nameof(clusterTopology));
                Current = current ?? throw new ArgumentNullException(nameof(current));
                Previous = previous ?? throw new ArgumentNullException(nameof(previous));
                LastIndexModification = lastIndexModification;
                ObserverIteration = observerIteration;
            }

            public long ReadTruncatedClusterTransactionCommandsCount()
            {
                RawDatabase.Raw.TryGet(nameof(DatabaseRecord.TruncatedClusterTransactionCommandsCount), out long count);
                return count;
            }

            public Dictionary<string, DeletionInProgressStatus> ReadDeletionInProgress()
            {
                return RawDatabase.DeletionInProgress;
            }

            public bool ReadDatabaseDisabled()
            {
                return RawDatabase.IsDisabled;
            }

            public Dictionary<string, string> ReadSettings()
            {
                return RawDatabase.Settings;
            }

            public bool HasActiveMigrations()
            {
                if (RawDatabase.IsSharded == false)
                    return false;

                return RawDatabase.Sharding.HasActiveMigrations();
            }

            public DatabaseStatusReport GetCurrentDatabaseReport(string node)
            {
                if (Current.TryGetValue(node, out var report) == false)
                    return null;

                if (report.Report.TryGetValue(Name, out var databaseReport) == false)
                    return null;

                return databaseReport;
            }

            public DatabaseStatusReport GetPreviousDatabaseReport(string node)
            {
                if (Previous.TryGetValue(node, out var report) == false)
                    return null;

                if (report.Report.TryGetValue(Name, out var databaseReport) == false)
                    return null;

                return databaseReport;
            }

            public static implicit operator MergedDatabaseObservationState(DatabaseObservationState state)
            {
                return new MergedDatabaseObservationState(state.RawDatabase, state);
            }
        }

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            internal Action<string> OnDiagnosticLog;
        }
    }
}

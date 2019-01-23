using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using static Raven.Server.ServerWide.Maintenance.DatabaseStatus;

namespace Raven.Server.ServerWide.Maintenance
{
    class ClusterObserver : IDisposable
    {
        private readonly PoolOfThreads.LongRunningWork _observe;
        private readonly CancellationTokenSource _cts;
        private readonly ClusterMaintenanceSupervisor _maintenance;
        private readonly string _nodeTag;
        private readonly RachisConsensus<ClusterStateMachine> _engine;
        private readonly TransactionContextPool _contextPool;
        private readonly Logger _logger;

        private readonly TimeSpan _supervisorSamplePeriod;
        private readonly ServerStore _server;
        private readonly TimeSpan _stabilizationTime;
        private readonly long _stabilizationTimeMs;
        private readonly TimeSpan _breakdownTimeout;
        private readonly bool _hardDeleteOnReplacement;

        private NotificationCenter.NotificationCenter NotificationCenter => _server.NotificationCenter;

        public ClusterObserver(
            ServerStore server,
            ClusterMaintenanceSupervisor maintenance,
            RachisConsensus<ClusterStateMachine> engine,
            long term,
            TransactionContextPool contextPool,
            CancellationToken token)
        {
            _maintenance = maintenance;
            _nodeTag = server.NodeTag;
            _server = server;
            _engine = engine;
            _term = term;
            _contextPool = contextPool;
            _logger = LoggingSource.Instance.GetLogger<ClusterObserver>(_nodeTag);
            _cts = CancellationTokenSource.CreateLinkedTokenSource(token);

            var config = server.Configuration.Cluster;
            _supervisorSamplePeriod = config.SupervisorSamplePeriod.AsTimeSpan;
            _stabilizationTime = config.StabilizationTime.AsTimeSpan;
            _stabilizationTimeMs = (long)config.StabilizationTime.AsTimeSpan.TotalMilliseconds;
            _moveToRehabTime = (long)config.MoveToRehabGraceTime.AsTimeSpan.TotalMilliseconds;
            _breakdownTimeout = config.AddReplicaTimeout.AsTimeSpan;
            _hardDeleteOnReplacement = config.HardDeleteOnReplacement;

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
            }, null, $"Cluster observer for term {_term}");
        }

        public bool Suspended = false; // don't really care about concurrency here
        private readonly BlockingCollection<ClusterObserverLogEntry> _decisionsLog = new BlockingCollection<ClusterObserverLogEntry>();
        private long _iteration;
        private readonly long _term;
        private readonly long _moveToRehabTime;
        private long _lastIndexCleanupTimeInTicks;

        public (ClusterObserverLogEntry[] List, long Iteration) ReadDecisionsForDatabase()
        {
            return (_decisionsLog.ToArray(), _iteration);
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
                        AnalyzeLatestStats(newStats, prevStats).Wait(token);
                        prevStats = newStats;
                    }
                }
                catch (Exception e)
                {
                    LogMessage($"An error occurred while analyzing maintenance stats on node {_nodeTag}.", e);
                }
                finally
                {
                    token.WaitHandle.WaitOne(_supervisorSamplePeriod);
                }
            }
        }

        private readonly Dictionary<string, long> _lastLogs = new Dictionary<string, long>();

        private void LogMessage(string message, Exception e = null, bool info = true)
        {
            if (_iteration % 10_000 == 0)
                _lastLogs.Clear();

            if (_logger.IsInfoEnabled || _logger.IsOperationsEnabled)
            {
                if (_lastLogs.TryGetValue(message, out var last))
                {
                    if (last + 10 > _iteration)
                        return;
                }
                _lastLogs[message] = _iteration;

                if (info)
                {
                    _logger.Info(message, e);
                }
                else
                {
                    _logger.Operations(message, e);
                }
            }
        }

        private async Task AnalyzeLatestStats(
            Dictionary<string, ClusterNodeStatusReport> newStats,
            Dictionary<string, ClusterNodeStatusReport> prevStats
            )
        {
            var currentLeader = _engine.CurrentLeader;
            if (currentLeader == null)
                return;

            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var updateCommands = new List<(UpdateTopologyCommand Update, string Reason)>();
                Dictionary<string, long> cleanUpState = null;
                List<DeleteDatabaseCommand> deletions = null;
                using (context.OpenReadTransaction())
                {
                    var now = SystemTime.UtcNow;
                    var cleanupIndexes = now.Ticks - _lastIndexCleanupTimeInTicks >= _server.Configuration.Indexing.CleanupInterval.AsTimeSpan.Ticks;

                    var clusterTopology = _server.GetClusterTopology(context);
                    foreach (var database in _engine.StateMachine.GetDatabaseNames(context))
                    {
                        using (var databaseRecord = _engine.StateMachine.ReadRawDatabase(context, database, out long etag))
                        {
                            if (databaseRecord == null)
                            {
                                LogMessage($"Can't analyze the stats of database the {database}, because the database record is null.");
                                continue;
                            }

                            var databaseTopology = _engine.StateMachine.ReadDatabaseTopology(databaseRecord);
                            var topologyStamp = databaseTopology?.Stamp ?? new LeaderStamp
                            {
                                Index = -1,
                                LeadersTicks = -1,
                                Term = -1
                            };
                            var graceIfLeaderChanged = _term > topologyStamp.Term && currentLeader.LeaderShipDuration < _stabilizationTimeMs;
                            var letStatsBecomeStable = _term == topologyStamp.Term &&
                                (currentLeader.LeaderShipDuration - topologyStamp.LeadersTicks < _stabilizationTimeMs);
                            if (graceIfLeaderChanged || letStatsBecomeStable)
                            {
                                LogMessage($"We give more time for the '{database}' stats to become stable, so we skip analyzing it for now.");
                                continue;
                            }

                            var state = new DatabaseObservationState
                            {
                                Name = database,
                                DatabaseTopology = databaseTopology,
                                ClusterTopology = clusterTopology,
                                Current = newStats,
                                Previous = prevStats,

                                RawDatabase = databaseRecord,
                            };

                            if (state.ReadDatabaseDisabled() == true)
                                continue;

                            var updateReason = UpdateDatabaseTopology(state, ref deletions);
                            if (updateReason != null)
                            {
                                AddToDecisionLog(database, updateReason);

                                var cmd = new UpdateTopologyCommand(database)
                                {
                                    Topology = databaseTopology,
                                    RaftCommandIndex = etag
                                };

                                updateCommands.Add((cmd, updateReason));
                            }

                            var cleanUp = CleanUpDatabaseValues(state);
                            if (cleanUp != null)
                            {
                                if (cleanUpState == null)
                                    cleanUpState = new Dictionary<string, long>();

                                AddToDecisionLog(database, $"Should clean up values up to raft index {cleanUp}.");
                                cleanUpState.Add(database, cleanUp.Value);
                            }

                            if (cleanupIndexes)
                            {
                                await CleanUpUnusedAutoIndexes(state);
                            }
                        }
                    }

                    if (cleanupIndexes)
                        _lastIndexCleanupTimeInTicks = now.Ticks;
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
                    catch (ConcurrencyException)
                    {
                        // this is sort of expected, if the database was
                        // modified by someone else, we'll avoid changing
                        // it and run the logic again on the next round
                    }
                }
                if (deletions != null)
                {
                    foreach (var command in deletions)
                    {
                        AddToDecisionLog(command.DatabaseName,
                             $"We reached the replication factor on '{command.DatabaseName}', so we try to remove promotables/rehabs from: {string.Join(", ", command.FromNodes)}");

                        await Delete(command);
                    }
                }

                if (cleanUpState != null)
                {
                    var cmd = new CleanUpClusterStateCommand
                    {
                        ClusterTransactionsCleanup = cleanUpState
                    };

                    if (_engine.LeaderTag != _server.NodeTag)
                    {
                        throw new NotLeadingException("This node is no longer the leader, so abort the cleaning.");
                    }

                    await _engine.PutAsync(cmd);
                }
            }
        }

        internal async Task CleanUpUnusedAutoIndexes(DatabaseObservationState databaseState)
        {
            if (databaseState.DatabaseTopology.Count != databaseState.Current.Count)
                return;

            var indexes = new Dictionary<string, TimeSpan>();

            var lowestDatabaseUptime = TimeSpan.MaxValue;
            var newestIndexQueryTime = TimeSpan.MaxValue;

            foreach (var node in databaseState.DatabaseTopology.AllNodes)
            {
                if (databaseState.Current.TryGetValue(node, out var nodeReport) == false)
                    return;

                if (nodeReport.Report.TryGetValue(databaseState.Name, out var report) == false)
                    return;

                if (report.UpTime.HasValue && lowestDatabaseUptime > report.UpTime)
                    lowestDatabaseUptime = report.UpTime.Value;

                foreach (var kvp in report.LastIndexStats)
                {
                    var lastQueried = kvp.Value.LastQueried;
                    if (lastQueried.HasValue == false)
                        continue;

                    if (newestIndexQueryTime > lastQueried.Value)
                        newestIndexQueryTime = lastQueried.Value;

                    var indexName = kvp.Key;
                    if (indexName.StartsWith("Auto/", StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    if (indexes.TryGetValue(indexName, out var lq) == false || lq > lastQueried)
                    {
                        indexes[indexName] = lastQueried.Value;
                    }
                }
            }

            if (indexes.Count == 0)
                return;

            var settings = databaseState.ReadSettings();
            var timeToWaitBeforeMarkingAutoIndexAsIdle = (TimeSetting)RavenConfiguration.GetValue(x => x.Indexing.TimeToWaitBeforeMarkingAutoIndexAsIdle, _server.Configuration, settings);
            var timeToWaitBeforeDeletingAutoIndexMarkedAsIdle = (TimeSetting)RavenConfiguration.GetValue(x => x.Indexing.TimeToWaitBeforeDeletingAutoIndexMarkedAsIdle, _server.Configuration, settings);

            foreach (var kvp in indexes)
            {
                TimeSpan difference;
                if (lowestDatabaseUptime > kvp.Value)
                    difference = kvp.Value;
                else
                {
                    difference = kvp.Value - newestIndexQueryTime;
                    if (difference == TimeSpan.Zero && lowestDatabaseUptime > kvp.Value)
                        difference = kvp.Value;
                }

                var state = IndexState.Normal;
                if (databaseState.TryGetAutoIndex(kvp.Key, out var definition) && definition.State.HasValue)
                    state = definition.State.Value;

                if (state == IndexState.Idle && difference >= timeToWaitBeforeDeletingAutoIndexMarkedAsIdle.AsTimeSpan)
                {
                    await _engine.PutAsync(new DeleteIndexCommand(kvp.Key, databaseState.Name));

                    AddToDecisionLog(databaseState.Name, $"Deleting idle auto-index '{kvp.Key}' because last query time value is '{difference}' and threshold is set to '{timeToWaitBeforeDeletingAutoIndexMarkedAsIdle.AsTimeSpan}'.");

                    continue;
                }

                if (state == IndexState.Normal && difference >= timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan)
                {
                    await _engine.PutAsync(new SetIndexStateCommand(kvp.Key, IndexState.Idle, databaseState.Name));

                    AddToDecisionLog(databaseState.Name, $"Marking auto-index '{kvp.Key}' as idle because last query time value is '{difference}' and threshold is set to '{timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan}'.");

                    continue;
                }

                if (state == IndexState.Idle && difference < timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan)
                {
                    await _engine.PutAsync(new SetIndexStateCommand(kvp.Key, IndexState.Normal, databaseState.Name));

                    AddToDecisionLog(databaseState.Name, $"Marking idle auto-index '{kvp.Key}' as normal because last query time value is '{difference}' and threshold is set to '{timeToWaitBeforeMarkingAutoIndexAsIdle.AsTimeSpan}'.");
                }
            }
        }

        private long? CleanUpDatabaseValues(DatabaseObservationState state)
        {
            if (ClusterCommandsVersionManager.CurrentClusterMinimalVersion <
                ClusterCommandsVersionManager.ClusterCommandsVersions[nameof(CleanUpClusterStateCommand)])
            {
                return null;
            }

            if (state.DatabaseTopology.Count != state.Current.Count)
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

        private void AddToDecisionLog(string database, string updateReason)
        {
            if (_decisionsLog.Count > 99)
                _decisionsLog.Take();

            _decisionsLog.Add(new ClusterObserverLogEntry
            {
                Database = database,
                Iteration = _iteration,
                Message = updateReason,
                Date = DateTime.UtcNow
            });
        }

        private const string ThingsToCheck = "Things you may check: verify the remote node is working and check for ports (HTTP and TCP) being blocked by the firewall or similar software. If ServerUrl.Tcp is *not* configured, RavenDB is using port 38888 for TCP communications and it should be allowed in the firewall.";

        private void RaiseNoLivingNodesAlert(string alertMsg, string dbName)
        {
            var alert = AlertRaised.Create(
                dbName,
                $"Could not reach any node of '{dbName}' database",
                $"{alertMsg}. {ThingsToCheck}",
                AlertType.DatabaseTopologyWarning,
                NotificationSeverity.Warning
            );

            NotificationCenter.Add(alert, updateExisting: false);
            LogMessage(alertMsg, info: false);
        }

        private void RaiseNodeNotFoundAlert(string alertMsg, string node)
        {
            var alert = AlertRaised.Create(
                null,
                $"Node {node} not found.",
                $"{alertMsg}",
                AlertType.DatabaseTopologyWarning,
                NotificationSeverity.Warning
            );

            NotificationCenter.Add(alert, updateExisting: false);
            LogMessage(alertMsg, info: false);
        }

        private string UpdateDatabaseTopology(DatabaseObservationState state, ref List<DeleteDatabaseCommand> deletions)
        {
            var hasLivingNodes = false;

            var databaseTopology = state.DatabaseTopology;
            var current = state.Current;
            var previous = state.Previous;
            var dbName = state.Name;
            var clusterTopology = state.ClusterTopology;
            var deletionInProgress = state.ReadDeletionInProgress();

            foreach (var member in databaseTopology.Members)
            {
                var status = None;
                if (current.TryGetValue(member, out var nodeStats) == false)
                {
                    // there isn't much we can do here, except for log it.
                    if (previous.TryGetValue(member, out _))
                    {
                        // if we found this node in the previous report, we will ignore it this time and wait for the next report.
                        continue;
                    }

                    var msg =
                        $"The member node {member} was not found in both current and previous reports of the cluster observer. " +
                        $"If this error continue to raise, check the latency between the cluster nodes.";
                    LogMessage(msg);
                    RaiseNodeNotFoundAlert(msg, member);
                    continue;
                }

                if (nodeStats.Status == ClusterNodeStatusReport.ReportStatus.Ok &&
                    nodeStats.Report.TryGetValue(dbName, out var dbStats))
                {
                    status = dbStats.Status;
                    if (status == Loaded ||
                        status == Loading ||
                        status == Unloaded ||
                        status == NoChange)
                    {
                        hasLivingNodes = true;

                        if (databaseTopology.PromotablesStatus.TryGetValue(member, out var _))
                        {
                            databaseTopology.DemotionReasons.Remove(member);
                            databaseTopology.PromotablesStatus.Remove(member);
                            return $"Node {member} is online";
                        }
                        continue;
                    }
                }

                // Give one minute of grace before we move the node to a rehab
                if (DateTime.UtcNow.AddMilliseconds(-_moveToRehabTime) < current[member]?.LastSuccessfulUpdateDateTime)
                {
                    continue;
                }

                if (TryMoveToRehab(dbName, databaseTopology, current, member))
                    return $"Node {member} is currently not responding (with status: {status}) and moved to rehab";

                // database distribution is off and the node is down
                if (databaseTopology.DynamicNodesDistribution == false && (
                    databaseTopology.PromotablesStatus.TryGetValue(member, out var currentStatus) == false
                    || currentStatus != DatabasePromotionStatus.NotResponding))
                {
                    databaseTopology.DemotionReasons[member] = "Not responding";
                    databaseTopology.PromotablesStatus[member] = DatabasePromotionStatus.NotResponding;
                    return $"Node {member} is currently not responding with the status '{status}'";
                }
            }

            if (hasLivingNodes == false)
            {
                var recoverable = new List<string>();
                foreach (var rehab in databaseTopology.Rehabs)
                {
                    if (FailedDatabaseInstanceOrNode(clusterTopology, rehab, dbName, current) == DatabaseHealth.Good)
                        recoverable.Add(rehab);
                }

                if (recoverable.Count > 0)
                {
                    var node = FindMostUpToDateNode(recoverable, dbName, current);
                    databaseTopology.Rehabs.Remove(node);
                    databaseTopology.Members.Add(node);

                    RaiseNoLivingNodesAlert($"None of '{dbName}' database nodes are responding to the supervisor, promoting {node} from rehab to avoid making the database completely unreachable.", dbName);
                    return $"None of '{dbName}' nodes are responding, promoting {node} from rehab";
                }

                if (databaseTopology.Members.Count == 0 && deletionInProgress?.Count > 0)
                {
                    return null; // We delete the whole database.
                }

                RaiseNoLivingNodesAlert($"None of '{dbName}' database nodes are responding to the supervisor, the database is unreachable.", dbName);
            }

            var shouldUpdateTopologyStatus = false;
            var updateTopologyStatusReason = new StringBuilder();

            foreach (var promotable in databaseTopology.Promotables)
            {
                if (FailedDatabaseInstanceOrNode(clusterTopology, promotable, dbName, current) == DatabaseHealth.Bad)
                {
                    // database distribution is off and the node is down
                    if (databaseTopology.DynamicNodesDistribution == false)
                    {
                        if (databaseTopology.PromotablesStatus.TryGetValue(promotable, out var currentStatus) == false
                            || currentStatus != DatabasePromotionStatus.NotResponding)
                        {
                            databaseTopology.DemotionReasons[promotable] = "Not responding";
                            databaseTopology.PromotablesStatus[promotable] = DatabasePromotionStatus.NotResponding;
                            return $"Node {promotable} is currently not responding";
                        }
                        continue;
                    }

                    if (TryFindFitNode(promotable, dbName, databaseTopology, clusterTopology, current, out var node) == false)
                    {
                        if (databaseTopology.PromotablesStatus.TryGetValue(promotable, out var currentStatus) == false
                            || currentStatus != DatabasePromotionStatus.NotResponding)
                        {
                            databaseTopology.DemotionReasons[promotable] = "Not responding";
                            databaseTopology.PromotablesStatus[promotable] = DatabasePromotionStatus.NotResponding;
                            return $"Node {promotable} is currently not responding";
                        }
                        continue;
                    }

                    if (_server.LicenseManager.CanDynamicallyDistributeNodes(out _) == false)
                        continue;

                    // replace the bad promotable otherwise we will continue to add more and more nodes.
                    databaseTopology.Promotables.Add(node);
                    databaseTopology.DemotionReasons[node] = $"Just replaced the promotable node {promotable}";
                    databaseTopology.PromotablesStatus[node] = DatabasePromotionStatus.WaitingForFirstPromotion;
                    var deletionCmd = new DeleteDatabaseCommand
                    {
                        ErrorOnDatabaseDoesNotExists = false,
                        DatabaseName = dbName,
                        FromNodes = new[] { promotable },
                        HardDelete = _hardDeleteOnReplacement,
                        UpdateReplicationFactor = false
                    };

                    if (deletions == null)
                        deletions = new List<DeleteDatabaseCommand>();
                    deletions.Add(deletionCmd);
                    return $"The promotable {promotable} is not responsive, replace it with a node {node}";
                }

                if (TryGetMentorNode(dbName, databaseTopology, clusterTopology, promotable, out var mentorNode) == false)
                    continue;

                var tryPromote = TryPromote(dbName, databaseTopology, current, previous, mentorNode, promotable);
                if (tryPromote.Promote)
                {
                    databaseTopology.Promotables.Remove(promotable);
                    databaseTopology.Members.Add(promotable);
                    databaseTopology.PredefinedMentors.Remove(promotable);
                    RemoveOtherNodesIfNeeded(state, ref deletions);
                    return $"Promoting node {promotable} to member";
                }
                if (tryPromote.UpdateTopologyReason != null)
                {
                    shouldUpdateTopologyStatus = true;
                    updateTopologyStatusReason.AppendLine(tryPromote.UpdateTopologyReason);
                }
            }

            var goodMembers = GetNumberOfRespondingNodes(clusterTopology, dbName, databaseTopology, current);
            var pendingDelete = GetPendingDeleteNodes(deletionInProgress);
            foreach (var rehab in databaseTopology.Rehabs)
            {
                var health = FailedDatabaseInstanceOrNode(clusterTopology, rehab, dbName, current);
                switch (health)
                {
                    case DatabaseHealth.Bad:
                        if (databaseTopology.DynamicNodesDistribution == false)
                            continue;

                        if (goodMembers < databaseTopology.ReplicationFactor &&
                            TryFindFitNode(rehab, dbName, databaseTopology, clusterTopology, current, out var node))
                        {
                            if (_server.LicenseManager.CanDynamicallyDistributeNodes(out _) == false)
                                continue;

                            databaseTopology.Promotables.Add(node);
                            databaseTopology.DemotionReasons[node] = $"Maintain the replication factor and create new replica instead of node {rehab}";
                            databaseTopology.PromotablesStatus[node] = DatabasePromotionStatus.WaitingForFirstPromotion;
                            return $"The rehab node {rehab} was too long in rehabilitation, create node {node} to replace it";
                        }

                        if (databaseTopology.PromotablesStatus.TryGetValue(rehab, out var status) == false || status != DatabasePromotionStatus.NotResponding)
                        {
                            // was already online, but now we lost the connection again
                            if (TryMoveToRehab(dbName, databaseTopology, current, rehab))
                            {
                                return $"Node {rehab} is currently not responding";
                            }
                        }

                        break;
                    case DatabaseHealth.Good:

                        if (pendingDelete.Contains(rehab) && databaseTopology.PromotablesStatus.ContainsKey(rehab) == false)
                        {
                            // already tried to promote, so we just ignore and continue
                            continue;
                        }

                        if (TryGetMentorNode(dbName, databaseTopology, clusterTopology, rehab, out var mentorNode) == false)
                            continue;

                        var tryPromote = TryPromote(dbName, databaseTopology, current, previous, mentorNode, rehab);
                        if (tryPromote.Promote)
                        {
                            LogMessage($"The database {dbName} on {rehab} is reachable and up to date, so we promote it back to member.", info: false);

                            databaseTopology.Members.Add(rehab);
                            databaseTopology.Rehabs.Remove(rehab);
                            RemoveOtherNodesIfNeeded(state, ref deletions);
                            return $"Node {rehab} was recovered from rehabilitation and promoted back to member";
                        }
                        if (tryPromote.UpdateTopologyReason != null)
                        {
                            shouldUpdateTopologyStatus = true;
                            updateTopologyStatusReason.AppendLine(tryPromote.UpdateTopologyReason);
                        }
                        break;
                }
            }
            RemoveOtherNodesIfNeeded(state, ref deletions);

            if (shouldUpdateTopologyStatus)
            {
                return updateTopologyStatusReason.ToString();
            }

            return null;
        }

        private int GetNumberOfRespondingNodes(ClusterTopology clusterTopology, string dbName, DatabaseTopology topology, Dictionary<string, ClusterNodeStatusReport> current)
        {
            var goodMembers = topology.Members.Count;
            foreach (var promotable in topology.Promotables)
            {
                if (FailedDatabaseInstanceOrNode(clusterTopology, promotable, dbName, current) != DatabaseHealth.Bad)
                    goodMembers++;
            }
            foreach (var rehab in topology.Rehabs)
            {
                if (FailedDatabaseInstanceOrNode(clusterTopology, rehab, dbName, current) != DatabaseHealth.Bad)
                    goodMembers++;
            }
            return goodMembers;
        }

        private bool TryMoveToRehab(string dbName, DatabaseTopology topology, Dictionary<string, ClusterNodeStatusReport> current, string member)
        {
            DatabaseStatusReport dbStats = null;
            if (current.TryGetValue(member, out var nodeStats) &&
                nodeStats.Status == ClusterNodeStatusReport.ReportStatus.Ok &&
                nodeStats.Report.TryGetValue(dbName, out dbStats) && dbStats.Status != Faulted)
            {
                return false;
            }

            string reason;
            if (nodeStats == null)
            {
                reason = "Node in rehabilitation due to no status report in the latest cluster stats";
            }
            else if (nodeStats.Status != ClusterNodeStatusReport.ReportStatus.Ok)
            {
                switch (nodeStats.Status)
                {
                    case ClusterNodeStatusReport.ReportStatus.Timeout:
                        reason = $"Node in rehabilitation due to timeout reached trying to get stats from node.{Environment.NewLine}";
                        break;

                    default:
                        reason = $"Node in rehabilitation due to last report status being '{nodeStats.Status}'.{Environment.NewLine}";
                        break;
                }
            }
            else if (nodeStats.Report.TryGetValue(dbName, out var stats) && stats.Status == Faulted)
            {
                reason = $"In rehabilitation because the DatabaseStatus for this node is {nameof(Faulted)}.{Environment.NewLine}";
            }
            else
            {
                reason = $"In rehabilitation because the node is reachable but had no report about the database.{Environment.NewLine}";
            }

            if (nodeStats?.Error != null)
            {
                reason += $". {nodeStats.Error}";
            }
            if (dbStats?.Error != null)
            {
                reason += $". {dbStats.Error}";
            }

            if (topology.Rehabs.Contains(member) == false)
            {
                topology.Members.Remove(member);
                topology.Rehabs.Add(member);
            }

            topology.DemotionReasons[member] = reason;
            topology.PromotablesStatus[member] = DatabasePromotionStatus.NotResponding;

            LogMessage($"Node {member} of database '{dbName}': {reason}", info: false);

            return true;
        }

        private bool TryGetMentorNode(string dbName, DatabaseTopology topology, ClusterTopology clusterTopology, string promotable, out string mentorNode)
        {
            var url = clusterTopology.GetUrlFromTag(promotable);
            topology.PredefinedMentors.TryGetValue(promotable, out var mentor);
            var task = new PromotableTask(promotable, url, dbName, mentor);
            mentorNode = topology.WhoseTaskIsIt(_server.Engine.CurrentState, task, null);

            if (mentorNode == null)
            {
                // We are in passive mode and were kicked out of the cluster.
                return false;
            }

            return true;
        }

        private (bool Promote, string UpdateTopologyReason) TryPromote(string dbName, DatabaseTopology topology, Dictionary<string, ClusterNodeStatusReport> current, Dictionary<string, ClusterNodeStatusReport> previous, string mentorNode, string promotable)
        {
            if (previous.TryGetValue(mentorNode, out var mentorPrevClusterStats) == false ||
                mentorPrevClusterStats.Report.TryGetValue(dbName, out var mentorPrevDbStats) == false)
                return (false, null);

            if (previous.TryGetValue(promotable, out var promotablePrevClusterStats) == false ||
                promotablePrevClusterStats.Report.TryGetValue(dbName, out var promotablePrevDbStats) == false)
                return (false, null);

            if (current.TryGetValue(mentorNode, out var mentorCurrClusterStats) == false ||
                mentorCurrClusterStats.Report.TryGetValue(dbName, out var mentorCurrDbStats) == false)
                return (false, null);

            if (current.TryGetValue(promotable, out var promotableClusterStats) == false ||
                promotableClusterStats.Report.TryGetValue(dbName, out var promotableDbStats) == false)
                return (false, null);

            if (topology.Members.Count == topology.ReplicationFactor)
            {
                return (false, null);
            }

            var mentorsEtag = mentorPrevDbStats.LastEtag;
            if (mentorCurrDbStats.LastSentEtag.TryGetValue(promotable, out var lastSentEtag) == false)
            {
                return (false, null);
            }

            var timeDiff = mentorCurrClusterStats.LastSuccessfulUpdateDateTime - mentorPrevClusterStats.LastSuccessfulUpdateDateTime;

            if (lastSentEtag < mentorsEtag || timeDiff > 3 * _supervisorSamplePeriod)
            {
                var msg = $"The database '{dbName}' on {promotable} not ready to be promoted, because the mentor hasn't sent all of the documents yet." + Environment.NewLine +
                          $"Last sent Etag: {lastSentEtag:#,#;;0}" + Environment.NewLine +
                          $"Mentor's Etag: {mentorsEtag:#,#;;0}";
                LogMessage(msg);

                if (msg.Equals(topology.DemotionReasons[promotable]) == false)
                {
                    topology.DemotionReasons[promotable] = msg;
                    topology.PromotablesStatus[promotable] = DatabasePromotionStatus.ChangeVectorNotMerged;
                    return (false, msg);
                }
                return (false, null);
            }

            var indexesCatchedUp = CheckIndexProgress(promotablePrevDbStats.LastEtag, promotablePrevDbStats.LastIndexStats, promotableDbStats.LastIndexStats,
                mentorCurrDbStats.LastIndexStats);
            if (indexesCatchedUp)
            {
                LogMessage($"We try to promoted the database '{dbName}' on {promotable} to be a full member", info: false);

                topology.PromotablesStatus.Remove(promotable);
                topology.DemotionReasons.Remove(promotable);

                return (true, $"Node {promotable} is up-to-date so promoting it to be member");
            }
            LogMessage($"The database '{dbName}' on {promotable} is not ready to be promoted, because the indexes are not up-to-date.{Environment.NewLine}");

            if (topology.PromotablesStatus.TryGetValue(promotable, out var currentStatus) == false
                || currentStatus != DatabasePromotionStatus.IndexNotUpToDate)
            {
                var msg = $"Node {promotable} not ready to be a member, because the indexes are not up-to-date";
                topology.PromotablesStatus[promotable] = DatabasePromotionStatus.IndexNotUpToDate;
                topology.DemotionReasons[promotable] = msg;
                return (false, msg);
            }
            return (false, null);
        }

        private void RemoveOtherNodesIfNeeded(DatabaseObservationState state, ref List<DeleteDatabaseCommand> deletions)
        {
            var topology = state.DatabaseTopology;
            var dbName = state.Name;
            var clusterTopology = state.ClusterTopology;

            if (topology.Members.Count < topology.ReplicationFactor)
                return;

            if (topology.Promotables.Count == 0 &&
                topology.Rehabs.Count == 0)
                return;

            LogMessage("We reached the replication factor, so we try to remove redundant nodes.", info: false);

            var nodesToDelete = new List<string>();
            var mentorChangeVector = new Dictionary<string, string>();

            foreach (var node in topology.Promotables.Concat(topology.Rehabs))
            {
                if (TryGetMentorNode(dbName, topology, clusterTopology, node, out var mentorNode) == false ||
                    state.Current.TryGetValue(mentorNode, out var mentorStats) == false ||
                    mentorStats.Report.TryGetValue(dbName, out var dbReport) == false)
                {
                    continue;
                }
                if (state.ReadDeletionInProgress()?.ContainsKey(node) == true)
                {
                    continue;
                }
                nodesToDelete.Add(node);
                mentorChangeVector.Add(node, dbReport.DatabaseChangeVector);
            }

            if (nodesToDelete.Count > 0)
            {
                var deletionCmd = new DeleteDatabaseCommand
                {
                    ErrorOnDatabaseDoesNotExists = false,
                    DatabaseName = dbName,
                    FromNodes = nodesToDelete.ToArray(),
                    HardDelete = _hardDeleteOnReplacement,
                    UpdateReplicationFactor = false,
                };

                if (deletions == null)
                    deletions = new List<DeleteDatabaseCommand>();
                deletions.Add(deletionCmd);
            }
        }

        private static List<string> GetPendingDeleteNodes(Dictionary<string, DeletionInProgressStatus> deletionInProgress)
        {
            var alreadyInDeletionProgress = new List<string>();
            alreadyInDeletionProgress.AddRange(deletionInProgress?.Keys);
            return alreadyInDeletionProgress;
        }

        private enum DatabaseHealth
        {
            NotEnoughInfo,
            Bad,
            Good
        }

        private DatabaseHealth FailedDatabaseInstanceOrNode(
            ClusterTopology clusterTopology,
            string node,
            string db,
            Dictionary<string, ClusterNodeStatusReport> current)
        {
            if (clusterTopology.Contains(node) == false) // this node is no longer part of the *Cluster* databaseTopology and need to be replaced.
                return DatabaseHealth.Bad;

            var hasCurrent = current.TryGetValue(node, out var currentNodeStats);

            // Wait until we have more info
            if (hasCurrent == false)
                return DatabaseHealth.NotEnoughInfo;

            // if server is down we should reassign
            if (DateTime.UtcNow - currentNodeStats.LastSuccessfulUpdateDateTime > _breakdownTimeout)
                return DatabaseHealth.Bad;

            if (currentNodeStats.LastGoodDatabaseStatus.TryGetValue(db, out var lastGoodTime) == false)
            {
                // here we have a problem, the databaseTopology says that the db needs to be in the node, but the node
                // doesn't know that the db is on it, that probably indicate some problem and we'll move it
                // to another node to resolve it.
                return DatabaseHealth.NotEnoughInfo;
            }
            if (lastGoodTime == default(DateTime) || lastGoodTime == DateTime.MinValue)
                return DatabaseHealth.NotEnoughInfo;

            return DateTime.UtcNow - lastGoodTime > _breakdownTimeout ? DatabaseHealth.Bad : DatabaseHealth.Good;
        }

        private bool TryFindFitNode(string badNode, string db, DatabaseTopology topology, ClusterTopology clusterTopology,
            Dictionary<string, ClusterNodeStatusReport> current, out string bestNode)
        {
            bestNode = null;
            var dbCount = int.MaxValue;
            var databaseNodes = topology.AllNodes.ToList();

            if (topology.Members.Count == 0) // no one can be used as mentor
                return false;

            foreach (var node in clusterTopology.AllNodes.Keys)
            {

                if (databaseNodes.Contains(node))
                    continue;

                if (FailedDatabaseInstanceOrNode(clusterTopology, node, db, current) == DatabaseHealth.Bad)
                    continue;

                if (current.TryGetValue(node, out var nodeReport) == false)
                {
                    if (bestNode == null)
                        bestNode = node;
                    continue;
                }

                if (dbCount > nodeReport.Report.Count)
                {
                    dbCount = nodeReport.Report.Count;
                    bestNode = node;
                }
            }

            if (bestNode == null)
            {
                LogMessage($"The database '{db}' on {badNode} has not responded for a long time, but there is no free node to reassign it.", info: false);
                return false;
            }
            LogMessage($"The database '{db}' on {badNode} has not responded for a long time, so we reassign it to {bestNode}.", info: false);

            return true;
        }

        private string FindMostUpToDateNode(List<string> nodes, string database, Dictionary<string, ClusterNodeStatusReport> current)
        {
            var updated = nodes[0];
            var highestChangeVectors = current[updated].Report[database].DatabaseChangeVector;
            var maxDocsCount = current[updated].Report[database].NumberOfDocuments;
            for (var index = 1; index < nodes.Count; index++)
            {
                var node = nodes[index];
                var report = current[node].Report[database];
                var cv = report.DatabaseChangeVector;
                var status = ChangeVectorUtils.GetConflictStatus(cv, highestChangeVectors);
                if (status == ConflictStatus.Update)
                {
                    highestChangeVectors = cv;
                }
                // In conflict we need to choose between 2 nodes that are not synced. 
                // So we take the one with the most documents.  
                if (status == ConflictStatus.Conflict)
                {
                    if (report.NumberOfDocuments > maxDocsCount)
                    {
                        highestChangeVectors = cv;
                        maxDocsCount = report.NumberOfDocuments;
                        updated = node;
                    }
                }
            }
            return updated;
        }

        private static bool CheckIndexProgress(
            long lastPrevEtag,
            Dictionary<string, DatabaseStatusReport.ObservedIndexStatus> previous,
            Dictionary<string, DatabaseStatusReport.ObservedIndexStatus> current,
            Dictionary<string, DatabaseStatusReport.ObservedIndexStatus> mentor)
        {
            /*
            Here we are being a bit tricky. A database node is consider ready for promotion when
            it's replication is one cycle behind its mentor, but there are still indexes to consider.

            If we just replicate a whole bunch of stuff and indexes are catching up, we want to only
            promote when the indexes actually caught up. We do that by also requiring that all indexes
            will be either fully caught up (non stale) or that they are at most a single cycle behind.

            This is check by looking at the global etag from the previous round, and comparing it to the 
            last etag that each index indexed in the current round. Note that technically, we need to compare
            on a per collection basis, but we can avoid it by noting that if the collection's last etag is
            not beyond the previous max etag, then the index will therefor not be non stale.


             */


            foreach (var mentorIndex in mentor)
            {
                // we go over all of the mentor indexes to validated that the promotable has them.
                // Since we don't save in the state machine the definition of side-by-side indexes, we will skip them, because
                // the promotable don't have them. 

                if (mentorIndex.Value.IsSideBySide)
                    continue;

                if (mentor.TryGetValue(Constants.Documents.Indexing.SideBySideIndexNamePrefix + mentorIndex.Key, out var mentorIndexStats) == false)
                {
                    mentorIndexStats = mentorIndex.Value;
                }

                if (previous.TryGetValue(mentorIndex.Key, out var _) == false)
                    return false;

                if (current.TryGetValue(mentorIndex.Key, out var currentIndexStats) == false)
                    return false;

                if (currentIndexStats.IsStale == false)
                    continue;

                if (mentorIndexStats.LastIndexedEtag == (long)Index.IndexProgressStatus.Faulty)
                {
                    continue; // skip the check for faulty indexes
                }

                if (mentorIndexStats.State == IndexState.Error && currentIndexStats.State == IndexState.Error)
                    continue;

                var lastIndexEtag = currentIndexStats.LastIndexedEtag;
                if (lastPrevEtag > lastIndexEtag)
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

            return _engine.PutAsync(cmd);
        }

        private Task<(long Index, object Result)> Delete(DeleteDatabaseCommand cmd)
        {
            if (_engine.LeaderTag != _server.NodeTag)
            {
                throw new NotLeadingException("This node is no longer the leader, so we abort the deletion command");
            }
            return _engine.PutAsync(cmd);
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

        internal class DatabaseObservationState
        {
            public string Name;
            public DatabaseTopology DatabaseTopology;
            public Dictionary<string, ClusterNodeStatusReport> Current;
            public Dictionary<string, ClusterNodeStatusReport> Previous;
            public ClusterTopology ClusterTopology;

            public BlittableJsonReaderObject RawDatabase;

            public long ReadTruncatedClusterTransactionCommandsCount()
            {
                RawDatabase.TryGet(nameof(DatabaseRecord.TruncatedClusterTransactionCommandsCount), out long count);
                return count;
            }

            public bool TryGetAutoIndex(string name, out AutoIndexDefinition definition)
            {
                BlittableJsonReaderObject autoDefinition = null;
                definition = null;
                RawDatabase.TryGet(nameof(DatabaseRecord.AutoIndexes), out BlittableJsonReaderObject autoIndexes);
                if (autoIndexes?.TryGet(name, out autoDefinition) == false)
                    return false;

                definition = JsonDeserializationServer.AutoIndexDefinition(autoDefinition);
                return true;
            }

            public Dictionary<string, DeletionInProgressStatus> ReadDeletionInProgress()
            {
                if (RawDatabase.TryGet(nameof(DatabaseRecord.DeletionInProgress), out BlittableJsonReaderObject obj) == false || obj == null)
                    return null;

                var deletionInProgress = new Dictionary<string, DeletionInProgressStatus>();

                var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                foreach (var propertyIndex in obj.GetPropertiesByInsertionOrder())
                {
                    obj.GetPropertyByIndex(propertyIndex, ref propertyDetails);

                    if (propertyDetails.Value == null)
                        continue;

                    if (Enum.TryParse(propertyDetails.Value.ToString(), out DeletionInProgressStatus result))
                        deletionInProgress[propertyDetails.Name] = result;
                }

                return deletionInProgress;
            }

            public bool? ReadDatabaseDisabled()
            {
                if (RawDatabase.TryGet(nameof(DatabaseRecord.Disabled), out bool disabled) == false)
                    return null;

                return disabled;
            }

            public Dictionary<string, string> ReadSettings()
            {
                var settings = new Dictionary<string, string>();
                if (RawDatabase.TryGet(nameof(DatabaseRecord.Settings), out BlittableJsonReaderObject obj) == false || obj == null)
                    return settings;

                var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                foreach (var propertyIndex in obj.GetPropertiesByInsertionOrder())
                {
                    obj.GetPropertyByIndex(propertyIndex, ref propertyDetails);

                    settings[propertyDetails.Name] = propertyDetails.Value?.ToString();
                }

                return settings;
            }
        }
    }
}

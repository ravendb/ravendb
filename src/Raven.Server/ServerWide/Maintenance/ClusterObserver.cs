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
using Raven.Server.Documents.Indexes;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Utils;
using static Raven.Server.ServerWide.Maintenance.DatabaseStatus;

namespace Raven.Server.ServerWide.Maintenance
{
    class ClusterObserver : IDisposable
    {
        private readonly Task _observe;
        private readonly CancellationTokenSource _cts;
        private readonly ClusterMaintenanceSupervisor _maintenance;
        private readonly string _nodeTag;
        private readonly RachisConsensus<ClusterStateMachine> _engine;
        private readonly TransactionContextPool _contextPool;
        private readonly Logger _logger;

        public readonly TimeSpan SupervisorSamplePeriod;
        private readonly ServerStore _server;
        private readonly long _stabilizationTime;
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
            SupervisorSamplePeriod = config.SupervisorSamplePeriod.AsTimeSpan;
            _stabilizationTime = (long)config.StabilizationTime.AsTimeSpan.TotalMilliseconds;
            _moveToRehabTime = (long)config.MoveToRehabGraceTime.AsTimeSpan.TotalMilliseconds;
            _breakdownTimeout = config.AddReplicaTimeout.AsTimeSpan;
            _hardDeleteOnReplacement = config.HardDeleteOnReplacement;
            _observe = Run(_cts.Token);
        }

        public bool Suspended = false; // don't really care about concurrency here
        private readonly BlockingCollection<ClusterObserverLogEntry> _decisionsLog = new BlockingCollection<ClusterObserverLogEntry>();
        private long _iteration;
        private readonly long _term;
        private readonly long _moveToRehabTime;

        public (ClusterObserverLogEntry[] List, long Iteration) ReadDecisionsForDatabase()
        {
            return (_decisionsLog.ToArray(), _iteration);
        }

        public async Task Run(CancellationToken token)
        {
            // we give some time to populate the stats.
            await TimeoutManager.WaitFor(SupervisorSamplePeriod, token);
            var prevStats = _maintenance.GetStats();
            // wait before collecting the stats again.
            await TimeoutManager.WaitFor(SupervisorSamplePeriod, token);

            while (_term == _engine.CurrentTerm && token.IsCancellationRequested == false)
            {
                var delay = TimeoutManager.WaitFor(SupervisorSamplePeriod, token);
                try
                {
                    if (Suspended == false)
                    {
                        _iteration++;
                        var newStats = _maintenance.GetStats();
                        await AnalyzeLatestStats(newStats, prevStats);
                        prevStats = newStats;
                    }
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info($"An error occurred while analyzing maintenance stats on node {_nodeTag}.", e);
                    }
                }
                finally
                {
                    await delay;
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
                List<DeleteDatabaseCommand> deletions = null;
                using (context.OpenReadTransaction())
                {
                    var clusterTopology = _server.GetClusterTopology(context);
                    foreach (var database in _engine.StateMachine.GetDatabaseNames(context))
                    {
                        var databaseRecord = _engine.StateMachine.ReadDatabase(context, database, out long etag);
                        if (databaseRecord == null)
                        {
                            if (_logger.IsInfoEnabled)
                            {
                                _logger.Info($"Can't analyze the stats of database the {database}, because the database record is null.");
                            }
                            continue;
                        }
                        var topologyStamp = databaseRecord.Topology?.Stamp ?? new LeaderStamp
                        {
                            Index = -1,
                            LeadersTicks = -1,
                            Term = -1
                        };
                        var graceIfLeaderChanged = _term > topologyStamp.Term && currentLeader.LeaderShipDuration < _stabilizationTime;
                        var letStatsBecomeStable = _term == topologyStamp.Term &&
                            (currentLeader.LeaderShipDuration - topologyStamp.LeadersTicks < _stabilizationTime);
                        if (graceIfLeaderChanged || letStatsBecomeStable)
                        {
                            if (_logger.IsInfoEnabled)
                            {
                                _logger.Info($"We give more time for the '{database}' stats to become stable, so we skip analyzing it for now.");
                            }
                            continue;
                        }
                      
                        var updateReason = UpdateDatabaseTopology(database, databaseRecord, clusterTopology, newStats, prevStats, ref deletions);
                        if (updateReason != null)
                        {
                            AddToDecisionLog(database, updateReason);

                            var cmd = new UpdateTopologyCommand(database)
                            {
                                Topology = databaseRecord.Topology,
                                RaftCommandIndex = etag
                            };

                            updateCommands.Add((cmd, updateReason));
                        }
                    }
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
            }
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

        private const string ThingsToCheck = "Things you may check: verify node is working, check for ports being blocked by firewall or similar software.";

        private void RaiseNoLivingNodesAlert(string alertMsg, string dbName)
        {
            var alert = AlertRaised.Create(
                dbName,
                $"Could not reach any node of '{dbName}' database",
                $"{alertMsg}. {ThingsToCheck}",
                AlertType.DatabaseTopologyWarning,
                NotificationSeverity.Warning
            );

            NotificationCenter.Add(alert);
            if (_logger.IsOperationsEnabled)
            {
                _logger.Operations(alertMsg);
            }
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

            NotificationCenter.Add(alert);
            if (_logger.IsOperationsEnabled)
            {
                _logger.Operations(alertMsg);
            }
        }

        private string UpdateDatabaseTopology(string dbName, DatabaseRecord record, ClusterTopology clusterTopology,
            Dictionary<string, ClusterNodeStatusReport> current,
            Dictionary<string, ClusterNodeStatusReport> previous,
            ref List<DeleteDatabaseCommand> deletions)
        {
            if (record.Disabled)
                return null;

            var topology = record.Topology;
            var hasLivingNodes = false;
            foreach (var member in topology.Members)
            {
                var status = None;
                if(current.TryGetValue(member, out var nodeStats) == false)
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
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info(msg);
                        
                    }
                    RaiseNodeNotFoundAlert(msg, member);
                    continue;
                }
                if (nodeStats.Status == ClusterNodeStatusReport.ReportStatus.Ok &&
                    nodeStats.Report.TryGetValue(dbName, out var dbStats))
                {
                    status = dbStats.Status;
                    if (status == Loaded || 
                        status == Loading || 
                        status == Unloaded)
                    {
                        hasLivingNodes = true;

                        if (topology.PromotablesStatus.TryGetValue(member, out var _))
                        {
                            topology.DemotionReasons.Remove(member);
                            topology.PromotablesStatus.Remove(member);
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
                
                if (TryMoveToRehab(dbName, topology, current, member))
                    return $"Node {member} is currently not responding (with status: {status}) and moved to rehab";

                // database distribution is off and the node is down
                if (topology.DynamicNodesDistribution == false && (
                    topology.PromotablesStatus.TryGetValue(member, out var currentStatus) == false
                    || currentStatus != DatabasePromotionStatus.NotResponding))
                {
                    topology.DemotionReasons[member] = "Not responding";
                    topology.PromotablesStatus[member] = DatabasePromotionStatus.NotResponding;
                    return $"Node {member} is currently not responding with the status '{status}'";
                }
            }

            if (hasLivingNodes == false)
            {
                var recoverable = new List<string>();
                foreach (var rehab in topology.Rehabs)
                {
                    if (FailedDatabaseInstanceOrNode(clusterTopology, rehab, dbName, current) == DatabaseHealth.Good)
                        recoverable.Add(rehab);
                }

                if (recoverable.Count > 0)
                {
                    var node = FindMostUpToDateNode(recoverable, dbName, current);
                    topology.Rehabs.Remove(node);   
                    topology.Members.Add(node);

                    RaiseNoLivingNodesAlert($"None of '{dbName}' database nodes are responding to the supervisor, promoting {node} from rehab to avoid making the database completely unreachable.", dbName);
                    return $"None of '{dbName}' nodes are responding, promoting {node} from rehab";
                }

                if (topology.Members.Count == 0 && record.DeletionInProgress?.Count > 0)
                {
                    return null; // We delete the whole database.
                }

                RaiseNoLivingNodesAlert($"None of '{dbName}' database nodes are responding to the supervisor, the database is unreachable.", dbName);
            }

            var shouldUpdateTopologyStatus = false;
            var updateTopologyStatusReason = new StringBuilder();

            foreach (var promotable in topology.Promotables)
            {
                if (FailedDatabaseInstanceOrNode(clusterTopology, promotable, dbName, current) == DatabaseHealth.Bad)
                {
                    // database distribution is off and the node is down
                    if (topology.DynamicNodesDistribution == false)
                    {
                        if (topology.PromotablesStatus.TryGetValue(promotable, out var currentStatus) == false
                            || currentStatus != DatabasePromotionStatus.NotResponding)
                        {
                            topology.DemotionReasons[promotable] = "Not responding";
                            topology.PromotablesStatus[promotable] = DatabasePromotionStatus.NotResponding;
                            return $"Node {promotable} is currently not responding";
                        }
                        continue;
                    }

                    if (TryFindFitNode(promotable, dbName, topology, clusterTopology, current, out var node) == false)
                    {
                        if (topology.PromotablesStatus.TryGetValue(promotable, out var currentStatus) == false
                            || currentStatus != DatabasePromotionStatus.NotResponding)
                        {
                            topology.DemotionReasons[promotable] = "Not responding";
                            topology.PromotablesStatus[promotable] = DatabasePromotionStatus.NotResponding;
                            return $"Node {promotable} is currently not responding";
                        }
                        continue;
                    }

                    if (_server.LicenseManager.CanDynamicallyDistributeNodes(out _) == false)
                        continue;

                    // replace the bad promotable otherwise we will continue to add more and more nodes.
                    topology.Promotables.Add(node);
                    topology.DemotionReasons[node] = $"Just replaced the promotable node {promotable}";
                    topology.PromotablesStatus[node] = DatabasePromotionStatus.WaitingForFirstPromotion;
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

                if (TryGetMentorNode(dbName, topology, clusterTopology, promotable, out var mentorNode) == false)
                    continue;

                var tryPromote = TryPromote(dbName, topology, current, previous, mentorNode, promotable);
                if (tryPromote.Promote)
                {
                    topology.Promotables.Remove(promotable);
                    topology.Members.Add(promotable);
                    topology.PredefinedMentors.Remove(promotable);
                    RemoveOtherNodesIfNeeded(dbName, record, clusterTopology, current, ref deletions);
                    return $"Promoting node {promotable} to member";
                }
                if (tryPromote.UpdateTopologyReason != null)
                {
                    shouldUpdateTopologyStatus = true;
                    updateTopologyStatusReason.AppendLine(tryPromote.UpdateTopologyReason);
                }
            }
            
            var goodMembers = GetNumberOfRespondingNodes(clusterTopology, dbName, topology, current);
            var pendingDelete = GetPendingDeleteNodes(record);
            foreach (var rehab in topology.Rehabs)
            {
                var health = FailedDatabaseInstanceOrNode(clusterTopology, rehab, dbName, current);
                switch (health)
                {
                    case DatabaseHealth.Bad:
                        if (topology.DynamicNodesDistribution == false)
                            continue;

                        if (goodMembers < topology.ReplicationFactor &&
                            TryFindFitNode(rehab, dbName, topology, clusterTopology, current, out var node))
                        {
                            if (_server.LicenseManager.CanDynamicallyDistributeNodes(out _) == false)
                                continue;

                            topology.Promotables.Add(node);
                            topology.DemotionReasons[node] = $"Maintain the replication factor and create new replica instead of node {rehab}";
                            topology.PromotablesStatus[node] = DatabasePromotionStatus.WaitingForFirstPromotion;
                            return $"The rehab node {rehab} was too long in rehabilitation, create node {node} to replace it";
                        }

                        if (topology.PromotablesStatus.TryGetValue(rehab, out var status) == false || status != DatabasePromotionStatus.NotResponding)
                        {
                            // was already online, but now we lost the connection again
                            if (TryMoveToRehab(dbName, topology, current, rehab))
                            {
                                return $"Node {rehab} is currently not responding";
                            }
                        }

                        break;
                    case DatabaseHealth.Good:

                        if (pendingDelete.Contains(rehab) && topology.PromotablesStatus.ContainsKey(rehab) == false)
                        {
                            // already tried to promote, so we just ignore and continue
                            continue;
                        }

                        if (TryGetMentorNode(dbName, topology, clusterTopology, rehab, out var mentorNode) == false)
                            continue;

                        var tryPromote = TryPromote(dbName, topology, current, previous, mentorNode, rehab);
                        if (tryPromote.Promote)
                        {
                            if (_logger.IsOperationsEnabled)
                            {
                                _logger.Operations($"The database {dbName} on {rehab} is reachable and up to date, so we promote it back to member.");
                            }
                            
                            topology.Members.Add(rehab);
                            topology.Rehabs.Remove(rehab);
                            RemoveOtherNodesIfNeeded(dbName, record, clusterTopology, current, ref deletions);
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
            RemoveOtherNodesIfNeeded(dbName, record, clusterTopology, current, ref deletions);

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

            if (_logger.IsOperationsEnabled)
            {
                _logger.Operations($"Node {member} of database '{dbName}': {reason}");
            }

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

            if (lastSentEtag < mentorsEtag || timeDiff > 3 * SupervisorSamplePeriod)
            {
                var msg = $"The database '{dbName}' on {promotable} not ready to be promoted, because the mentor hasn't sent all of the documents yet." + Environment.NewLine +
                          $"Last sent Etag: {lastSentEtag:#,#;;0}" + Environment.NewLine +
                          $"Mentor's Etag: {mentorsEtag:#,#;;0}";
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info(msg);
                }

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
                if (_logger.IsOperationsEnabled)
                {
                    _logger.Operations($"We try to promoted the database '{dbName}' on {promotable} to be a full member");
                }
                topology.PromotablesStatus.Remove(promotable);
                topology.DemotionReasons.Remove(promotable);

                return (true, $"Node {promotable} is up-to-date so promoting it to be member");
            }
            if (_logger.IsInfoEnabled)
            {
                _logger.Info($"The database '{dbName}' on {promotable} is not ready to be promoted, because the indexes are not up-to-date." + Environment.NewLine);
            }

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

        private void RemoveOtherNodesIfNeeded(string dbName, DatabaseRecord record, ClusterTopology clusterTopology,
            Dictionary<string, ClusterNodeStatusReport> current, ref List<DeleteDatabaseCommand> deletions)
        {
            var topology = record.Topology;
            if (topology.Members.Count < topology.ReplicationFactor)
                return;

            if (topology.Promotables.Count == 0 &&
                topology.Rehabs.Count == 0)
                return;

            if (_logger.IsOperationsEnabled)
            {
                _logger.Operations("We reached the replication factor, so we try to remove redundant nodes.");
            }

            var nodesToDelete = new List<string>();
            var mentorChangeVector = new Dictionary<string,string>();

            foreach (var node in topology.Promotables.Concat(topology.Rehabs))
            {
                if (TryGetMentorNode(dbName, topology, clusterTopology, node, out var mentorNode) == false ||
                    current.TryGetValue(mentorNode, out var metorStats) == false ||
                    metorStats.Report.TryGetValue(dbName, out var dbReport) == false)
                {
                   continue; 
                }
                if (record.DeletionInProgress?.ContainsKey(node) == true)
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

        private static List<string> GetPendingDeleteNodes(DatabaseRecord record)
        {
            var alreadInDeletionProgress = new List<string>();
            alreadInDeletionProgress.AddRange(record.DeletionInProgress?.Keys);
            return alreadInDeletionProgress;
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
            if (clusterTopology.Contains(node) == false) // this node is no longer part of the *Cluster* topology and need to be replaced.
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
                // here we have a problem, the topology says that the db needs to be in the node, but the node
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
                if (_logger.IsOperationsEnabled)
                {
                    _logger.Operations($"The database '{db}' on {badNode} has not responded for a long time, but there is no free node to reassign it.");
                }
                return false;
            }
            if (_logger.IsOperationsEnabled)
            {
                _logger.Operations($"The database '{db}' on {badNode} has not responded for a long time, so we reassign it to {bestNode}.");
            }

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
                throw new NotLeadingException("This node is no longer the leader, so we abort updating the database topology");
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
                if (_observe.Wait(TimeSpan.FromSeconds(30)) == false)
                {
                    throw new ObjectDisposedException($"Cluster observer on node {_nodeTag} still running and can't be closed");
                }
            }
            finally
            {
                _cts.Dispose();
            }
        }
    }
}

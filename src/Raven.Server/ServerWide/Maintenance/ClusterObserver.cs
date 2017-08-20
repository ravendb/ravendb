using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
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
            TransactionContextPool contextPool,
            CancellationToken token)
        {
            _maintenance = maintenance;
            _nodeTag = server.NodeTag;
            _server = server;
            _engine = engine;
            _contextPool = contextPool;
            _logger = LoggingSource.Instance.GetLogger<ClusterObserver>(_nodeTag);
            _cts = CancellationTokenSource.CreateLinkedTokenSource(token);

            var config = server.Configuration.Cluster;
            SupervisorSamplePeriod = config.SupervisorSamplePeriod.AsTimeSpan;
            _stabilizationTime = (long)config.StabilizationTime.AsTimeSpan.TotalMilliseconds;
            _breakdownTimeout = config.AddReplicaTimeout.AsTimeSpan;
            _hardDeleteOnReplacement = config.HardDeleteOnReplacement;
            _observe = Run(_cts.Token);
        }

        public bool Suspended = false; // don't really care about concurrency here

        public static string FormatDecision(long iteration, string database, string message)
        {
            return $"{DateTime.UtcNow:o}, {iteration}, {database}, {message}";
        }

        private readonly BlockingCollection<string> _decisionsLog = new BlockingCollection<string>();
        private long _iteration;

        public (string[] List, long Iteration) ReadDecisionsForDatabase()
        {
            return (_decisionsLog.ToArray(), _iteration);
        }

        public async Task Run(CancellationToken token)
        {
            var prevStats = new Dictionary<string, ClusterNodeStatusReport>();
            while (token.IsCancellationRequested == false)
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

                    await delay;
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

        public async Task AnalyzeLatestStats(
            Dictionary<string, ClusterNodeStatusReport> newStats,
            Dictionary<string, ClusterNodeStatusReport> prevStats
            )
        {
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
                        var graceIfLeaderChanged = _engine.CurrentTerm > topologyStamp.Term && _engine.CurrentLeader.LeaderShipDuration < _stabilizationTime;
                        var letStatsBecomeStable = _engine.CurrentTerm == topologyStamp.Term &&
                            (_engine.CurrentLeader.LeaderShipDuration - topologyStamp.LeadersTicks < _stabilizationTime);
                        if (graceIfLeaderChanged || letStatsBecomeStable)
                        {
                            if (_logger.IsInfoEnabled)
                            {
                                _logger.Info($"We give more time for the {database} stats to become stable, so we skip analyzing it for now.");
                            }
                            continue;
                        }
                        var updateReason = UpdateDatabaseTopology(database, databaseRecord.Topology, clusterTopology, newStats, prevStats, ref deletions);
                        if (updateReason != null)
                        {
                            if (_decisionsLog.Count > 99)
                                _decisionsLog.Take();

                            _decisionsLog.Add(FormatDecision(_iteration, database, updateReason));

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
                        await Delete(command);
                    }
                }
            }
        }

        private void RaiseNoLivingNodesAlert(string alertMsg)
        {
            var alert = AlertRaised.Create(
                "No living nodes in the database topology",
                alertMsg,
                AlertType.DatabaseTopologyWarning,
                NotificationSeverity.Warning
            );

            NotificationCenter.Add(alert);
            if (_logger.IsOperationsEnabled)
            {
                _logger.Operations(alertMsg);
            }
        }
        private string UpdateDatabaseTopology(string dbName, DatabaseTopology topology, ClusterTopology clusterTopology,
            Dictionary<string, ClusterNodeStatusReport> current,
            Dictionary<string, ClusterNodeStatusReport> previous,
            ref List<DeleteDatabaseCommand> deletions)
        {
            var hasLivingNodes = false;
            foreach (var member in topology.Members)
            {
                if (current.TryGetValue(member, out var nodeStats) &&
                    nodeStats.Status == ClusterNodeStatusReport.ReportStatus.Ok &&
                    nodeStats.Report.TryGetValue(dbName, out var dbStats) &&
                    dbStats.Status == Loaded)
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

                if (TryMoveToRehab(dbName, topology, current, member))
                    return $"Node {member} is currently not responding and moved to rehab";

                // node distribution is off and the node is down
                if (topology.DynamicNodesDistribution == false && (
                    topology.PromotablesStatus.TryGetValue(member, out var currentStatus) == false
                    || currentStatus != DatabasePromotionStatus.NotResponding))
                {
                    topology.DemotionReasons[member] = "Not responding";
                    topology.PromotablesStatus[member] = DatabasePromotionStatus.NotResponding;
                    return $"Node {member} is currently not responding";
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
                    RaiseNoLivingNodesAlert($"It appears that all nodes of the {dbName} database are not responding to the supervisor, promoting {node} from rehab to avoid making the database completely unreachable");
                    return $"All nodes are not responding, promoting {node} from rehab";
                }
                RaiseNoLivingNodesAlert($"It appears that all nodes of the {dbName} database are not responding to the supervisor, the database is not reachable");
            }

            foreach (var promotable in topology.Promotables)
            {
                if (FailedDatabaseInstanceOrNode(clusterTopology, promotable, dbName, current) == DatabaseHealth.Bad)
                {
                    // node distribution is off and the node is down
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

                    //replace the bad promotable otherwise we will continute to add more and more nodes.
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
                    return $"Promoting node {promotable} to member";
                }
                if (tryPromote.UpdateTopologyReason != null)
                    return tryPromote.UpdateTopologyReason;
            }

            var goodMembers = GetNumberOfRespondingNodes(clusterTopology, dbName, topology, current);

            foreach (var rehab in topology.Rehabs)
            {
                var health = FailedDatabaseInstanceOrNode(clusterTopology, rehab, dbName, current);
                switch (health)
                {
                    case DatabaseHealth.Bad:
                        if(topology.DynamicNodesDistribution == false)
                            continue;

                        if (goodMembers < topology.ReplicationFactor &&
                            TryFindFitNode(rehab, dbName, topology, clusterTopology, current, out var node))
                        {
                            topology.Promotables.Add(node);
                            topology.DemotionReasons[node] = $"Maintaine the replication factor and create new rplica instead of node {rehab}";
                            topology.PromotablesStatus[node] = DatabasePromotionStatus.WaitingForFirstPromotion;
                            return $"The rehab node {rehab} was too long in rehabilitation, create node {node} to replace it";
                        }
                        break;
                    case DatabaseHealth.Good:
                        if (TryGetMentorNode(dbName, topology, clusterTopology, rehab, out var mentorNode) == false)
                            continue;

                        var tryPromote = TryPromote(dbName, topology, current, previous, mentorNode, rehab);
                        if (tryPromote.Promote)
                        {
                            if (_logger.IsOperationsEnabled)
                            {
                                _logger.Operations($"The database {dbName} on {rehab} is reachable and update, so we promote it back to member.");
                            }
                            topology.Members.Add(rehab);
                            topology.Rehabs.Remove(rehab);
                            return $"Node {rehab} was recovered from rehabilitation and promoted back to member";
                        }
                        if (tryPromote.UpdateTopologyReason != null)
                            return tryPromote.UpdateTopologyReason;
                        break;
                }
            }

            RemoveOtherNodesIfNeeded(dbName, topology, ref deletions);
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
                return false;

            topology.Members.Remove(member);
            topology.Rehabs.Add(member);

            string reason;
            if (nodeStats == null)
            {
                reason = "In rehabilitation because it had no status report in the latest cluster stats";
            }
            else if (nodeStats.Status != ClusterNodeStatusReport.ReportStatus.Ok)
            {
                reason = $"In rehabilitation because the last report status was \"{nodeStats.Status}\"";
            }
            else if (nodeStats.Report.TryGetValue(dbName, out var stats) && stats.Status == Faulted)
            {
                reason = "In rehabilitation because the DatabaseStatus for this node is Faulted";
            }
            else
            {
                reason = "In rehabilitation because the node is reachable but had no report about the database";
            }

            if (nodeStats?.Error != null)
            {
                reason += $". {nodeStats.Error}";
            }
            if (dbStats?.Error != null)
            {
                reason += $". {dbStats.Error}";
            }

            topology.DemotionReasons[member] = reason;
            topology.PromotablesStatus[member] = DatabasePromotionStatus.NotResponding;

            if (_logger.IsOperationsEnabled)
            {
                _logger.Operations(reason);
            }

            return true;
        }

        private bool TryGetMentorNode(string dbName, DatabaseTopology topology, ClusterTopology clusterTopology, string promotable, out string mentorNode)
        {
            var url = clusterTopology.GetUrlFromTag(promotable);
            var task = new PromotableTask(promotable, url, dbName);
            mentorNode = topology.WhoseTaskIsIt(task, _server.IsPassive());

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

            if (current.TryGetValue(promotable, out var promotableClusterStats) == false ||
                promotableClusterStats.Report.TryGetValue(dbName, out var promotableDbStats) == false)
                return (false, null);

            var status = ChangeVectorUtils.GetConflictStatus(mentorPrevDbStats.LastChangeVector, promotableDbStats.LastChangeVector);
            if (status == ConflictStatus.AlreadyMerged)
            {
                if (previous.TryGetValue(promotable, out var promotablePrevClusterStats) == false ||
                    promotablePrevClusterStats.Report.TryGetValue(dbName, out var promotablePrevDbStats) == false)
                    return (false, null);

                var indexesCatchedUp = CheckIndexProgress(promotablePrevDbStats.LastEtag, promotablePrevDbStats.LastIndexStats, promotableDbStats.LastIndexStats);
                if (indexesCatchedUp)
                {
                    if (_logger.IsOperationsEnabled)
                    {
                        _logger.Operations($"We promoted the database {dbName} on {promotable} to be a full member");
                    }
                    topology.PromotablesStatus.Remove(promotable);
                    topology.DemotionReasons.Remove(promotable);

                    return (true, $"Node {promotable} is up-to-date so promoting it to be member");
                }
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"The database {dbName} on {promotable} not ready to be promoted, because the indexes are not up-to-date.\n");
                }

                if (topology.PromotablesStatus.TryGetValue(promotable, out var currentStatus) == false
                    || currentStatus != DatabasePromotionStatus.IndexNotUpToDate)
                {
                    topology.PromotablesStatus[promotable] = DatabasePromotionStatus.IndexNotUpToDate;
                    return (false, $"Node {promotable} not ready to be a member, because the indexes are not up-to-date");
                }
            }
            else
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"The database {dbName} on {promotable} not ready to be promoted, because the change vectors are in status: '{status}'.\n" +
                                 $"mentor's change vector : {mentorPrevDbStats.LastChangeVector}, node's change vector : {promotableDbStats.LastChangeVector}");
                }


                if (topology.PromotablesStatus.TryGetValue(promotable, out var currentStatus) == false
                    || currentStatus != DatabasePromotionStatus.ChangeVectorNotMerged)
                {
                    topology.DemotionReasons[promotable] = $"Node is not ready to be promoted, because the change vectors are in status: '{status}'.\n" +
                                                           $"mentor's change vector : {mentorPrevDbStats.LastChangeVector},\n" +
                                                           $"node's change vector : {promotableDbStats.LastChangeVector}";
                    topology.PromotablesStatus[promotable] = DatabasePromotionStatus.ChangeVectorNotMerged;
                    return (false, $"Node {promotable} not ready to be a member, because the change vector is in status: '{status}' (should be AlreadyMerged)");
                }
            }
            return (false, null);
        }

        private void RemoveOtherNodesIfNeeded(string dbName, DatabaseTopology topology, ref List<DeleteDatabaseCommand> deletions)
        {
            if (topology.Members.Count < topology.ReplicationFactor)
                return;

            if (topology.Promotables.Count == 0 &&
                topology.Rehabs.Count == 0)
                return;

            if (_logger.IsOperationsEnabled)
            {
                _logger.Operations("We reached the replication factor, so we remove all other rehab/promotable nodes.");
            }

            var nodesToDelete = topology.Promotables.Concat(topology.Rehabs);
            var deletionCmd = new DeleteDatabaseCommand
            {
                ErrorOnDatabaseDoesNotExists = false,
                DatabaseName = dbName,
                FromNodes = nodesToDelete.ToArray(),
                HardDelete = _hardDeleteOnReplacement,
                UpdateReplicationFactor = false
            };

            if (deletions == null)
                deletions = new List<DeleteDatabaseCommand>();
            deletions.Add(deletionCmd);
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
                    _logger.Operations($"The database {db} on {badNode} has not responded for a long time, but there is no free node to reassign it.");
                }
                return false;
            }
            if (_logger.IsOperationsEnabled)
            {
                _logger.Operations($"The database {db} on {badNode} has not responded for a long time, so we reassign it to {bestNode}.");
            }

            return true;
        }

        private string FindMostUpToDateNode(List<string> nodes, string database, Dictionary<string, ClusterNodeStatusReport> current)
        {
            var updated = nodes[0];
            var highestChangeVectors = current[updated].Report[database].LastChangeVector;
            var maxDocsCount = current[updated].Report[database].NumberOfDocuments;
            for (var index = 1; index < nodes.Count; index++)
            {
                var node = nodes[index];
                var report = current[node].Report[database];
                var cv = report.LastChangeVector;
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
            Dictionary<string, DatabaseStatusReport.ObservedIndexStatus> current)
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


            foreach (var currentIndexStatus in current)
            {
                if (currentIndexStatus.Value.IsStale == false)
                    continue;

                if (previous.TryGetValue(currentIndexStatus.Key, out var _) == false)
                    return false;

                if (lastPrevEtag > currentIndexStatus.Value.LastIndexedEtag)
                    return false;

            }
            return true;
        }

        private Task<(long Etag, object Result)> UpdateTopology(UpdateTopologyCommand cmd)
        {
            if (_engine.LeaderTag != _server.NodeTag)
            {
                throw new NotLeadingException("This node is no longer the leader, so we abort updating the database topology");
            }

            return _engine.PutAsync(cmd);
        }

        private Task<(long Etag, object Result)> Delete(DeleteDatabaseCommand cmd)
        {
            if (_engine.LeaderTag != _server.NodeTag)
            {
                throw new NotLeadingException("This node is no longer the leader, so we abort the delection command");
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

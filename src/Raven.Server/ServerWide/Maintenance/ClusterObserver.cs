using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.Server;
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
            _observe = Run(_cts.Token);
        }

        public async Task Run(CancellationToken token)
        {
            var prevStats = new Dictionary<string, ClusterNodeStatusReport>();
            while (token.IsCancellationRequested == false)
            {
                var delay = TimeoutManager.WaitFor(SupervisorSamplePeriod, token);
                try
                {
                    var newStats = _maintenance.GetStats();
                    await AnalyzeLatestStats(newStats, prevStats);
                    prevStats = newStats;
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
            Dictionary<string, ClusterNodeStatusReport> prevStats)
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var updateCommands = new List<UpdateTopologyCommand>();

                using (context.OpenReadTransaction())
                {
                    var clusterTopology = _server.GetClusterTopology(context);
                    foreach (var database in _engine.StateMachine.GetDatabaseNames(context))
                    {
                        var databaseRecord = _engine.StateMachine.ReadDatabase(context, database, out long etag);
                        var topologyStamp = databaseRecord?.Topology?.Stamp ?? new LeaderStamp
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

                        if (UpdateDatabaseTopology(database, databaseRecord.Topology, clusterTopology, newStats, prevStats))
                        {
                            var cmd = new UpdateTopologyCommand(database)
                            {
                                Topology = databaseRecord.Topology,
                                RaftCommandIndex = etag
                            };

                            updateCommands.Add(cmd);
                        }
                    }
                }

                foreach (var command in updateCommands)
                    await UpdateTopology(command);
            }
        }

        private bool _hasLivingNodesFlag;
        private bool UpdateDatabaseTopology(string dbName, DatabaseTopology topology, ClusterTopology clusterTopology,
            Dictionary<string, ClusterNodeStatusReport> current,
            Dictionary<string, ClusterNodeStatusReport> previous)
        {
            if (topology.Members.Count > 1)
            {
                var hasLivingNode = false;
                foreach (var member in topology.Members)
                {
                    if (current.TryGetValue(member, out var nodeStats) &&
                        nodeStats.LastReportStatus == ClusterNodeStatusReport.ReportStatus.Ok &&
                        nodeStats.LastReport.TryGetValue(dbName, out var dbStats) &&
                        dbStats.Status == Loaded)
                    {
                        hasLivingNode = true;
                        _hasLivingNodesFlag = true;

                        topology.DemotionReasons.Remove(member);
                        topology.PromotablesStatus.Remove(member);
                    }
                }

                if (hasLivingNode == false)
                {
                    var alertMsg = $"It appears that all nodes of the {dbName} database are dead, and we can't demote the last member-node left.";
                    if (_hasLivingNodesFlag)
                    {
                        var alert = AlertRaised.Create(
                            "No living nodes in the database topology",
                            alertMsg,
                            AlertType.ClusterTopologyWarning,
                            NotificationSeverity.Warning
                        );
                        _server.NotificationCenter.Add(alert);
                    }
                    _hasLivingNodesFlag = false;
                    if (_logger.IsOperationsEnabled)
                    {
                        _logger.Operations(alertMsg);
                    }
                    return false;
                }

                foreach (var member in topology.Members)
                {
                    if (IsBroken(topology, member, dbName, current))
                    {
                        InvestigateRehabReason(dbName, topology, current, previous, member);
                        if (TryFindFitNode(member, dbName, current, out var node) == false)
                            continue;

                        topology.Members.Remove(member);
                        topology.Promotables.Add(node);
                        topology.Rehab.Add(member);
                        return true;
                    }
                }
            }
            
            foreach (var promotable in topology.Promotables)
            {
                if (TryGetMontorNode(dbName, topology, clusterTopology, promotable, out var mentorNode) == false)
                    return false;

                if (IsBroken(topology, promotable, dbName, current))
                {
                    if (TryFindFitNode(promotable, dbName, current, out var node) == false)
                        continue;

                    topology.Promotables.Add(node);
                    return true;
                }

                if (TryPromote(dbName, topology, current, previous, mentorNode, promotable))
                {
                    RemoveOtherNodesIfNeeded(topology);
                    return true;
                }
            }

            foreach (var broked in topology.Rehab)
            {
                if (IsBroken(topology, broked, dbName, current) == false)
                {
                    if (TryGetMontorNode(dbName, topology, clusterTopology, broked, out var mentorNode) == false)
                        return false;

                    if(TryPromote(dbName, topology, current, previous, mentorNode, broked))
                    {
                        if (_logger.IsOperationsEnabled)
                        {
                            _logger.Operations($"The database {dbName} on {broked} is reachable and update, so we promote it back to member.");
                        }
                        topology.Members.Add(broked);
                        topology.Rehab.Remove(broked);
                        RemoveOtherNodesIfNeeded(topology);
                        return true;
                    }
                }
            }
            return false;
        }

        private void InvestigateRehabReason(string dbName, DatabaseTopology topology, Dictionary<string, ClusterNodeStatusReport> current, Dictionary<string, ClusterNodeStatusReport> previous, string member)
        {
            DatabaseStatusReport dbStats = null;
            if (current.TryGetValue(member, out var nodeStats) == false ||
                nodeStats.LastReportStatus != ClusterNodeStatusReport.ReportStatus.Ok ||
                nodeStats.LastReport.TryGetValue(dbName, out dbStats) == false ||
                dbStats.Status == Faulted)
            {
                topology.Members.Remove(member);
                topology.Rehab.Add(member);

                if (_logger.IsOperationsEnabled)
                {
                    _logger.Operations($"We rehabilitate the database {dbName} on {member}");
                }

                string reason;
                if (nodeStats == null)
                {
                    reason = "In rehabilitation because it had no status report in the latest cluster stats";
                }
                else if (nodeStats.LastReportStatus != ClusterNodeStatusReport.ReportStatus.Ok)
                {
                    reason = $"In rehabilitation because the last report status was \"{nodeStats.LastReportStatus}\" ";
                }
                else if (nodeStats.LastReport.TryGetValue(dbName, out var stats) && stats.Status == Faulted)
                {
                    reason = "In rehabilitation because the DatabaseStatus for this node is Faulted";
                }
                else
                {
                    reason = "In rehabilitation because it had no DatabaseStatusReport";
                }

                if (nodeStats?.LastError != null)
                {
                    reason += $". {nodeStats.LastError}";
                }

                topology.DemotionReasons[member] = reason;
                topology.PromotablesStatus[member] = nodeStats?.LastReportStatus.ToString();
            }

            if (dbStats != null && dbStats.Status == Loading)
            {
                if (previous.TryGetValue(member, out var prevNodeStats) &&
                    prevNodeStats.LastReport.TryGetValue(dbName, out var prevDbStats) &&
                    prevDbStats.Status == Loading)
                {
                    topology.Members.Remove(member);
                    topology.Promotables.Add(member);

                    if (_logger.IsOperationsEnabled)
                    {
                        _logger.Operations($"We rehabilitate the database {dbName} on {member}, because it is too long in Loading state.");
                    }
                    topology.DemotionReasons[member] = "In rehabilitation because it is too long in Loading state";
                    topology.PromotablesStatus[member] = "Loading the databse";
                }
            }
        }

        private bool TryGetMontorNode(string dbName, DatabaseTopology topology, ClusterTopology clusterTopology, string promotable, out string mentorNode)
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

        private bool TryPromote(string dbName, DatabaseTopology topology, Dictionary<string, ClusterNodeStatusReport> current, Dictionary<string, ClusterNodeStatusReport> previous, string mentorNode, string promotable)
        {
            if (previous.TryGetValue(mentorNode, out var mentorPrevClusterStats) == false ||
                mentorPrevClusterStats.LastReport.TryGetValue(dbName, out var mentorPrevDbStats) == false)
                return false;

            if (current.TryGetValue(promotable, out var promotableClusterStats) == false ||
                promotableClusterStats.LastReport.TryGetValue(dbName, out var promotableDbStats) == false)
                return false;

            var status = ChangeVectorUtils.GetConflictStatus(mentorPrevDbStats.LastChangeVector, promotableDbStats.LastChangeVector);
            if (status == ConflictStatus.AlreadyMerged)
            {
                if (previous.TryGetValue(promotable, out var promotablePrevClusterStats) == false ||
                    promotablePrevClusterStats.LastReport.TryGetValue(dbName, out var promotablePrevDbStats) == false)
                    return false;

                var indexesCatchedUp = CheckIndexProgress(promotablePrevDbStats.LastEtag, promotablePrevDbStats.LastIndexStats, promotableDbStats.LastIndexStats);
                if (indexesCatchedUp)
                {
                    topology.Promotables.Remove(promotable);
                    topology.Members.Add(promotable);
                    if (_logger.IsOperationsEnabled)
                    {
                        _logger.Operations($"We promote the database {dbName} on {promotable} to be a full member");
                    }
                    topology.PromotablesStatus.Remove(promotable);
                    topology.DemotionReasons.Remove(promotable);

                    return true;
                }
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"The database {dbName} on {promotable} not ready to be promoted, because the indexes are not up-to-date.\n");
                }

                topology.PromotablesStatus[promotable] = "node is not ready to be promoted, because the indexes are not up-to-date";
            }
            else
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"The database {dbName} on {promotable} not ready to be promoted, because the change vectors are {status}.\n" +
                                 $"mentor's change vector : {mentorPrevDbStats.LastChangeVector}, node's change vector : {promotableDbStats.LastChangeVector}");
                }
                topology.PromotablesStatus[promotable] = $"node is not ready to be promoted, because the change vectors are {status}.\n" +
                                                         $"mentor's change vector : {mentorPrevDbStats.LastChangeVector}, " +
                                                         $"node's change vector : {promotableDbStats.LastChangeVector}";
            }
            return false;
        }

        private void RemoveOtherNodesIfNeeded(DatabaseTopology topology)
        {
            if (topology.Members.Count == topology.ReplicationFactor)
            {
                if (topology.Promotables.Count + topology.Rehab.Count > 0)
                {
                    if (_logger.IsOperationsEnabled)
                    {
                        _logger.Operations("We reached the replication factor, so we remove all other broken/promotable nodes.");
                    }
                    topology.Promotables.Clear();
                    topology.Rehab.Clear();
                }
            }
        }
        
        private bool IsBroken(
            DatabaseTopology topology, string node, string db,
            Dictionary<string, ClusterNodeStatusReport> current)
        {
            if (topology.DynamicNodesDistribution == false)
                return false;

            var hasCurrent = current.TryGetValue(node, out var currentNodeStats);

            // Wait until we have more info
            if (hasCurrent == false)
                return false;

            // if server is down we should reassign
            if (DateTime.UtcNow - currentNodeStats.LastSuccessfulUpdateDateTime > _breakdownTimeout)
                return true;

            if (currentNodeStats.LastGoodDatabaseStatus.TryGetValue(db, out var lastGoodTime) == false)
            {
                throw new MissingFieldException($"The {db} database last known good time is missing!"); // should never hit
            }

            return DateTime.UtcNow - lastGoodTime > _breakdownTimeout;
        }

        private bool TryFindFitNode(string badNode, string db,
            Dictionary<string, ClusterNodeStatusReport> current, out string bestNode)
        {
            bestNode = null;
            var dbCount = int.MaxValue;
            foreach (var report in current)
            {
                if(report.Key == badNode)
                    continue;
                if(report.Value.LastReport.ContainsKey(db))
                    continue;
                if (dbCount > report.Value.LastReport.Count)
                {
                    dbCount = report.Value.LastReport.Count;
                    bestNode = report.Key;
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

        private bool CheckIndexProgress(
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

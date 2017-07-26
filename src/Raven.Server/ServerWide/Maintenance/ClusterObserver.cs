using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Raven.Client.Http;
using Raven.Client.Server;
using Raven.Client.Util;
using Raven.Server.Documents;
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
                    if (current.TryGetValue(member, out var nodeStats) == false ||
                        nodeStats.LastReportStatus != ClusterNodeStatusReport.ReportStatus.Ok ||
                        nodeStats.LastReport.TryGetValue(dbName, out var dbStats) == false ||
                        dbStats.Status == Faulted)
                    {
                        topology.Members.Remove(member);
                        topology.Promotables.Add(member);

                        if (_logger.IsOperationsEnabled)
                        {
                            _logger.Operations($"We demote the database {dbName} on {member}");
                        }

                        string reason;
                        if (nodeStats == null)
                        {
                            reason = "Demoted because it had no status report in the latest cluster stats";
                        }
                        else if (nodeStats.LastReportStatus != ClusterNodeStatusReport.ReportStatus.Ok)
                        {
                            reason = $"Demoted because the last report status was \"{nodeStats.LastReportStatus}\" ";
                        }
                        else if (nodeStats.LastReport.TryGetValue(dbName, out var stats) && stats.Status == Faulted)
                        {
                            reason = "Demoted because the DatabaseStatus for this node is Faulted";
                        }
                        else
                        {
                            reason = "Demoted because it had no DatabaseStatusReport";
                        }

                        if (nodeStats?.LastError != null)
                        {
                            reason += $". {nodeStats.LastError}";
                        }

                        topology.DemotionReasons[member] = reason;
                        topology.PromotablesStatus[member] = nodeStats?.LastReportStatus.ToString();

                        return true;
                    }

                    if (dbStats.Status == Loading)
                    {
                        if (previous.TryGetValue(member, out var prevNodeStats) &&
                            prevNodeStats.LastReport.TryGetValue(dbName, out var prevDbStats) &&
                            prevDbStats.Status == Loading)
                        {
                            topology.Members.Remove(member);
                            topology.Promotables.Add(member);

                            if (_logger.IsOperationsEnabled)
                            {
                                _logger.Operations($"We demote the database {dbName} on {member}, because it is too long in Loading state.");
                            }
                            topology.DemotionReasons[member] = "Demoted because it is too long in Loading state";
                            topology.PromotablesStatus[member] = "Loading the databse";
                            return true;
                        }
                    }
                }
            }

            if (topology.Promotables.Count == 0)
                return false;

            foreach (var promotable in topology.Promotables)
            {
                var url = clusterTopology.GetUrlFromTag(promotable);
                var task = new PromotableTask(promotable,url, dbName);
                var mentorNode = topology.WhoseTaskIsIt(task,_server.IsPassive());
                if (mentorNode == null)
                {
                    // We are in passive mode and were kicked out of the cluster.
                    return false;
                }

                if (ShouldReassign(topology, promotable, dbName, current, previous))
                {
                    var node = FindLeastDbNode(promotable, dbName, current);
                    if (node == null)
                    {
                        if (_logger.IsOperationsEnabled)
                        {
                            _logger.Operations($"Somthing is wrong with {dbName} on {promotable}, but we were unable to reassign it.");
                        }
                        continue;
                    }
                    if (_logger.IsOperationsEnabled)
                    {
                        _logger.Operations($"Somthing is wrong with {dbName} on {promotable}, so we reassign it to {node}.");
                    }
                    topology.RemoveFromTopology(promotable);
                    topology.Promotables.Add(node);
                    return true;
                }

                if (previous.TryGetValue(mentorNode, out var mentorPrevClusterStats) == false ||
                    mentorPrevClusterStats.LastReport.TryGetValue(dbName, out var mentorPrevDbStats) == false)
                    continue;

                if (current.TryGetValue(promotable, out var promotableClusterStats) == false ||
                   promotableClusterStats.LastReport.TryGetValue(dbName, out var promotableDbStats) == false)
                    continue;

                var status = ChangeVectorUtils.GetConflictStatus(mentorPrevDbStats.LastChangeVector, promotableDbStats.LastChangeVector);
                if (status == ConflictStatus.AlreadyMerged)
                {
                    if (previous.TryGetValue(promotable, out var promotablePrevClusterStats) == false ||
                        promotablePrevClusterStats.LastReport.TryGetValue(dbName, out var promotablePrevDbStats) == false)
                        continue;

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
            }
            return false;
        }

        private const DatabaseStatus BadFlags = Faulted | Shutdown;
        private bool ShouldReassign(
            DatabaseTopology topology, string node, string db,
            Dictionary<string, ClusterNodeStatusReport> current, 
            Dictionary<string, ClusterNodeStatusReport> previous)
        {
            if (topology.DynamicNodesDistribution == false)
                return false;

            DatabaseStatusReport currentDbStats = null;
            DatabaseStatusReport prevDbStats = null;
            var hasCurrent = current.TryGetValue(node, out var currentNodeStats) && currentNodeStats.LastReport.TryGetValue(db, out currentDbStats);
            var hasPrev = previous.TryGetValue(node, out var prevNodeStats) && prevNodeStats.LastReport.TryGetValue(db, out prevDbStats);

            if (hasCurrent == false && hasPrev == false)
                return true;

            // Wait until we have more info
            if (hasCurrent == false || hasPrev == false)
                return false;

            if (currentNodeStats.LastReportStatus != ClusterNodeStatusReport.ReportStatus.Ok &&
                prevNodeStats.LastReportStatus != ClusterNodeStatusReport.ReportStatus.Ok)
                return true;

            if ((currentDbStats.Status & BadFlags) != 0 && (prevDbStats.Status & BadFlags) != 0)
                return true;

            return false;
        }

        private string FindLeastDbNode(string badNode, string db,
            Dictionary<string, ClusterNodeStatusReport> current)
        {
            string bestNode = null;
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
            return bestNode;
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

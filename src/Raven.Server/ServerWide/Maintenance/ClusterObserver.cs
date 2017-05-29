using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Server;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;

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
                        _logger.Info($"An error occured while analyzing maintainance stats on node {_nodeTag}.", e);
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
                var updateCommands = new List<BlittableJsonReaderObject>();

                using (context.OpenReadTransaction())
                {
                    foreach (var database in _engine.StateMachine.GetDatabaseNames(context))
                    {
                        var databaseRecord = _engine.StateMachine.ReadDatabase(context, database, out long etag);

                        if (UpdateDatabaseTopology(database, databaseRecord.Topology, newStats, prevStats))
                        {
                            var cmd = new UpdateTopologyCommand(database)
                            {
                                Topology = databaseRecord.Topology,
                                Etag = etag
                            };

                            updateCommands.Add(context.ReadObject(cmd.ToJson(), "update-topology"));
                        }
                    }
                }

                foreach (var command in updateCommands)
                {
                    await UpdateTopology(command);
                }
            }
        }

        private bool _hasLivingNodesFlag;
        private bool UpdateDatabaseTopology(string dbName, DatabaseTopology topology,
            Dictionary<string, ClusterNodeStatusReport> currentClusterStats,
            Dictionary<string, ClusterNodeStatusReport> previousClusterStats)
        {
            if (topology.Members.Count > 1)
            {
                var hasLivingNode = false;
                foreach (var member in topology.Members)
                {
                    if (currentClusterStats.TryGetValue(member.NodeTag, out var nodeStats) &&
                        nodeStats.LastReportStatus == ClusterNodeStatusReport.ReportStatus.Ok &&
                        nodeStats.LastReport.TryGetValue(dbName, out var dbStats) &&
                        dbStats.Status == DatabaseStatus.Loaded)
                    {
                        hasLivingNode = true;
                        _hasLivingNodesFlag = true;
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
                    if (currentClusterStats.TryGetValue(member.NodeTag, out var nodeStats) == false ||
                        nodeStats.LastReportStatus != ClusterNodeStatusReport.ReportStatus.Ok ||
                        nodeStats.LastReport.TryGetValue(dbName, out var dbStats) == false ||
                        dbStats.Status == DatabaseStatus.Faulted)
                    {
                        topology.Members.Remove(member);
                        topology.Promotables.Add(member);

                        if (_logger.IsOperationsEnabled)
                        {
                            _logger.Operations($"We demote the database {dbName} on {member.NodeTag}");
                        }
                        return true;
                    }

                    if (dbStats.Status == DatabaseStatus.Loading)
                    {
                        if (previousClusterStats.TryGetValue(member.NodeTag, out var prevNodeStats) &&
                            prevNodeStats.LastReport.TryGetValue(dbName, out var prevDbStats) &&
                            prevDbStats.Status == DatabaseStatus.Loading)
                        {
                            topology.Members.Remove(member);
                            topology.Promotables.Add(member);

                            if (_logger.IsOperationsEnabled)
                            {
                                _logger.Operations($"We demote the database {dbName} on {member.NodeTag}, because it is too long in Loading state.");
                            }
                            return true;
                        }
                    }
                }
            }

            if (topology.Promotables.Count == 0)
                return false;

            foreach (var promotable in topology.Promotables)
            {
                var mentorNode = topology.WhoseTaskIsIt(promotable);

                if (previousClusterStats.TryGetValue(mentorNode, out var mentorPrevClusterStats) == false ||
                    mentorPrevClusterStats.LastReport.TryGetValue(dbName, out var mentorPrevDbStats) == false)
                    continue;

                if (currentClusterStats.TryGetValue(promotable.NodeTag, out var promotableClusterStats) == false ||
                   promotableClusterStats.LastReport.TryGetValue(dbName, out var promotableDbStats) == false)
                    continue;

                var status = ConflictsStorage.GetConflictStatus(mentorPrevDbStats.LastDocumentChangeVector, promotableDbStats.LastDocumentChangeVector);
                if (status == ConflictsStorage.ConflictStatus.AlreadyMerged)
                {
                    if (previousClusterStats.TryGetValue(promotable.NodeTag, out var promotablePrevClusterStats) == false ||
                        promotablePrevClusterStats.LastReport.TryGetValue(dbName, out var promotablePrevDbStats) == false)
                        continue;

                    var indexesCatchedUp = CheckIndexProgress(promotablePrevDbStats.LastEtag, promotablePrevDbStats.LastIndexStats, promotableDbStats.LastIndexStats);
                    if (indexesCatchedUp)
                    {
                        topology.Promotables.Remove(promotable);
                        topology.Members.Add(promotable);
                        if (_logger.IsOperationsEnabled)
                        {
                            _logger.Operations($"We promote the database {dbName} on {promotable.NodeTag} to be a full member");
                        }
                        return true;
                    }
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info($"The database {dbName} on {promotable.NodeTag} not ready to be promoted, because the indexes are not up-to-date.\n");
                    }
                }
                else
                {
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info($"The database {dbName} on {promotable.NodeTag} not ready to be promoted, because the change vectors are {status}.\n" +
                                           $"mentor's change vector : {mentorPrevDbStats.LastDocumentChangeVector}, node's change vector : {promotableDbStats.LastDocumentChangeVector}");
                    }
                }
            }
            return false;
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
                if(currentIndexStatus.Value.IsStale == false)
                    continue;

                if (previous.TryGetValue(currentIndexStatus.Key, out var _) == false)
                    return false;

                if (lastPrevEtag > currentIndexStatus.Value.LastIndexedEtag)
                    return false;

            }
            return true;
        }

        private Task<(long, BlittableJsonReaderObject)> UpdateTopology(BlittableJsonReaderObject cmd)
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

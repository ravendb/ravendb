using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server.Documents;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Logging;

namespace Raven.Server.ServerWide.Maintance
{
    class ClusterObserver : IDisposable
    {
        private readonly Task _observe;
        private readonly CancellationTokenSource _cts;
        private readonly ClusterMaintenanceMaster _maintenance;
        private readonly string _nodeTag;
        private readonly RachisConsensus<ClusterStateMachine> _engine;
        private readonly TransactionContextPool _contextPool;
        private readonly Logger _logger;

        public readonly long MaxIndexEtagInterval;
        public readonly long LeaderSamplePeriod;
        
        public ClusterObserver(
            ClusterMaintenanceMaster maintenance,
            ServerStore server,
            RachisConsensus<ClusterStateMachine> engine,
            TransactionContextPool contextPool,
            CancellationToken token)
        {
            _maintenance = maintenance;
            _nodeTag = server.NodeTag;
            _engine = engine;
            _contextPool = contextPool;
            _logger = LoggingSource.Instance.GetLogger<ClusterObserver>(_nodeTag);
            _cts = CancellationTokenSource.CreateLinkedTokenSource(token);

            var config = server.Configuration.ClusterMaintaince;
            LeaderSamplePeriod = (long)config.LeaderSamplePeriod.AsTimeSpan.TotalMilliseconds;
            MaxIndexEtagInterval = config.MaxIndexEtagInterval;

            _observe = Run(_cts.Token);
        }

        public async Task Run(CancellationToken token)
        {
            var prevStats = new Dictionary<string, ClusterNodeStatusReport>();
            while (token.IsCancellationRequested == false)
            {
                try
                {
                    var newStats = _maintenance.GetStats();
                    var delay = Task.Delay(TimeSpan.FromMilliseconds(LeaderSamplePeriod), token);
                    await OnNewStatsArrival(newStats, prevStats);
                    prevStats = newStats;
                    await delay;
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info($"Closing observer on {_nodeTag}, caused by an interrupt.",e);
                    }
                    return;
                }
            }
        }

        public async Task OnNewStatsArrival(
            Dictionary<string, ClusterNodeStatusReport> newStats, 
            Dictionary<string, ClusterNodeStatusReport> prevStats)
        {          
            foreach (var database in GetDatabases())
            {
                var (topology, etag) = GetTopology(database);

                if (UpdateDatabaseTopology(database, topology, newStats, prevStats))
                {
                    await UpdateTopology(database, topology, etag);
                }                   
            }
        }

        private IEnumerable<string> GetDatabases()
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                foreach (var db in _engine.StateMachine.GetDatabaseNames(ctx))
                {
                    yield return db;
                }
            }
        }

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
                    }
                }

                if (hasLivingNode == false)
                {
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info($"It appears that all nodes of the {dbName} database are dead, and we can't demote the last member-node left.");
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
                        if (_logger.IsInfoEnabled)
                        {
                            _logger.Info($"We demote the database {dbName} on {member.NodeTag}");
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
                            if (_logger.IsInfoEnabled)
                            {
                                _logger.Info($"We demote the database {dbName} on {member.NodeTag}, because it is too long in Loading state.");
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
                var index = (int)Hashing.JumpConsistentHash.Calculate(promotable.GetTaskKey(), topology.Members.Count);
                var mentor = topology.Members[index];
                if(previousClusterStats.TryGetValue(mentor.NodeTag, out var mentorPrevClusterStats) == false ||
                    mentorPrevClusterStats.LastReport.TryGetValue(dbName, out var mentorPrevDbStats) == false)
                    continue;

                if(currentClusterStats.TryGetValue(promotable.NodeTag, out var promotableClusterStats) == false ||
                   promotableClusterStats.LastReport.TryGetValue(dbName, out var promotableDbStats) == false)
                    continue;

                var mentorIndexes = mentorPrevDbStats.LastIndexedDocumentEtag.Select(i => i.Key);
                var indexesCatchedUp = CheckIndexProgress(mentorIndexes, promotableDbStats.LastIndexedDocumentEtag);

                var status = ConflictsStorage.GetConflictStatus(mentorPrevDbStats.LastDocumentChangeVector, promotableDbStats.LastDocumentChangeVector);
                if (status == ConflictsStorage.ConflictStatus.AlreadyMerged && indexesCatchedUp)
                {
                    topology.Promotables.Remove(promotable);
                    topology.Members.Add(promotable);
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info($"We promte the database {dbName} on {promotable.NodeTag}");
                    }
                    return true;
                }               
            }
            return false;
        }
        

        private bool CheckIndexProgress(IEnumerable<string> indexNames, Dictionary<string, long> databaseChangeIndexes)
        {
            foreach (var index in indexNames)
            {
                if (databaseChangeIndexes.TryGetValue(index, out long diffEtag) == false)
                {
                    return false;
                }
                if (diffEtag > MaxIndexEtagInterval)
                {
                    return false;
                }
            }
            return true;
        }

        private Task<long> UpdateTopology(string databaseName,DatabaseTopology topology, long etag)
        {
            var cmd = new UpdateTopologyCommand(databaseName)
            {
                Topology = topology,
                Etag = etag
            };
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                return _engine.PutAsync(ctx.ReadObject(cmd.ToJson(), "update-topology"));
            }
        }

        private (DatabaseTopology topology, long etag) GetTopology(string database)
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var rec = _engine.StateMachine.ReadDatabase(ctx, database, out long etag);
                return (rec.Topology, etag);
            }
        }

        public void Dispose()
        {
            _cts.Cancel();
            if (_observe.Wait(TimeSpan.FromSeconds(30)) == false)
            {
                throw new ObjectDisposedException($"Cluster observer on node {_nodeTag} still running and can't be closed");
            }
            _cts.Dispose();
        }
    }
}

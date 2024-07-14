using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Xunit;

namespace Tests.Infrastructure;

public partial class ClusterTestBase
{
    public readonly ShardingClusterTestBase ShardingCluster;

    public class ShardingClusterTestBase
    {
        private readonly ClusterTestBase _parent;

        public ShardingClusterTestBase(ClusterTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }

        public Task<(long Index, List<RavenServer> Servers)> CreateShardedDatabaseInCluster(string databaseName, int replicationFactor, (List<RavenServer> Nodes, RavenServer Leader) tuple, int shards = 3, X509Certificate2 certificate = null)
        {
            var tags = tuple.Nodes.Select(x => x.ServerStore.NodeTag).ToList();

            var record = new DatabaseRecord(databaseName)
            {
                Sharding = new ShardingConfiguration
                {
                    Orchestrator = new OrchestratorConfiguration
                    {
                        Topology = CreateTopology<OrchestratorTopology>(replicationFactor, tags, 0)
                    },
                    Shards = GetDatabaseTopologyForShards(replicationFactor, tags, shards)
                }
            };
            return _parent.CreateDatabaseInCluster(record, replicationFactor, tuple.Leader.WebUrl, certificate);
        }

        private static Dictionary<int, DatabaseTopology> GetDatabaseTopologyForShards(int replicationFactor, List<string> tags, int shards)
        {
            Assert.True(replicationFactor <= tags.Count);
            var topology = new Dictionary<int, DatabaseTopology>(shards);
            for (int i = 0; i < shards; i++)
            {
                topology[i] = CreateTopology<DatabaseTopology>(replicationFactor, tags, i);
            }

            return topology;
        }

        private static TTopology CreateTopology<TTopology>(int replicationFactor, List<string> tags, int shardNumber)
            where TTopology : DatabaseTopology, new()
        {
            var currentTag = tags[shardNumber % tags.Count];
            var otherTags = tags.Where(x => x != currentTag).ToList();
            var members = new List<string> { currentTag };
            var localReplicationFactor = replicationFactor;
            while (--localReplicationFactor != 0 && otherTags.Count > 0)
            {
                int index = new Random().Next(otherTags.Count);
                members.Add(otherTags[index]);
                otherTags.Remove(otherTags[index]);
            }

            return new TTopology { Members = members };
        }

        public async Task<IDictionary<string, List<DocumentDatabase>>> GetShardsDocumentDatabaseInstancesFor(IDocumentStore store, List<RavenServer> nodes, string database = null)
        {
            var dbs = new Dictionary<string, List<DocumentDatabase>>();
            foreach (var server in nodes)
            {
                dbs.Add(server.ServerStore.NodeTag, new List<DocumentDatabase>());
                foreach (var task in server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(database ?? store.Database))
                {
                    var list = dbs[server.ServerStore.NodeTag];
                    list.Add(await task);
                    dbs[server.ServerStore.NodeTag] = list;
                }
            }


            return dbs;
        }

        public bool WaitForShardedChangeVectorInCluster(List<RavenServer> nodes, string database, int replicationFactor, int timeout = 15000)
        {
            return AsyncHelpers.RunSync(() => WaitForShardedChangeVectorInClusterAsync(nodes, database, replicationFactor, timeout));
        }

        public async Task<bool> WaitForShardedChangeVectorInClusterAsync(List<RavenServer> nodes, string database, int replicationFactor, int timeout = 15000)
        {
            return await WaitForValueAsync(async () =>
            {
                var cvs = new Dictionary<string, List<string>>();
                foreach (var server in nodes)
                {
                    foreach (var task in server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(database))
                    {
                        var storage = await task;
                        cvs.TryAdd(storage.Name, new List<string>());
                        using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var list = cvs[storage.Name];
                            list.Add(DocumentsStorage.GetDatabaseChangeVector(context));
                            cvs[storage.Name] = list;
                        }
                    }
                }

                var result = true;
                foreach ((var _, List<string> shardCvs) in cvs)
                {
                    var first = shardCvs.FirstOrDefault();
                    var stringEqual = shardCvs.Any(x => x != first) == false;
                    if (string.IsNullOrEmpty(first))
                    {
                        result = stringEqual;
                    }
                    else
                    {
                        var sizeEqual = shardCvs.Any(x => x.ToChangeVectorList().Count != replicationFactor) == false;
                        result = stringEqual && sizeEqual;
                    }

                    if (result == false)
                        return false;
                }

                return true;
            }, true, timeout: timeout, interval: 333);
        }

        public async Task<Dictionary<int, DatabaseTopology>> GetShards(DocumentStore store)
        {
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            return record.Sharding?.Shards;
        }

        public Task EnsureNoReplicationLoopForSharding(RavenServer server, string database) => _parent.Sharding.EnsureNoReplicationLoopForShardingAsync(server, database);
    }
}

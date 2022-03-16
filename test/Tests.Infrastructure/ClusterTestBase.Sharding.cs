using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
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
            var record = new DatabaseRecord(databaseName)
            {
                Shards = GetDatabaseTopologyForShards(replicationFactor, tuple.Nodes.Select(x => x.ServerStore.NodeTag).ToList(), shards)
            };
            return _parent.CreateDatabaseInCluster(record, replicationFactor, tuple.Leader.WebUrl, certificate);
        }

        public static DatabaseTopology[] GetDatabaseTopologyForShards(int replicationFactor, List<string> tags, int shards)
        {
            Assert.True(replicationFactor <= tags.Count);
            var topology = new DatabaseTopology[shards];
            for (int i = 0; i < shards; i++)
            {
                var currentTag = tags[i % tags.Count];
                var otherTags = tags.Where(x => x != currentTag).ToList();
                var members = new List<string>() { currentTag };
                var localReplicationFactor = replicationFactor;
                while (--localReplicationFactor != 0 && otherTags.Count > 0)
                {
                    int index = new Random().Next(otherTags.Count);
                    members.Add(otherTags[index]);
                    otherTags.Remove(otherTags[index]);
                }

                topology[i] = new DatabaseTopology { Members = members };
            }

            return topology;
        }

        public async Task<DatabaseTopology[]> GetShards(DocumentStore store)
        {
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            return record.Shards;
        }
    }
}

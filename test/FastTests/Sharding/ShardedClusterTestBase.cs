using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding
{
    [Trait("Category", "Sharding")]
    public abstract class ShardedClusterTestBase : ClusterTestBase
    {
        protected ShardedClusterTestBase(ITestOutputHelper output) : base(output)
        {
        }

        public Task<(long Index, List<RavenServer> Servers)> CreateShardedDatabaseInCluster(string databaseName, int replicationFactor, (List<RavenServer> Nodes, RavenServer Leader) tuple, int shards = 3, X509Certificate2 certificate = null)
        {
            var record = new DatabaseRecord(databaseName)
            {
                Shards = GetDatabaseTopologyForShards(replicationFactor, tuple.Nodes.Select(x => x.ServerStore.NodeTag).ToList(), shards)
            };
            return CreateDatabaseInCluster(record, replicationFactor, tuple.Leader.WebUrl, certificate);
        }

        internal static DatabaseTopology[] GetDatabaseTopologyForShards(int replicationFactor, List<string> tags, int shards)
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
    }
}

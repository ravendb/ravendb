using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Sharding;
using Raven.Server;
using Raven.Server.Utils;
using Sparrow.Server;
using Sparrow.Threading;
using Xunit;
using static Tests.Infrastructure.ReplicationTestBase;

namespace FastTests;

public partial class RavenTestBase
{
    public class ShardedReplicationTestBase
    {
        internal readonly RavenTestBase _parent;

        public ShardedReplicationTestBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }

        public class ShardedReplicationManager : ReplicationManager //TODO stav: change to interface?
        {
            public readonly Dictionary<int, ReplicationManager> ShardReplications;
            private readonly ShardingConfiguration _config;

            protected ShardedReplicationManager(Dictionary<int, ReplicationManager> shardReplications, string databaseName, ShardingConfiguration config) : base(databaseName)
            {
                ShardReplications = shardReplications;
                _config = config;
            }

            public override void Mend()
            {
                foreach (var (shardNumber, brokenReplication) in ShardReplications)
                {
                    brokenReplication.Mend();
                }
            }

            public override void Break()
            {
                foreach (var (shardNumber, shardReplication) in ShardReplications)
                {
                    shardReplication.Break();
                }
            }

            public override void ReplicateOnce(string docId)
            {
                int shardNumber;
                using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
                    shardNumber = ShardHelper.GetShardNumberFor(_config, allocator, docId);

                ShardReplications[shardNumber].ReplicateOnce(docId);
            }

            public override async Task EnsureNoReplicationLoopAsync()
            {
                foreach (var (node, replicationInstance) in ShardReplications)
                {
                    await replicationInstance.EnsureNoReplicationLoopAsync();
                }
            }

            public override void Dispose()
            {
                foreach (var manager in ShardReplications.Values)
                {
                    manager.Dispose();
                }
            }

            internal static async ValueTask<ShardedReplicationManager> GetShardedReplicationManager(ShardingConfiguration configuration, List<RavenServer> servers,
                string databaseName)
            {
                Dictionary<int, ReplicationManager> shardReplications = new();
                foreach (var shardNumber in configuration.Shards.Keys)
                {
                    shardReplications.Add(shardNumber, await ReplicationManager.GetReplicationManagerAsync(servers, ShardHelper.ToShardName(databaseName, shardNumber)));

                    Assert.True(shardReplications.ContainsKey(shardNumber), $"Couldn't find document database of shard {shardNumber} in any of the servers.");
                }

                return new ShardedReplicationManager(shardReplications, databaseName, configuration);
            }
        }
    }
}

using System.Linq;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues
{
    public class RavenDB_21104 : ClusterTestBase
    {
        public RavenDB_21104(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Replication | RavenTestCategory.Cluster)]
        public async Task ContinueReshardingAfterTopologyChanged()
        {
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Replication.MaxItemsCount)] = "1";

            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 3, orchestratorReplicationFactor: 3);

            using (var store = GetDocumentStore(options))
            {
                var databaseName = store.Database;
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                var shardNumber = await Sharding.GetShardNumberForAsync(store, "users/1-A");
                var nextShardNumber = (shardNumber + 1) % 2;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1-A");
                    await session.SaveChangesAsync();
                }

                for (int i = 0; i < 100; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User(), $"initial-{i}$users/1-A");
                        await session.SaveChangesAsync();
                    }
                }

                var writes = Task.Run(() =>
                {
                    for (int i = 0; i < 100; i++)
                    {
                        using (var session = store.OpenSession())
                        {
                            session.Store(new User(), $"num-{i}$users/1-A");
                            session.SaveChanges();
                        }
                    }
                });

                await writes;

                var resharding = Sharding.Resharding.MoveShardForId(store, "users/1-A");

                var databaseTopology = record.Sharding.Shards[nextShardNumber];
                var name = ShardHelper.ToShardName(databaseName, nextShardNumber);

                var node = databaseTopology.Members.First();
                databaseTopology.Members.Remove(node);
                databaseTopology.Members.Add(node);

                await store.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(name, databaseTopology.Members));

                await resharding;

                for (int i = 0; i < 100; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var u = await session.LoadAsync<User>($"initial-{i}$users/1-A");
                        Assert.NotNull(u);
                    }
                }

                for (int i = 0; i < 100; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var u = await session.LoadAsync<User>($"num-{i}$users/1-A");
                        Assert.NotNull(u);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Replication | RavenTestCategory.Cluster)]
        public async Task ContinueReshardingAfterTopologyChanged2()
        {
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Replication.MaxItemsCount)] = "1";

            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 3, orchestratorReplicationFactor: 3);

            using (var store = GetDocumentStore(options))
            {
                var databaseName = store.Database;
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                var shardNumber = await Sharding.GetShardNumberForAsync(store, "users/1-A");
                var nextShardNumber = (shardNumber + 1) % 2;

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "users/1-A");
                    await session.SaveChangesAsync();
                }

                for (int i = 0; i < 100; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User(), $"initial-{i}$users/1-A");
                        await session.SaveChangesAsync();
                    }
                }

                var writes = Task.Run(() =>
                {
                    for (int i = 0; i < 100; i++)
                    {
                        using (var session = store.OpenSession())
                        {
                            session.Store(new User(), $"num-{i}$users/1-A");
                            session.SaveChanges();
                        }
                    }
                });

                var resharding = Sharding.Resharding.MoveShardForId(store, "users/1-A");

                var databaseTopology = record.Sharding.Shards[nextShardNumber];
                var name = ShardHelper.ToShardName(databaseName, nextShardNumber);

                var node = databaseTopology.Members.First();
                databaseTopology.Members.Remove(node);
                databaseTopology.Members.Add(node);

                await store.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(name, databaseTopology.Members));

                await writes;
                await resharding;

                for (int i = 0; i < 100; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var u = await session.LoadAsync<User>($"initial-{i}$users/1-A");
                        Assert.NotNull(u);
                    }
                }

                for (int i = 0; i < 100; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var u = await session.LoadAsync<User>($"num-{i}$users/1-A");
                        Assert.NotNull(u);
                    }
                }
            }
        }
    }
}

using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Sharding;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Entities;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Cluster
{
    public class ReshardingTests : ClusterTestBase
    {
        public ReshardingTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanMoveOneBucketManually()
        {
            // TODO: once wired, disable the observer
            using (var store = Sharding.GetDocumentStore())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

                var id = "foo/bar";
                using (var session = store.OpenAsyncSession())
                {
                    var user = new User
                    {
                        Name = "Original shard"
                    };
                    await session.StoreAsync(user, id);
                    await session.SaveChangesAsync();
                }

                var bucket = ShardHelper.GetBucket(id);
                var location = ShardHelper.GetShardNumber(record.Sharding.BucketRanges, bucket);
                var newLocation = (location + 1) % record.Sharding.Shards.Length;
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, location)))
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.NotNull(user);
                }

                var result = await Server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, location, newLocation);

                var exists = WaitForDocument<User>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, newLocation));
                Assert.True(exists);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var user = await session.LoadAsync<User>(id);
                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    await Server.ServerStore.Sharding.SourceMigrationCompleted(store.Database, bucket, result.Index, changeVector);
                }

                result = await Server.ServerStore.Sharding.DestinationMigrationConfirm(store.Database, bucket, result.Index);
                await Server.ServerStore.Cluster.WaitForIndexNotification(result.Index);

                // the document will be written to the new location
                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    user.Name = "New shard";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, location)))
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.Equal("Original shard", user.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanMoveOneBucketOfSampleData()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation(DatabaseItemType.Documents /*| DatabaseItemType.Attachments | DatabaseItemType.CounterGroups | DatabaseItemType.RevisionDocuments*/));

                var id = "orders/830-A";
                var oldLocation = await Sharding.GetShardNumber(store, id);
               
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, oldLocation)))
                {
                    var order = await session.LoadAsync<Order>(id);
                    Assert.NotNull(order);
                }

                await Sharding.Resharding.MoveShardForId(store, id);

                // the document will be written to the new location
                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>(id);
                    order.Employee = "New shard";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, oldLocation)))
                {
                    var order = await session.LoadAsync<Order>(id);
                    Assert.Null(order);
                }

                var newLocation = await Sharding.GetShardNumber(store, id);
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var order = await session.LoadAsync<Order>(id);
                    Assert.NotNull(order);
                }

                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "assert for everything");
            }
        }

        [Fact(Skip = "Waiting for RavenDB-17760")]
        public async Task CanMoveOneBucket()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

                var id = "foo/bar";
                var bucket = ShardHelper.GetBucket(id);
                var location = ShardHelper.GetShardNumber(record.Sharding.BucketRanges, bucket);
                var newLocation = (location + 1) % record.Sharding.Shards.Length;
                using (var session = store.OpenAsyncSession())
                {
                    var user = new User
                    {
                        Name = "Original shard"
                    };
                    await session.StoreAsync(user, id);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, location)))
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.NotNull(user);
                }

                var exists = WaitForDocument<User>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, newLocation));
                Assert.True(exists);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var user = await session.LoadAsync<User>(id);
                }

                // the document will be written to the new location
                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    user.Name = "New shard";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, location)))
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.Equal("Original shard", user.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanMoveBucketWhileWriting()
        {
            using var store = Sharding.GetDocumentStore();

            var writes = Task.Run(() =>
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                    }, "users/1-A");
                    session.SaveChanges();
                }

                for (int i = 0; i < 100; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                        }, $"num-{i}$users/1-A");
                        session.SaveChanges();
                    }
                }
            });
            
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");

            await writes;

            await AssertWaitForValueAsync(async () =>
            {
                using (var session = store.OpenAsyncSession())
                {
                    var q = await session.Query<User>().ToListAsync();
                    return q.Count;
                }
            }, 101);

            var expectedShard = await Sharding.GetShardNumber(store, "users/1-A");
            for (int shard = 0; shard < 3; shard++)
            {
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, shard)))
                {
                    var q = await session.Query<User>().ToListAsync();
                    Assert.Equal(expectedShard == shard ? 101 : 0, q.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanMoveBucketWhileWriting2()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using var store = Sharding.GetDocumentStore(new Options
            {
                Server = cluster.Leader
            });

            var writes = Task.Run(() =>
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Count = 10
                    }, "users/1-A");
                    session.SaveChanges();
                }

                for (int i = 0; i < 100; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                            Count = 666
                        },"users/");
                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                            Count = 10
                        }, $"num-{i}$users/1-A");
                        session.SaveChanges();
                    }
                }
            });
            
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");

            await writes;

            await AssertWaitForValueAsync(async () =>
            {
                using (var session = store.OpenAsyncSession())
                {
                    var q = await session.Query<User>().Where(u => u.Count == 10, exact: true).ToListAsync();
                    return q.Count;
                }
            }, 101);

            var expectedShard = await Sharding.GetShardNumber(store, "users/1-A");
            for (int shard = 0; shard < 3; shard++)
            {
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, shard)))
                {
                    var q = await session.Query<User>().Where(u => u.Count == 10, exact: true).ToListAsync();
                    Assert.Equal(expectedShard == shard ? 101 : 0, q.Count);
                }
            }
        }
    }
}

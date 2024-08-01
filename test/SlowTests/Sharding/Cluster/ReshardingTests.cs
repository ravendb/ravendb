using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Raven.Client.Documents;
using System.Threading.Tasks;
using Orders;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Client.Util;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
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
            DoNotReuseServer();
            using (var store = Sharding.GetDocumentStore())
            {
                Server.ServerStore.Sharding.ManualMigration = true;

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

                var bucket = Sharding.GetBucket(record.Sharding, id);
                var shardNumber = ShardHelper.GetShardNumberFor(record.Sharding, bucket);
                var toShard = ShardingTestBase.GetNextSortedShardNumber(record.Sharding.Shards, shardNumber);
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, shardNumber)))
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.NotNull(user);
                }

                var result = await Server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, toShard, RaftIdGenerator.NewId());

                var exists = WaitForDocument<User>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, toShard));
                Assert.True(exists);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, toShard)))
                {
                    var user = await session.LoadAsync<User>(id);
                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    await Server.ServerStore.Sharding.SourceMigrationCompleted(store.Database, bucket, result.Index, changeVector, RaftIdGenerator.NewId());
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

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, shardNumber)))
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
                await store.Maintenance.SendAsync(new CreateSampleDataOperation( /*| DatabaseItemType.Attachments | DatabaseItemType.CounterGroups | DatabaseItemType.RevisionDocuments*/));

                var id = "orders/830-A";
                var oldLocation = await Sharding.GetShardNumberForAsync(store, id);
               
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

                var newLocation = await Sharding.GetShardNumberForAsync(store, id);
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var order = await session.LoadAsync<Order>(id);
                    Assert.NotNull(order);
                }

                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "assert for everything");
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanMoveOneBucket()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

                var id = "foo/bar";
                var bucket = Sharding.GetBucket(record.Sharding, id);
                var location = ShardHelper.GetShardNumberFor(record.Sharding, bucket);
                var newLocation = ShardingTestBase.GetNextSortedShardNumber(record.Sharding.Shards, location);
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

                await Sharding.Resharding.MoveShardForId(store, id);

                var exists = WaitForDocument<User>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, newLocation));
                Assert.True(exists);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.NotNull(user);
                }

                var config = await Sharding.GetShardingConfigurationAsync(store);

                // the document will be written to the new location
                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.True(user != null, $"{id} is null (bucket: {bucket}, old:{location}, new:{newLocation}){Environment.NewLine}. {session.Advanced.JsonConverter.ToBlittable(config, documentInfo: null)}");
                    user.Name = "New shard";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, location)))
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.Null(user);
                }
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.Equal("New shard", user.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanMoveOneBucketToNewShard()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(leader, shards: 2, shardReplicationFactor: 2, orchestratorReplicationFactor: 2);

            using (var store = GetDocumentStore(options))
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var shardTopology = record.Sharding.Shards[0];
                Assert.Equal(2, shardTopology.Members.Count);
                Assert.Equal(0, shardTopology.Promotables.Count);
                Assert.Equal(2, shardTopology.ReplicationFactor);

                //create new shard
                var res = store.Maintenance.Server.Send(new AddDatabaseShardOperation(store.Database));
                var newShardNumber = res.ShardNumber;
                Assert.Equal(2, newShardNumber);
                Assert.Equal(2, res.ShardTopology.ReplicationFactor);
                Assert.Equal(2, res.ShardTopology.AllNodes.Count());
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(res.RaftCommandIndex);

                await AssertWaitForValueAsync(async () =>
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return record.Sharding.Shards.Count;
                }, 3);

                await AssertWaitForValueAsync(async () =>
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    record.Sharding.Shards.TryGetValue(newShardNumber, out shardTopology);
                    return shardTopology?.Members?.Count;
                }, 2);

                var nodesContainingNewShard = shardTopology.Members;

                foreach (var node in nodesContainingNewShard)
                {
                    var serverWithNewShard = Servers.Single(x => x.ServerStore.NodeTag == node);
                    Assert.True(serverWithNewShard.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(ShardHelper.ToShardName(store.Database, newShardNumber), out _));
                }
                
                //migrate doc to new shard
                var id = "foo/bar";
                var originalDocShard = await Sharding.GetShardNumberForAsync(store, id);
                var toShard = newShardNumber;

                Assert.NotEqual(toShard, originalDocShard);

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 1);

                    var user = new User
                    {
                        Name = "Original shard"
                    };
                    await session.StoreAsync(user, id);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, originalDocShard)))
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.NotNull(user);
                }

                await Sharding.Resharding.MoveShardForId(store, id, toShard);

                var exists = WaitForDocument<User>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, toShard));
                Assert.True(exists);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, toShard)))
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.NotNull(user);
                }

                // the document will be written to the new location
                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    user.Name = "New shard";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, originalDocShard)))
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.Null(user);
                }
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, toShard)))
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.Equal("New shard", user.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanMoveBucketWhileWriting()
        {
            using var store = Sharding.GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new User(), "users/1-A");
                session.SaveChanges();
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

            var expectedShard = await Sharding.GetShardNumberForAsync(store, "users/1-A");
            var sharding = await Sharding.GetShardingConfigurationAsync(store);
            foreach (var shard in sharding.Shards.Keys)
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
            
            using (var session = store.OpenSession())
            {
                session.Store(new User
                {
                    Count = 10
                }, "users/1-A");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                session.Store(new User
                {
                    Count = 10
                }, "users/1-A");
                session.SaveChanges();
            }

            var writes = Task.Run(() =>
            {
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

            var expectedShard = await Sharding.GetShardNumberForAsync(store, "users/1-A");
            var sharding = await Sharding.GetShardingConfigurationAsync(store);
            foreach (var shard in sharding.Shards.Keys)
            {
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, shard)))
                {
                    var q = await session.Query<User>().Where(u => u.Count == 10, exact: true).ToListAsync();
                    Assert.Equal(expectedShard == shard ? 101 : 0, q.Count);
                }
            }

            using (var session = store.OpenAsyncSession())
            {
                var q = await session.Query<User>().Where(u => u.Count == 10, exact: true).ToListAsync();
                Assert.Equal(101, q.Count);
            }
        }
        
        [RavenFact(RavenTestCategory.Sharding)]
        public async Task BucketDeletionShouldCreateArtificialTombstones()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                const string id = "orders/1";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order(), id);
                    await session.SaveChangesAsync();
                }

                var oldLocation = await Sharding.GetShardNumberForAsync(store, id);

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

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, oldLocation));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = db.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Assert.Equal(1, tombs.Count);

                    var tomb = (DocumentReplicationItem)tombs[0];
                
                    Assert.Equal(id.ToLower(), tomb.Id.ToString(CultureInfo.InvariantCulture));
                    Assert.Equal(ReplicationBatchItem.ReplicationItemType.DocumentTombstone, tomb.Type);
                    Assert.True(tomb.Flags.Contain(DocumentFlags.Artificial));
                    Assert.True(tomb.Flags.Contain(DocumentFlags.FromResharding));
                }

                var newLocation = await Sharding.GetShardNumberForAsync(store, id);
                Assert.NotEqual(oldLocation, newLocation);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var order = await session.LoadAsync<Order>(id);
                    Assert.Equal("New shard", order.Employee);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task BucketDeletionShouldCreateArtificialTombstones2()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                await InsertData(store);

                var suffix = "usa";
                var id1 = $"users/1${suffix}";
                var id2 = $"users/2${suffix}";
                var id3 = $"users/3${suffix}";
                var id4 = $"users/4${suffix}";

                var oldLocation = await Sharding.GetShardNumberForAsync(store, id1);

                await Sharding.Resharding.MoveShardForId(store, id1);

                // the document will be written to the new location
                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id1);
                    user.AddressId = "New shard";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, oldLocation)))
                {
                    var user = await session.LoadAsync<User>(id1);
                    Assert.Null(user);

                    user = await session.LoadAsync<User>(id2);
                    Assert.Null(user);

                    user = await session.LoadAsync<User>(id3);
                    Assert.Null(user);

                    user = await session.LoadAsync<User>(id4);
                    Assert.Null(user);
                }

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, oldLocation));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = db.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Assert.Equal(22, tombs.Count);

                    foreach (var item in tombs)
                    {
                        DocumentFlags flags = item switch
                        {
                            DocumentReplicationItem documentReplicationItem => documentReplicationItem.Flags,
                            AttachmentTombstoneReplicationItem attachmentTombstone => attachmentTombstone.Flags,
                            RevisionTombstoneReplicationItem revisionTombstone => revisionTombstone.Flags,
                            _ => DocumentFlags.None
                        };

                        Assert.True(flags.Contain(DocumentFlags.Artificial));
                        Assert.True(flags.Contain(DocumentFlags.FromResharding));
                    }
                }

                var newLocation = await Sharding.GetShardNumberForAsync(store, id1);
                Assert.NotEqual(oldLocation, newLocation);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var user = await session.LoadAsync<User>(id1);
                    Assert.NotNull(user);

                    user = await session.LoadAsync<User>(id2);
                    Assert.NotNull(user);

                    user = await session.LoadAsync<User>(id3);
                    Assert.NotNull(user);

                    user = await session.LoadAsync<User>(id4);
                    Assert.NotNull(user);
                }

                await CheckData(store, database: ShardHelper.ToShardName(store.Database, newLocation));
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task BucketDeletionShouldMarkExistingTombstonesAsArtificial()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                const string suffix = "eu";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), $"users/1${suffix}");
                    await session.StoreAsync(new User(), $"users/2${suffix}");
                    await session.StoreAsync(new User(), $"users/3${suffix}");
                    await session.StoreAsync(new User(), $"users/4${suffix}");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete($"users/2${suffix}");
                    session.Delete($"users/3${suffix}");
                    session.Delete($"users/4${suffix}");

                    await session.SaveChangesAsync();
                }

                var id = $"users/1${suffix}";
                var oldLocation = await Sharding.GetShardNumberForAsync(store, id);
                var lastProcessedEtag = 0L;

                var oldLocationShard = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, oldLocation));
                using (oldLocationShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = oldLocationShard.DocumentsStorage.GetTombstonesFrom(context, lastProcessedEtag).ToList();
                    Assert.Equal(3, tombs.Count);

                    foreach (var tomb in tombs)
                    {
                        var replicationItem = tomb as DocumentReplicationItem;

                        Assert.NotNull(replicationItem);
                        Assert.False(replicationItem.Flags.Contain(DocumentFlags.Artificial));
                        Assert.False(replicationItem.Flags.Contain(DocumentFlags.FromResharding));

                        lastProcessedEtag = replicationItem.Etag;
                    }
                }

                await Sharding.Resharding.MoveShardForId(store, id);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, oldLocation)))
                {
                    var user = await session.LoadAsync<User>($"users/1${suffix}");
                    Assert.Null(user);
                }

                using (oldLocationShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = oldLocationShard.DocumentsStorage.GetTombstonesFrom(context, lastProcessedEtag + 1).ToList();
                    Assert.Equal(1, tombs.Count);

                    var replicationItem = tombs[0] as DocumentReplicationItem;

                    Assert.NotNull(replicationItem);
                    Assert.True(replicationItem.Flags.Contain(DocumentFlags.Artificial));
                    Assert.True(replicationItem.Flags.Contain(DocumentFlags.FromResharding));
                }

                var newLocation = await Sharding.GetShardNumberForAsync(store, id);
                Assert.NotEqual(oldLocation, newLocation);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var user = await session.LoadAsync<User>($"users/1${suffix}");
                    Assert.NotNull(user);
                }

                var newLocationShard = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, newLocation));
                using (newLocationShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = newLocationShard.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Assert.Equal(3, tombs.Count);

                    foreach (var tomb in tombs)
                    {
                        var replicationItem = tomb as DocumentReplicationItem;

                        Assert.NotNull(replicationItem);
                        Assert.False(replicationItem.Flags.Contain(DocumentFlags.Artificial));
                        Assert.True(replicationItem.Flags.Contain(DocumentFlags.FromResharding));
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task BucketDeletionShouldMarkExistingTombstonesAsArtificial2()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                const string suffix = "eu";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), $"users/1${suffix}");
                    await session.StoreAsync(new User(), $"users/2${suffix}");
                    await session.StoreAsync(new User(), $"users/3${suffix}");
                    await session.StoreAsync(new User(), $"users/4${suffix}");

                    await session.SaveChangesAsync();
                }
                
                var id = $"users/1${suffix}";

                await Sharding.Resharding.MoveShardForId(store, id);

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete($"users/2${suffix}");
                    session.Delete($"users/3${suffix}");
                    session.Delete($"users/4${suffix}");

                    await session.SaveChangesAsync();
                }

                var oldLocation = await Sharding.GetShardNumberForAsync(store, id);
                var lastProcessedEtag = 0L;

                var oldLocationShard = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, oldLocation));
                using (oldLocationShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = oldLocationShard.DocumentsStorage.GetTombstonesFrom(context, lastProcessedEtag).ToList();
                    Assert.Equal(3, tombs.Count);

                    foreach (var tomb in tombs)
                    {
                        var replicationItem = tomb as DocumentReplicationItem;

                        Assert.NotNull(replicationItem);
                        Assert.False(replicationItem.Flags.Contain(DocumentFlags.Artificial));
                        Assert.False(replicationItem.Flags.Contain(DocumentFlags.FromResharding));

                        lastProcessedEtag = replicationItem.Etag;
                    }
                }

                await Sharding.Resharding.MoveShardForId(store, id);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, oldLocation)))
                {
                    var user = await session.LoadAsync<User>($"users/1${suffix}");
                    Assert.Null(user);
                }

                using (oldLocationShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = oldLocationShard.DocumentsStorage.GetTombstonesFrom(context, lastProcessedEtag + 1).ToList();
                    Assert.Equal(1, tombs.Count);

                    var replicationItem = tombs[0] as DocumentReplicationItem;

                    Assert.NotNull(replicationItem);
                    Assert.True(replicationItem.Flags.Contain(DocumentFlags.Artificial));
                    Assert.True(replicationItem.Flags.Contain(DocumentFlags.FromResharding));
                }

                var newLocation = await Sharding.GetShardNumberForAsync(store, id);
                Assert.NotEqual(oldLocation, newLocation);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var user = await session.LoadAsync<User>($"users/1${suffix}");
                    Assert.NotNull(user);
                }

                var newLocationShard = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, newLocation));
                using (newLocationShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = newLocationShard.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Assert.Equal(3, tombs.Count);

                    foreach (var tomb in tombs)
                    {
                        var replicationItem = tomb as DocumentReplicationItem;

                        Assert.NotNull(replicationItem);
                        Assert.False(replicationItem.Flags.Contain(DocumentFlags.Artificial));
                        Assert.True(replicationItem.Flags.Contain(DocumentFlags.FromResharding));
                    }
                }

                await Sharding.EnsureNoDatabaseChangeVectorLeakAsync(store.Database);
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanMoveBucketWhenLastItemIsNotDocument()
        {
            using var store = Sharding.GetDocumentStore();

            const string id = "users/1-A";
            using (var session = store.OpenSession())
            {
                session.Store(new User(), id);
                session.CountersFor(id).Increment("Likes", 100);
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                session.CountersFor(id).Increment("Likes", 100);
                session.SaveChanges();
            }

            var oldLocation = await Sharding.GetShardNumberForAsync(store, id);

            await Sharding.Resharding.MoveShardForId(store, id);

            using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, oldLocation)))
            {
                var user = await session.LoadAsync<User>(id);
                Assert.Null(user);
            }

            var newLocation = await Sharding.GetShardNumberForAsync(store, id);
            Assert.NotEqual(oldLocation, newLocation);

            using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
            {
                var user = await session.LoadAsync<User>(id);
                Assert.NotNull(user);

                var counter = await session.CountersFor(user).GetAsync("Likes");
                Assert.Equal(200, counter);
            }
        }

        [RavenFact(RavenTestCategory.Replication |RavenTestCategory.Sharding)]
        public async Task ShouldNotReplicateTombstonesCreatedByBucketDeletion()
        {
            using (var store = Sharding.GetDocumentStore())
            using (var replica = GetDocumentStore())
            {
                await InsertData(store);

                var suffix = "$usa";
                var id = $"users/1{suffix}";

                await SetupReplication(store, replica);

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    user.AddressId = "New";
                    await session.SaveChangesAsync();
                }

                Assert.True(WaitForDocument<User>(replica, id, u => u.AddressId == "New"));

                await CheckData(replica);

                var oldLocation = await Sharding.GetShardNumberForAsync(store, id);

                await Sharding.Resharding.MoveShardForId(store, id);

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, oldLocation));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = db.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Assert.Equal(24, tombs.Count);
                }

                var newLocation = await Sharding.GetShardNumberForAsync(store, id);

                await CheckData(store, ShardHelper.ToShardName(store.Database, newLocation));

                await CheckData(replica);
            }
        }

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Sharding)]
        public async Task IndexesShouldTakeIntoAccountArtificialTombstones()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                const string suffix = "usa";

                await new UsersByName().ExecuteAsync(store);

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(10));

                    for (int i = 1; i <= 10; i++)
                    {
                        var docId = $"users/{i}${suffix}";
                        await session.StoreAsync(new User
                        {
                            Name = $"Name-{i}",
                        }, docId);
                    }

                    await session.SaveChangesAsync();
                }

                var id = $"users/1${suffix}";
                var oldLocation = await Sharding.GetShardNumberForAsync(store, id);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, oldLocation)))
                {
                    var q = await session.Query<User, UsersByName>().ToListAsync();
                    Assert.Equal(10, q.Count);
                }

                await Sharding.Resharding.MoveShardForId(store, id);

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, oldLocation));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = db.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Assert.Equal(10, tombs.Count);
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, oldLocation)))
                {
                    var q = await session.Query<User, UsersByName>().ToListAsync();
                    Assert.Equal(0, q.Count);
                }

                var newLocation = await Sharding.GetShardNumberForAsync(store, id);
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var q = await session.Query<User, UsersByName>().ToListAsync();
                    Assert.Equal(10, q.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding, Skip = "RavenDB-19696")]
        public async Task EtlShouldNotSendTombstonesCreatedByBucketDeletion()
        {
            using (var store = Sharding.GetDocumentStore())
            using (var replica = GetDocumentStore())
            {
                await InsertData(store);

                AddEtl(store, replica);

                var suffix = "usa";
                var id = $"users/1${suffix}";

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    user.AddressId = "New";
                    await session.SaveChangesAsync();
                }

                Assert.True(WaitForDocument<User>(replica, id, u => u.AddressId == "New"));

                await CheckData(replica, expectedRevisionsCount: 0);

                var oldLocation = await Sharding.GetShardNumberForAsync(store, id);

                await Sharding.Resharding.MoveShardForId(store, id);

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, oldLocation));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = db.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Assert.Equal(22, tombs.Count);
                }

                var newLocation = await Sharding.GetShardNumberForAsync(store, id);

                await CheckData(store, ShardHelper.ToShardName(store.Database, newLocation));

                await CheckData(replica, expectedRevisionsCount: 0);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task RestoreShardedDatabaseFromIncrementalBackupAfterBucketMigration()
        {
            const string suffix = "eu";
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var cluster = await CreateRaftCluster(3, watcherCluster: true);

            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);
            using (var store = Sharding.GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 10; i++)
                    {
                        session.Store(new User(), $"users/{i}${suffix}");
                    }

                    session.SaveChanges();
                }

                var waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(cluster.Nodes, store, config, isFullBackup: false);

                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                // migrate bucket
                const string id = $"users/1${suffix}";
                var oldLocation = await Sharding.GetShardNumberForAsync(store, id);
                await Sharding.Resharding.MoveShardForId(store, id);

                // add more data
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), $"users/11${suffix}");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession(ShardHelper.ToShardName(store.Database, oldLocation)))
                {
                    for (int i = 1; i <= 11; i++)
                    {
                        var doc = session.Load<User>($"users/{i}${suffix}");
                        Assert.Null(doc);
                    }
                }

                var newLocation = await Sharding.GetShardNumberForAsync(store, id);
                using (var session = store.OpenSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    for (int i = 1; i <= 11; i++)
                    {
                        var doc = session.Load<User>($"users/{i}${suffix}");
                        Assert.NotNull(doc);
                    }
                }

                // run backup again
                waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);

                await Sharding.Backup.RunBackupAsync(store.Database, backupTaskId, isFullBackup: false, cluster.Nodes);
                Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                var dirs = Directory.GetDirectories(backupPath);
                Assert.Equal(cluster.Nodes.Count, dirs.Length);

                foreach (var dir in dirs)
                {
                    var files = Directory.GetFiles(dir);
                    Assert.Equal(2, files.Length);
                }

                var sharding = await Sharding.GetShardingConfigurationAsync(store);
                var settings = Sharding.Backup.GenerateShardRestoreSettings(dirs, sharding);

                // restore the database with a different name
                var restoredDatabaseName = $"restored_database-{Guid.NewGuid()}";
                using (Sharding.Backup.ReadOnly(backupPath))
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    DatabaseName = restoredDatabaseName,
                    ShardRestoreSettings = settings
                }, timeout: TimeSpan.FromSeconds(60)))
                {
                    var dbRec = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(restoredDatabaseName));

                    Assert.Equal(3, dbRec.Sharding.Shards.Count);

                    var server = cluster.Nodes.Single(n => n.ServerStore.NodeTag == sharding.Shards[oldLocation].Members[0]);
                    var oldLocationShard = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(ShardHelper.ToShardName(restoredDatabaseName, oldLocation));
                    using (oldLocationShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var docs = oldLocationShard.DocumentsStorage.GetDocumentsFrom(ctx, 0).ToList();
                        Assert.Equal(0, docs.Count);

                        var tombs = oldLocationShard.DocumentsStorage.GetTombstonesFrom(ctx, 0).ToList();

                        Assert.Equal(10, tombs.Count);
                        Assert.All(tombs, t => ((DocumentReplicationItem)t).Flags.Contain(DocumentFlags.Artificial));
                        Assert.All(tombs, t => ((DocumentReplicationItem)t).Flags.Contain(DocumentFlags.FromResharding));
                    }

                    server = cluster.Nodes.Single(n => n.ServerStore.NodeTag == sharding.Shards[newLocation].Members[0]);
                    var newLocationShard = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(ShardHelper.ToShardName(restoredDatabaseName, newLocation));
                    using (newLocationShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var docs = newLocationShard.DocumentsStorage.GetDocumentsFrom(ctx, 0).ToList();
                        Assert.Equal(11, docs.Count);

                        var tombs = newLocationShard.DocumentsStorage.GetTombstonesFrom(ctx, 0).ToList();
                        Assert.Equal(0, tombs.Count);
                    }

                    using (var session = store.OpenSession(restoredDatabaseName))
                    {
                        for (int i = 1; i <= 11; i++)
                        {
                            var docId = $"users/{i}${suffix}";
                            var doc = session.Load<User>(id);
                            Assert.True(doc != null, await AddDebugInfoOnFailure(docId));
                        }

                        async Task<string> AddDebugInfoOnFailure(string docId)
                        {
                            var sb = new StringBuilder();
                            sb.AppendLine($"failed to load document '{docId}' from restored database");

                            var count = session.Query<User>().Count();
                            sb.AppendLine($"count of documents : {count}");

                            var restored = await Sharding.GetShardingConfigurationAsync(store, restoredDatabaseName);

                            using (var ctx = JsonOperationContext.ShortTermSingleUse())
                            {
                                sb.AppendLine("sharding configuration of original database :")
                                    .AppendLine(store.Conventions.Serialization.DefaultConverter.ToBlittable(sharding, ctx).ToString());

                                sb.AppendLine("sharding configuration of restored database :")
                                    .AppendLine(store.Conventions.Serialization.DefaultConverter.ToBlittable(restored, ctx).ToString());
                            }

                            return sb.ToString();
                        }
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ShouldHaveValidBucketStatsAfterBucketMigration()
        {
            DoNotReuseServer();

            using (var store = Sharding.GetDocumentStore())
            {
                var suffix = "usa";
                var id1 = $"users/1${suffix}";
                var id2 = $"users/2${suffix}";
                var id3 = $"users/3${suffix}";
                var id4 = $"users/4${suffix}";

                await InsertData(store);
                var bucket = await Sharding.GetBucketAsync(store, id1);
                var oldLocation = await Sharding.GetShardNumberForAsync(store, id1);

                var originalShard = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, oldLocation));
                using (originalShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
                    Assert.Equal(bucket, stats.Bucket);
                    Assert.Equal(9121, stats.Size);
                    Assert.Equal(4, stats.NumberOfDocuments);
                }

                await Sharding.Resharding.MoveShardForId(store, id1);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, oldLocation)))
                {
                    var user = await session.LoadAsync<User>(id1);
                    Assert.Null(user);

                    user = await session.LoadAsync<User>(id2);
                    Assert.Null(user);

                    user = await session.LoadAsync<User>(id3);
                    Assert.Null(user);

                    user = await session.LoadAsync<User>(id4);
                    Assert.Null(user);
                }

                using (originalShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
                    Assert.Equal(bucket, stats.Bucket);
                    Assert.Equal(2794, stats.Size); // we still have 'artificial' tombstones on this shard
                    Assert.Equal(0, stats.NumberOfDocuments);
                }

                using (originalShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = originalShard.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Assert.Equal(22, tombs.Count);

                    foreach (var item in tombs)
                    {
                        DocumentFlags flags = item switch
                        {
                            DocumentReplicationItem documentReplicationItem => documentReplicationItem.Flags,
                            AttachmentTombstoneReplicationItem attachmentTombstone => attachmentTombstone.Flags,
                            RevisionTombstoneReplicationItem revisionTombstone => revisionTombstone.Flags,
                            _ => DocumentFlags.None
                        };

                        Assert.True(flags.Contain(DocumentFlags.Artificial));
                        Assert.True(flags.Contain(DocumentFlags.FromResharding));
                    }

                    await originalShard.TombstoneCleaner.ExecuteCleanup();
                }

                using (originalShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombsCount = originalShard.DocumentsStorage.GetNumberOfTombstones(context);
                    Assert.Equal(0, tombsCount);

                    var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(context, bucket);
                    Assert.Null(stats);
                }

                var newLocation = await Sharding.GetShardNumberForAsync(store, id1);
                Assert.NotEqual(oldLocation, newLocation);

                var newShard = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, newLocation));
                using (newShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
                    Assert.Equal(bucket, stats.Bucket);
                    Assert.Equal(9987, stats.Size);
                    Assert.Equal(4, stats.NumberOfDocuments);
                }

                await CheckData(store, database: ShardHelper.ToShardName(store.Database, newLocation), expectedRevisionsCount: 11);
            }
        }

        private static void AddEtl(IDocumentStore source, IDocumentStore destination)
        {
            var taskName = "etl-test";
            var csName = "cs-test";

            var configuration = new RavenEtlConfiguration
            {
                ConnectionStringName = csName, 
                Name = taskName, 
                Transforms =
                {
                    new Transformation
                    {
                        Name = "S1", 
                        Collections = { "Users" }
                    }
                }
            };

            var connectionString = new RavenConnectionString
            {
                Name = csName, 
                TopologyDiscoveryUrls = destination.Urls, 
                Database = destination.Database,
            };

            var putResult = source.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
            Assert.NotNull(putResult.RaftCommandIndex);
            source.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(configuration));
        }

        private static async Task InsertData(IDocumentStore store)
        {
            var suffix = "usa";
            var id1 = $"users/1${suffix}";
            var id2 = $"users/2${suffix}";
            var id3 = $"users/3${suffix}";
            var id4 = $"users/4${suffix}";

            using (var session = store.OpenAsyncSession())
            {
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false
                    }
                }));

                //Docs
                await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1", Age = 5 }, id1);
                await session.StoreAsync(new User { Name = "Name2", LastName = "LastName2", Age = 78 }, id2);
                await session.StoreAsync(new User { Name = "Name3", LastName = "LastName3", Age = 4 }, id3);
                await session.StoreAsync(new User { Name = "Name4", LastName = "LastName4", Age = 15 }, id4);

                //Time series
                session.TimeSeriesFor(id1, "Heartrate")
                    .Append(DateTime.Now, 59d, "watches/fitbit");
                session.TimeSeriesFor(id2, "Heartrate")
                    .Append(DateTime.Now.AddHours(6), 59d, "watches/fitbit");

                //counters
                session.CountersFor(id3).Increment("Downloads", 100);

                //Attachments
                var names = new[]
                {
                    "background-photo.jpg",
                    "fileNAME_#$1^%_בעברית.txt",
                    "profile.png"
                };

                await using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                await using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                await using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    session.Advanced.Attachments.Store(id1, names[0], backgroundStream, "ImGgE/jPeG");
                    session.Advanced.Attachments.Store(id2, names[1], fileStream);
                    session.Advanced.Attachments.Store(id3, names[2], profileStream, "image/png");
                    await session.SaveChangesAsync();
                }
            }
            
            // revision
            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(id1);
                user.Age = 10;
                await session.SaveChangesAsync();
            }
        }

        private async Task CheckData(IDocumentStore store, string database = null, long expectedRevisionsCount = 12)
        {
            database ??= store.Database;
            var db = await GetDocumentDatabaseInstanceFor(store, database);
            var storage = db.DocumentsStorage;

            var docsCount = storage.GetNumberOfDocuments();
            using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                //tombstones
                var tombstonesCount = storage.GetNumberOfTombstones(context);
                Assert.Equal(0, tombstonesCount);

                //revisions
                var revisionsCount = storage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
                Assert.Equal(expectedRevisionsCount, revisionsCount);
            }

            //docs
            Assert.Equal(4, docsCount);

            var suffix = "usa";
            using (var session = store.OpenSession(database))
            {
                var val = session.TimeSeriesFor("users/1$usa", "Heartrate")
                    .Get(DateTime.MinValue, DateTime.MaxValue);

                Assert.Equal(1, val.Length);

                val = session.TimeSeriesFor("users/2$usa", "Heartrate")
                    .Get(DateTime.MinValue, DateTime.MaxValue);

                Assert.Equal(1, val.Length);

                //Counters
                var counterValue = session.CountersFor($"users/3${suffix}").Get("Downloads");
                Assert.Equal(100, counterValue.Value);
            }

            //Attachments
            using (var session = store.OpenAsyncSession(database))
            {
                var attachmentNames = new[]
                {
                    "background-photo.jpg",
                    "fileNAME_#$1^%_בעברית.txt",
                    "profile.png"
                };

                for (var i = 0; i < attachmentNames.Length; i++)
                {
                    var id = $"users/{i + 1}${suffix}";
                    var user = await session.LoadAsync<User>(id);
                    var metadata = session.Advanced.GetMetadataFor(user);

                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Assert.Equal(1, attachments.Length);
                    
                    var attachment = attachments[0];
                    var name = attachment.GetString(nameof(AttachmentName.Name));
                    var hash = attachment.GetString(nameof(AttachmentName.Hash));
                    var size = attachment.GetLong(nameof(AttachmentName.Size));

                    Assert.Equal(attachmentNames[i], name);

                    string expectedHash = default;
                    long expectedSize = default;

                    switch (i)
                    {
                        case 0:
                            expectedHash = "igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=";
                            expectedSize = 5;
                            break;
                        case 1:
                            expectedHash = "Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=";
                            expectedSize = 5;
                            break;
                        case 2:
                            expectedHash = "EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=";
                            expectedSize = 3;
                            break;
                    }

                    Assert.Equal(expectedHash, hash);
                    Assert.Equal(expectedSize, size);

                    var attachmentResult = await session.Advanced.Attachments.GetAsync(id, name);
                    Assert.NotNull(attachmentResult);
                }
            }
        }

        private static Task SetupReplication(IDocumentStore fromStore, IDocumentStore toStore)
        {
            var databaseWatcher = new ExternalReplication(toStore.Database, $"ConnectionString-{toStore.Identifier}");
            return ReplicationTestBase.AddWatcherToReplicationTopology(fromStore, databaseWatcher, fromStore.Urls);
        }

        private class UsersByName : AbstractIndexCreationTask<User>
        {
            public UsersByName()
            {
                Map = users =>
                    from user in users
                    select new { user.Name };
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task GetDocuments()
        {
            using var store = Sharding.GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User(), "users/1-A");
                await session.SaveChangesAsync();
            }

            for (int i = 3; i < 100; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), $"num-{i}$users/1-A");
                    await session.StoreAsync(new User(), $"users/{i}-A");
                    await session.SaveChangesAsync();
                }
            }

            var sp = Stopwatch.StartNew();
            
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            sp.Restart();
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            sp.Restart();
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            sp.Restart();
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            sp.Restart();
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            sp.Restart();
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            sp.Restart();
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            sp.Restart();
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            sp.Restart();
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            sp.Restart();

            using (var session = store.OpenAsyncSession())
            {
                var total = await session.Query<User>().CountAsync();
                Assert.Equal(195, total);
            }
        }


        [RavenFact(RavenTestCategory.Sharding)]
        public async Task GetDocuments2()
        {
            using var store = Sharding.GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                await AddOrUpdateUserAsync(session, "users/1-A");
                await session.SaveChangesAsync();
            }

            var writes = Task.Run(async () =>
            {
                for (int j = 0; j < 10; j++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await AddOrUpdateUserAsync(session, "users/1-A");
                        await session.SaveChangesAsync();
                    }

                    for (int i = 3; i < 100; i++)
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            await AddOrUpdateUserAsync(session, $"num-{i}$users/1-A");
                            await AddOrUpdateUserAsync(session, $"users/{i}-A");
                            await session.SaveChangesAsync();
                        }
                    }
                }
            });
                
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");

            await writes;

            using (var session = store.OpenAsyncSession())
            {
                var total = await session.Query<User>().CountAsync();
                Assert.Equal(195, total);
            }
        }

        private static async Task AddOrUpdateUserAsync(IAsyncDocumentSession session, string id)
        {
            var current = await session.LoadAsync<User>(id);
            if (current == null)
            {
                current = new User();
                await session.StoreAsync(current, id);
            }

            if (current.Age == 0)
            {
                current.Age = Random.Shared.Next(-1, 1);
            }
            else
            {
                if (current.Age > 0)
                    current.Age++;
                else
                    current.Age--;

                current.Age *= -1;
            }

            // current.Age = Random.Shared.Next(-10, 10);
        }
    }
}

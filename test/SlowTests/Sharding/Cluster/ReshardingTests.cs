using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Orders;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Identities;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
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

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task BucketDeletionShouldCreateArtificialTombstones()
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
                }

                WaitForUserToContinueTheTest(store);

                var newLocation = await Sharding.GetShardNumber(store, id);
                Assert.NotEqual(oldLocation, newLocation);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var order = await session.LoadAsync<Order>(id);
                    Assert.NotNull(order);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task BucketDeletionShouldCreateArtificialTombstones2()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                await InsertData(store);

                var suffix = "$usa";
                var id1 = $"users/1{suffix}";
                var id2 = $"users/2{suffix}";
                var id3 = $"users/3{suffix}";
                var id4 = $"users/4{suffix}";

                var oldLocation = await Sharding.GetShardNumber(store, id1);

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
                }

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, oldLocation));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = db.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Assert.Equal(20, tombs.Count);

                    /*foreach (var item in tombs)
                    {
                        switch (item)
                        {
                            case AttachmentTombstoneReplicationItem attachmentTombstoneReplicationItem:
                                break;
                            case DocumentReplicationItem documentReplicationItem:
                                break;
                            case RevisionTombstoneReplicationItem revisionTombstoneReplicationItem:
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(nameof(item));
                        }
                        var tomb = (DocumentReplicationItem)item;
                        //Assert.Equal(id.ToLower(), tomb.Id.ToString(CultureInfo.InvariantCulture));
                        Assert.Equal(ReplicationBatchItem.ReplicationItemType.DocumentTombstone, tomb.Type);
                        Assert.True(tomb.Flags.Contain(DocumentFlags.Artificial));
                    }*/
                }

                //WaitForUserToContinueTheTest(store);

                var newLocation = await Sharding.GetShardNumber(store, id1);
                Assert.NotEqual(oldLocation, newLocation);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var user = await session.LoadAsync<User>(id1);
                    Assert.NotNull(user);

                    user = await session.LoadAsync<User>(id2);
                    Assert.NotNull(user);

                    user = await session.LoadAsync<User>(id3);
                    Assert.NotNull(user);
                }

                await CheckData(store, database: ShardHelper.ToShardName(store.Database, newLocation));
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
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

                var oldLocation = await Sharding.GetShardNumber(store, id);

                await Sharding.Resharding.MoveShardForId(store, id);

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, oldLocation));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = db.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Assert.Equal(22, tombs.Count);
                }

                WaitForUserToContinueTheTest(store);

                var newLocation = await Sharding.GetShardNumber(store, id);

                await CheckData(store, ShardHelper.ToShardName(store.Database, newLocation));

                await CheckData(replica);
            }
        }

        private static async Task InsertData(IDocumentStore store)
        {
            var suffix = "$usa";
            var id1 = $"users/1{suffix}";
            var id2 = $"users/2{suffix}";
            var id3 = $"users/3{suffix}";
            var id4 = $"users/4{suffix}";

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

                //todo fix timeseries migration
                //Time series
                /*session.TimeSeriesFor(id1, "Heartrate")
                    .Append(DateTime.Now, 59d, "watches/fitbit");
                session.TimeSeriesFor(id2, "Heartrate")
                    .Append(DateTime.Now.AddHours(6), 59d, "watches/fitbit");*/

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

            //tombstone + revision
            using (var session = store.OpenAsyncSession())
            {
                // todo fix tombstones migration 
                //session.Delete(id4);

                var user = await session.LoadAsync<User>(id1);
                user.Age = 10;
                await session.SaveChangesAsync();
            }
        }

        public async Task CheckData(IDocumentStore store, string database = null)
        {
            database ??= store.Database;
            var db = await GetDocumentDatabaseInstanceFor(store, database);
            var storage = db.DocumentsStorage;

            var docsCount = storage.GetNumberOfDocuments();
            using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var tombstonesCount = storage.GetNumberOfTombstones(context);
                //tombstone
                Assert.Equal(0, tombstonesCount);

                var revisionsCount = storage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
                Assert.Equal(10, revisionsCount);
            }

            //doc
            Assert.Equal(4, docsCount);

            using (var session = store.OpenSession(database))
            {
                //todo Time series
                /*var val = session.TimeSeriesFor("users/1$usa", "Heartrate")
                    .Get(DateTime.MinValue, DateTime.MaxValue);

                Assert.Equal(1, val.Length);

                val = session.TimeSeriesFor("users/2$usa", "Heartrate")
                    .Get(DateTime.MinValue, DateTime.MaxValue);

                Assert.Equal(1, val.Length);*/

                //Counters
                var counterValue = session.CountersFor("users/3$usa").Get("Downloads");
                Assert.Equal(100, counterValue.Value);
            }

            using (var session = store.OpenAsyncSession(database))
            {
                var attachmentNames = new[]
                {
                    "background-photo.jpg",
                    "fileNAME_#$1^%_בעברית.txt",
                    "profile.png"
                };

                var suffix = "usa";
                for (var i = 0; i < attachmentNames.Length; i++)
                {
                    var id = $"users/{(i + 1)}${suffix}";
                    var user = await session.LoadAsync<User>(id);
                    var metadata = session.Advanced.GetMetadataFor(user);

                    //Attachment
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
    }
}

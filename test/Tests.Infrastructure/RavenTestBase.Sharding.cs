using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Identities;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit;

namespace FastTests;

public partial class RavenTestBase
{
    public readonly ShardingTestBase Sharding;

    public class ShardingTestBase
    {
        public ShardedBackupTestsBase Backup;

        private readonly RavenTestBase _parent;
        public readonly ReshardingTestBase Resharding;

        public ShardingTestBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
            Backup = new ShardedBackupTestsBase(_parent);
            Resharding = new ReshardingTestBase(_parent);
        }

        public IDocumentStore GetDocumentStore(Options options = null, [CallerMemberName] string caller = null, DatabaseTopology[] shards = null)
        {
            var shardedOptions = options ?? new Options();
            shardedOptions.ModifyDatabaseRecord += r =>
            {
                r.Sharding ??= new ShardingConfiguration();

                if (shards == null)
                {
                    r.Sharding.Shards = new[]
                    {
                        new DatabaseTopology(),
                        new DatabaseTopology(),
                        new DatabaseTopology(),
                    };
                }
                else
                {
                    r.Sharding.Shards = shards;
                }
            };
            return _parent.GetDocumentStore(shardedOptions, caller);
        }

        public async Task<int> GetShardNumber(IDocumentStore store, string id)
        {
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var bucket = ShardHelper.GetBucket(id);
            return ShardHelper.GetShardNumber(record.Sharding.ShardBucketRanges, bucket);
        }

        public async Task<IEnumerable<DocumentDatabase>> GetShardsDocumentDatabaseInstancesFor(IDocumentStore store, string database = null)
        {
            var dbs = new List<DocumentDatabase>();
            foreach (var task in _parent.Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(database ?? store.Database))
            {
                dbs.Add(await task);
            }

            return dbs;
        }

        public bool AllShardHaveDocs(IDictionary<string, List<DocumentDatabase>> servers, long count = 1L)
        {
            foreach (var kvp in servers)
            {
                foreach (var documentDatabase in kvp.Value)
                {
                    using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var ids = documentDatabase.DocumentsStorage.GetNumberOfDocuments(context);
                        if (ids < count)
                            return false;
                    }
                }
            }

            return true;
        }

        public class ShardedBackupTestsBase
        {
            internal readonly RavenTestBase _parent;

            public ShardedBackupTestsBase(RavenTestBase parent)
            {
                _parent = parent ?? throw new ArgumentNullException(nameof(parent));
            }

            public async Task InsertData(IDocumentStore store, IReadOnlyList<string> names)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await SetupRevisionsAsync(store, configuration: new RevisionsConfiguration
                    {
                        Default = new RevisionsCollectionConfiguration
                        {
                            Disabled = false
                        }
                    });

                    //Docs
                    await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1", Age = 5 }, "users/1");
                    await session.StoreAsync(new User { Name = "Name2", LastName = "LastName2", Age = 78 }, "users/2");
                    await session.StoreAsync(new User { Name = "Name1", LastName = "LastName3", Age = 4 }, "users/3");
                    await session.StoreAsync(new User { Name = "Name2", LastName = "LastName4", Age = 15 }, "users/4");

                    //Time series
                    session.TimeSeriesFor("users/1", "Heartrate")
                        .Append(DateTime.Now, 59d, "watches/fitbit");
                    session.TimeSeriesFor("users/3", "Heartrate")
                        .Append(DateTime.Now.AddHours(6), 59d, "watches/fitbit");
                    //counters
                    session.CountersFor("users/2").Increment("Downloads", 100);
                    //Attachments
                    await using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                    await using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                    await using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                    {
                        session.Advanced.Attachments.Store("users/1", names[0], backgroundStream, "ImGgE/jPeG");
                        session.Advanced.Attachments.Store("users/2", names[1], fileStream);
                        session.Advanced.Attachments.Store("users/3", names[2], profileStream, "image/png");
                        await session.SaveChangesAsync();
                    }
                }

                //tombstone + revision
                using (var session = store.OpenAsyncSession())
                {
                    session.Delete("users/4");
                    var user = await session.LoadAsync<User>("users/1");
                    user.Age = 10;
                    await session.SaveChangesAsync();
                }

                //subscription
                await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User>());

                //Identity
                var result1 = store.Maintenance.Send(new SeedIdentityForOperation("users", 1990));

                //CompareExchange
                var user1 = new User
                {
                    Name = "Toli"
                };
                var user2 = new User
                {
                    Name = "Mitzi"
                };

                var operationResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("cat/toli", user1, 0));
                operationResult = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<User>("cat/mitzi", user2, 0));
                var result = await store.Operations.SendAsync(new DeleteCompareExchangeValueOperation<User>("cat/mitzi", operationResult.Index));

                //Cluster transaction
                using (var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var user5 = new User { Name = "Ayende" };
                    await session.StoreAsync(user5, "users/5");
                    await session.StoreAsync(new { ReservedFor = user5.Id }, "usernames/" + user5.Name);

                    await session.SaveChangesAsync();
                }

                //Index
                await new Index().ExecuteAsync(store);
            }

            public async Task CheckData(IDocumentStore store, IReadOnlyList<string> attachmentNames, RavenDatabaseMode dbMode = RavenDatabaseMode.Single)
            {
                long docsCount = default, tombstonesCount = default, revisionsCount = default;
                if (dbMode == RavenDatabaseMode.Sharded)
                {
                    var dbs = _parent.Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database);
                    foreach (var task in dbs)
                    {
                        var shard = await task;
                        var storage = shard.DocumentsStorage;

                        docsCount += storage.GetNumberOfDocuments();
                        using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            tombstonesCount += storage.GetNumberOfTombstones(context);
                            revisionsCount += storage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
                        }

                        //Index
                        Assert.Equal(1, shard.IndexStore.Count);
                    }
                }
                else
                {
                    var db = await _parent.GetDocumentDatabaseInstanceFor(store, store.Database);
                    var storage = db.DocumentsStorage;

                    docsCount = storage.GetNumberOfDocuments();
                    using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        tombstonesCount = storage.GetNumberOfTombstones(context);
                        revisionsCount = storage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
                    }

                    //Index
                    var indexes = await store.Maintenance.SendAsync(new GetIndexesOperation(0, 128));
                    Assert.Equal(1, indexes.Length);
                }

                //doc
                Assert.Equal(5, docsCount);

                //Assert.Equal(1, detailedStats.CountOfCompareExchangeTombstones); //TODO - Not working for 4.2

                //tombstone
                Assert.Equal(1, tombstonesCount);

                //revisions
                Assert.Equal(28, revisionsCount);

                //Subscriptions
                var subscriptionDocuments = await store.Subscriptions.GetSubscriptionsAsync(0, 10);
                Assert.Equal(1, subscriptionDocuments.Count);

                using (var session = store.OpenSession())
                {
                    //Time series
                    var val = session.TimeSeriesFor("users/1", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue);

                    Assert.Equal(1, val.Length);

                    val = session.TimeSeriesFor("users/3", "Heartrate")
                        .Get(DateTime.MinValue, DateTime.MaxValue);

                    Assert.Equal(1, val.Length);

                    //Counters
                    var counterValue = session.CountersFor("users/2").Get("Downloads");
                    Assert.Equal(100, counterValue.Value);
                }

                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < attachmentNames.Count; i++)
                    {
                        var user = await session.LoadAsync<User>("users/" + (i + 1));
                        var metadata = session.Advanced.GetMetadataFor(user);

                        //Attachment
                        var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                        Assert.Equal(1, attachments.Length);
                        var attachment = attachments[0];
                        Assert.Equal(attachmentNames[i], attachment.GetString(nameof(AttachmentName.Name)));
                        var hash = attachment.GetString(nameof(AttachmentName.Hash));
                        if (i == 0)
                        {
                            Assert.Equal("igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=", hash);
                            Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                        }
                        else if (i == 1)
                        {
                            Assert.Equal("Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=", hash);
                            Assert.Equal(5, attachment.GetLong(nameof(AttachmentName.Size)));
                        }
                        else if (i == 2)
                        {
                            Assert.Equal("EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=", hash);
                            Assert.Equal(3, attachment.GetLong(nameof(AttachmentName.Size)));
                        }
                    }

                    await session.StoreAsync(new User() { Name = "Toli" }, "users|");
                    await session.SaveChangesAsync();
                }
                //Identity
                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1991");
                    Assert.NotNull(user);
                }
                //CompareExchange
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var user = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("cat/toli");
                    Assert.NotNull(user);

                    user = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("rvn-atomic/usernames/Ayende");
                    Assert.NotNull(user);

                    user = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<User>("rvn-atomic/users/5");
                    Assert.NotNull(user);

                    var user2 = await session.LoadAsync<User>("users/5");
                    Assert.NotNull(user2);

                    user2 = await session.LoadAsync<User>("usernames/Ayende");
                    Assert.NotNull(user2);
                }
            }

            public Task<WaitHandle[]> WaitForBackupToComplete(IDocumentStore store)
            {
                return WaitForBackupsToComplete(new[] { store });
            }

            public async Task<WaitHandle[]> WaitForBackupsToComplete(IEnumerable<IDocumentStore> stores)
            {
                var waitHandles = new List<WaitHandle>();

                foreach (var store in stores)
                {
                    var dbs = _parent.Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database).ToList();
                    foreach (var task in dbs)
                    {
                        var mre = new ManualResetEventSlim();
                        waitHandles.Add(mre.WaitHandle);

                        var db = await task;
                        db.PeriodicBackupRunner._forTestingPurposes ??= new PeriodicBackupRunner.TestingStuff();
                        db.PeriodicBackupRunner._forTestingPurposes.AfterBackupBatchCompleted = () => mre.Set();
                    }
                }

                return waitHandles.ToArray();
            }

            public async Task UpdateConfigurationAndRunBackupAsync(RavenServer server, IDocumentStore store, PeriodicBackupConfiguration config, bool isFullBackup = false)
            {
                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                var shards = server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database);
                foreach (var shard in shards)
                {
                    var documentDatabase = await shard;
                    var periodicBackupRunner = documentDatabase.PeriodicBackupRunner;
                    periodicBackupRunner.StartBackupTask(result.TaskId, isFullBackup);
                }
            }

            private static async Task<long> SetupRevisionsAsync(
                IDocumentStore store,
                RevisionsConfiguration configuration)
            {
                if (store == null)
                    throw new ArgumentNullException(nameof(store));

                var result = await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRevisionsOperation(configuration));
                return result.RaftCommandIndex ?? -1;
            }

            private class Item
            {

            }

            private class Index : AbstractIndexCreationTask<Item>
            {
                public Index()
                {
                    Map = items =>
                        from item in items
                        select new
                        {
                            _ = new[]
                            {
                                CreateField("foo", "a"),
                                CreateField("foo", "b"),
                            }
                        };
                }
            }
        }

        public class ReshardingTestBase
        {
            private readonly RavenTestBase _parent;

            public ReshardingTestBase(RavenTestBase parent)
            {
                _parent = parent;
            }

            public async Task MoveShardForId(IDocumentStore store, string id, List<RavenServer> servers = null)
            {
                if (_parent.Servers.Count > 0)
                {
                    servers ??= _parent.Servers;
                }
                else
                {
                    servers ??= new List<RavenServer> { _parent.Server };
                }

                var server = servers[0].ServerStore;

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var bucket = ShardHelper.GetBucket(id);
                var location = ShardHelper.GetShardNumber(record.ShardBucketRanges, bucket);
                var newLocation = (location + 1) % record.Shards.Length;

                var destination = record.Shards[newLocation];
                var source = record.Shards[location];


                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, location)))
                {
                    var user = await session.Advanced.ExistsAsync(id);
                    Assert.NotNull(user);
                }

                var result = await server.Sharding.StartBucketMigration(store.Database, bucket, location, newLocation);
                var migrationIndex = result.Index;

                var exists = _parent.WaitForDocument<dynamic>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, newLocation));
                Assert.True(exists, $"{id} wasn't found at shard {newLocation}");

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var user = await session.LoadAsync<dynamic>(id);
                    var changeVector = session.Advanced.GetChangeVectorFor(user);

                    result = await server.Sharding.SourceMigrationCompleted(store.Database, bucket, migrationIndex, changeVector);
                    await _parent.Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.Index, servers);
                }

                foreach (var s in servers)
                {
                    if (destination.AllNodes.Contains(s.ServerStore.NodeTag) == false)
                        continue;

                    result = await s.ServerStore.Sharding.DestinationMigrationConfirm(store.Database, bucket, migrationIndex);
                }
                await _parent.Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.Index, servers);

                foreach (var s in servers)
                {
                    if (source.AllNodes.Contains(s.ServerStore.NodeTag) == false)
                        continue;

                    result = await s.ServerStore.Sharding.SourceMigrationCleanup(store.Database, bucket, migrationIndex);
                }
                await _parent.Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.Index, servers);
            }
        }
    }
}


using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Raven.Client.ServerWide.Sharding;
using Raven.Server;
using Raven.Server.Documents.Commands.Replication;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Replication
{
    public class ShardedExternalReplicationTests : ReplicationTestBase
    {
        public ShardedExternalReplicationTests(ITestOutputHelper output) : base(output)
        {
        }

        private const DatabaseItemType AllTypes = DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments | DatabaseItemType.Tombstones |
                                                  DatabaseItemType.Conflicts | DatabaseItemType.Attachments | DatabaseItemType.CounterGroups |
                                                  DatabaseItemType.TimeSeries;


        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task EnsureCantChooseMentorNodeForShardedExternalReplication()
        {
            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore())
            {
                var error = await Assert.ThrowsAnyAsync<RavenException>(async () =>
                {
                    await SetupReplicationAsync(store1, responsibleNode: "A", new IDocumentStore[] { store2 });
                });
                Assert.Contains("Choosing a mentor node for an ongoing task is not supported in sharding", error.Message);
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task GetReplicationActiveConnectionsShouldWork()
        {
            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore())
            {
                await SetupReplicationAsync(store1, store2);

                using (var s1 = store1.OpenSession())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        s1.Store(new User(), $"foo/bar/{i}");
                    }

                    s1.SaveChanges();
                }

                for (int i = 0; i < 50; i++)
                {
                    var id = $"foo/bar/{i}";
                    Assert.True(WaitForDocument<User>(store2, id, predicate: null, timeout: 30_000));
                }

                var dbs = Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store2.Database);
                foreach (var task in dbs)
                {
                    var db = await task;
                    var shardNumber = ShardHelper.GetShardNumberFromDatabaseName(db.Name);
                    var replicationActiveConnections = await store2.Maintenance.ForShard(shardNumber).SendAsync(new GetReplicationActiveConnectionsInfoOperation());
                    Assert.NotNull(replicationActiveConnections.IncomingConnections);
                    Assert.Equal(3, replicationActiveConnections.IncomingConnections.Count);
                    Assert.Empty(replicationActiveConnections.OutgoingConnections);
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ExternalReplicationFromNonShardedToShardedShouldWork()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore())
            {
                await SetupReplicationAsync(store1, store2);

                using (var s1 = store1.OpenSession())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        s1.Store(new User(), $"foo/bar/{i}");
                    }

                    s1.SaveChanges();
                }

                for (int i = 0; i < 50; i++)
                {
                    var id = $"foo/bar/{i}";
                    Assert.True(WaitForDocument<User>(store2, id, predicate: null, timeout: 30_000));
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ExternalReplicationFromNonShardedToShardedShouldWork2()
        {
            using (var store1 = GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"NonSharded_{s}"
            }))
            using (var store2 = Sharding.GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"Sharded_{s}"
            }))
            {
                await SetupReplicationAsync(store1, store2);

                await store1.Maintenance.SendAsync(new CreateSampleDataOperation(AllTypes));

                await EnsureReplicatingAsync(store1, store2);

                WaitForUserToContinueTheTest(store2);
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ExternalReplicationFromShardedToShardedShouldWork()
        {
            using (var store1 = Sharding.GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"Source_{s}"
            }))
            using (var store2 = Sharding.GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"Destination_{s}"
            }))
            {
                await SetupReplicationAsync(store1, store2);

                using (var s1 = store1.OpenSession())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        s1.Store(new User(), $"foo/bar/{i}");
                    }

                    s1.SaveChanges();
                }

                for (int i = 0; i < 50; i++)
                {
                    var id = $"foo/bar/{i}";
                    Assert.True(WaitForDocument<User>(store2, id, predicate: null, timeout: 30_000));
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ExternalReplicationFromShardedToShardedShouldWork2()
        {
            using (var store1 = Sharding.GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"NonSharded_{s}"
            }))
            using (var store2 = Sharding.GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"Sharded_{s}"
            }))
            {
                await SetupReplicationAsync(store1, store2);

                await store1.Maintenance.SendAsync(new CreateSampleDataOperation(AllTypes));
                WaitForUserToContinueTheTest(store2);

                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                WaitForUserToContinueTheTest(store2);
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task EnsureNoReplicationLoopInExternalReplicationBetweenTwoShardedDBs()
        {
            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore())
            {
                using (var s1 = store1.OpenSession())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        s1.Store(new User(), $"foo/bar/{i}");
                    }

                    s1.SaveChanges();
                }

                using (var s2 = store2.OpenSession())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        s2.Store(new User(), $"users/{i}");
                    }

                    s2.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store2, store1);

                for (int i = 0; i < 50; i++)
                {
                    var id = $"foo/bar/{i}";
                    Assert.True(WaitForDocument<User>(store2, id, predicate: null, timeout: 30_000));
                }

                for (int i = 0; i < 50; i++)
                {
                    var id = $"users/{i}";
                    Assert.True(WaitForDocument<User>(store1, id, predicate: null, timeout: 30_000));
                }

                await ShardingCluster.EnsureNoReplicationLoopForSharding(Server, store1.Database);
                await ShardingCluster.EnsureNoReplicationLoopForSharding(Server, store2.Database);
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task EnsureNoReplicationLoopInExternalReplicationBetweenTwoShardedDBs2()
        {
            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore())
            {
                using (var s1 = store1.OpenSession())
                {
                    s1.Store(new User() { Name = "Shiran" }, "users/1");
                    s1.SaveChanges();
                }

                using (var s2 = store2.OpenSession())
                {
                    s2.Store(new User() { Name = "Queeni" }, "users/1");
                    s2.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                await SetupReplicationAsync(store2, store1);
                await EnsureReplicatingAsync(store2, store1);

                Assert.True(WaitForDocument<User>(store1, "users/1", predicate: u => u.Name == "Queeni", timeout: 30_000));
                Assert.True(WaitForDocument<User>(store2, "users/1", predicate: u => u.Name == "Queeni", timeout: 30_000));


                await ShardingCluster.EnsureNoReplicationLoopForSharding(Server, store1.Database);
                await ShardingCluster.EnsureNoReplicationLoopForSharding(Server, store2.Database);
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ExternalReplicationFromShardedToNonShardedShouldWork()
        {
            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await SetupReplicationAsync(store1, store2);

                using (var s1 = store1.OpenSession())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        s1.Store(new User(), $"foo/bar/{i}");
                    }

                    s1.SaveChanges();
                }

                for (int i = 0; i < 50; i++)
                {
                    var id = $"foo/bar/{i}";
                    Assert.True(WaitForDocument<User>(store2, id, predicate: null, timeout: 30_000));
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ExternalReplicationFromShardedToNonShardedShouldWork2()
        {
            using (var store1 = Sharding.GetDocumentStore(new Options
            {
                ModifyDatabaseName = s => $"Sharded_{s}"
            }))
            using (var store2 = GetDocumentStore())
            {
                await SetupReplicationAsync(store1, store2);

                await store1.Maintenance.SendAsync(new CreateSampleDataOperation(AllTypes));

                await EnsureReplicatingAsync(store1, store2);

                WaitForUserToContinueTheTest(store2);
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ServerWideExternalReplicationShouldWork_NonShardedToSharded()
        {
            var clusterSize = 3;
            var dbName = GetDatabaseName();

            var (_, leader) = await CreateRaftCluster(clusterSize);
            var (shardNodes, shardsLeader) = await CreateRaftCluster(clusterSize);

            await CreateDatabaseInCluster(dbName, 3, leader.WebUrl);
            await ShardingCluster.CreateShardedDatabaseInCluster(dbName, 3, (shardNodes, shardsLeader));

            using (var store = new DocumentStore() { Urls = new[] { leader.WebUrl }, Database = dbName }.Initialize())
            {
                var putConfiguration = new ServerWideExternalReplication
                {
                    MentorNode = leader.ServerStore.NodeTag,
                    TopologyDiscoveryUrls = shardNodes.Select(s => s.WebUrl).ToArray(),
                    Name = dbName
                };

                var result = await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration));
                var serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideExternalReplicationOperation(result.Name));
                Assert.NotNull(serverWideConfiguration);

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.Equal(1, record.ExternalReplications.Count);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    shardNodes,
                    dbName,
                    "users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(60)));
            }
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public async Task ExternalReplicationWithRevisionTombstones_NonShardedToNonSharded()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                await store1.Maintenance.ForDatabase(store1.Database).SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        PurgeOnDelete = true
                    }
                }));

                var id1 = "foo/bar/0";

                await SetupReplicationAsync(store1, store2);

                using (var s1 = store1.OpenSession())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        s1.Store(new User(), $"foo/bar/{i}");
                    }

                    s1.SaveChanges();
                }

                for (int i = 0; i < 50; i++)
                {
                    var id = $"foo/bar/{i}";
                    Assert.True(WaitForDocument<User>(store2, id, predicate: null, timeout: 30_000));
                }

                using (var s1 = store1.OpenSession())
                {
                    s1.Delete(id1);
                    s1.SaveChanges();
                }

                await EnsureReplicatingAsync(store1, store2);

                using (var session = store1.OpenSession())
                {
                    var revisions = session.Advanced.Revisions.GetFor<User>(id1);
                    Assert.Equal(0, revisions.Count);
                }

                using (var session = store2.OpenSession())
                {
                    var revisions = session.Advanced.Revisions.GetFor<User>(id1);
                    Assert.Equal(0, revisions.Count);
                }

                var db = await GetDocumentDatabaseInstanceFor(store1, store1.Database);
                var storage = db.DocumentsStorage;
                using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = storage.GetTombstonesFrom(context, 0).ToList();
                    Assert.Equal(2, tombs.Count);

                    int revisionTombsCount = 0, documentTombsCount = 0;
                    foreach (var item in tombs)
                    {
                        if (item is RevisionTombstoneReplicationItem)
                            revisionTombsCount++;
                        else if (item is DocumentReplicationItem)
                            documentTombsCount++;
                    }

                    Assert.Equal(1, revisionTombsCount);
                    Assert.Equal(1, documentTombsCount);
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ServerWideExternalReplicationShouldWork_ShardedToNonSharded()
        {
            var clusterSize = 3;
            var dbName = GetDatabaseName();

            var (nodes, leader) = await CreateRaftCluster(clusterSize);
            var (shardNodes, shardsLeader) = await CreateRaftCluster(clusterSize);

            await CreateDatabaseInCluster(dbName, 3, leader.WebUrl);
            await ShardingCluster.CreateShardedDatabaseInCluster(dbName, 3, (shardNodes, shardsLeader));

            using (var store = new DocumentStore() { Urls = new[] { shardsLeader.WebUrl }, Database = dbName }.Initialize())
            {
                var putConfiguration = new ServerWideExternalReplication
                {
                    MentorNode = shardsLeader.ServerStore.NodeTag,
                    TopologyDiscoveryUrls = nodes.Select(s => s.WebUrl).ToArray(),
                    Name = dbName
                };

                var result = await store.Maintenance.Server.SendAsync(new PutServerWideExternalReplicationOperation(putConfiguration));
                var serverWideConfiguration = await store.Maintenance.Server.SendAsync(new GetServerWideExternalReplicationOperation(result.Name));
                Assert.NotNull(serverWideConfiguration);

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.Equal(1, record.ExternalReplications.Count);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    nodes,
                    dbName,
                    "users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(60)));
            }
        }
        
        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        [RavenExternalReplication(RavenDatabaseMode.Sharded, RavenDatabaseMode.Sharded)]
        [RavenExternalReplication(RavenDatabaseMode.Sharded, RavenDatabaseMode.Single)]
        public async Task ExternalReplicationFailOverWithResharding(Options source, Options destination)
        {
            var clusterSize = 3;

            var (srcNodes, srcLeader) = await CreateRaftCluster(clusterSize);
            var (dstNodes, dstLeader) = await CreateRaftCluster(clusterSize);

            source = Replication.AdjustOptionsToClusterSize(source, srcLeader, clusterSize);
            destination = Replication.AdjustOptionsToClusterSize(destination, dstLeader, clusterSize);

            using (var src = GetDocumentStore(source))
            using (var dst = GetDocumentStore(destination))
            {
                using (var session = src.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(30), replicas: clusterSize - 1);
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                // add watcher with invalid url to test the failOver on database topology discovery
                var watcher = new ExternalReplication(dst.Database, "connection");
                var r = await AddWatcherToReplicationTopology(src, watcher, new[] { "http://127.0.0.1:1234", dstNodes[0].WebUrl, dstNodes[1].WebUrl });

                using (var dstSession = dst.OpenSession())
                {
                    dstSession.Load<User>("Karmel");
                    Assert.True(await WaitForDocumentInClusterAsync<User>(
                        dstNodes,
                        dst.Database,
                        "users/1",
                        u => u.Name.Equals("Karmel"),
                        TimeSpan.FromSeconds(60)));
                }

                await Sharding.Resharding.MoveShardForId(src, "users/1");

                var destinationUrl = await AssertWaitForNotNullAsync(async () =>
                {
                    var taskStatus = (OngoingTaskReplication)await GetExecutor(src.Maintenance).SendAsync(new GetOngoingTaskInfoOperation(r.TaskId, OngoingTaskType.Replication));
                    if (taskStatus?.DestinationUrl == null)
                    {
                        taskStatus = (OngoingTaskReplication)await GetExecutor(src.Maintenance).ForNode(taskStatus.ResponsibleNode.NodeTag).SendAsync(new GetOngoingTaskInfoOperation(r.TaskId, OngoingTaskType.Replication));
                    }

                    return taskStatus?.DestinationUrl;
                    
                    MaintenanceOperationExecutor GetExecutor(MaintenanceOperationExecutor executor)
                    {
                        if (source.DatabaseMode == RavenDatabaseMode.Sharded)
                        {
                            return executor.ForShard(0);
                        }

                        return executor;
                    }
                });

                await DisposeServerAndWaitForFinishOfDisposalAsync(Servers.Single(s => s.WebUrl == destinationUrl));

                using (var session = src.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(30), replicas: clusterSize - 1);
                    session.Store(new User
                    {
                        Name = "Karmel2"
                    }, "users/2");
                    session.SaveChanges();
                }

                Assert.True(WaitForDocument(dst, "users/2", 30_000));
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        [RavenExternalReplication(RavenDatabaseMode.Sharded, RavenDatabaseMode.Sharded)]
        [RavenExternalReplication(RavenDatabaseMode.Single, RavenDatabaseMode.Sharded)]
        public async Task BidirectionalReplicationWithReshardingShouldWork(Options source, Options destination)
        {
            var clusterSize = 3;

            var (srcNodes, srcLeader) = await CreateRaftCluster(clusterSize);
            var (dstNodes, dstLeader) = await CreateRaftCluster(clusterSize);

            source = Replication.AdjustOptionsToClusterSize(source, srcLeader, clusterSize);
            destination = Replication.AdjustOptionsToClusterSize(destination, dstLeader, clusterSize);

            using (var src = GetDocumentStore(source))
            using (var dst = GetDocumentStore(destination))
            {
                using (var session = src.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(30), replicas: clusterSize - 1);
                    session.Store(new User { Name = "Karmel" }, "users/1");
                    session.SaveChanges();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    srcNodes,
                    src.Database,
                    "users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(60)));


                await SetupReplicationAsync(src, dst);
                await SetupReplicationAsync(dst, src);

                Assert.True(WaitForDocument(dst, "users/1", 30_000));

                using (var session = dst.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/2$users/1");
                    session.SaveChanges();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    dstNodes,
                    dst.Database,
                    "users/2$users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(60)));

                Assert.True(WaitForDocument(src, "users/2$users/1", 30_000));

                var oldLocation = await Sharding.GetShardNumberForAsync(dst, "users/2$users/1");
                await Sharding.Resharding.MoveShardForId(dst, "users/2$users/1", servers: dstNodes);

                var db = await dstLeader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(ShardHelper.ToShardName(dst.Database, oldLocation));
                var storage = db.DocumentsStorage;
                using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    //tombstones
                    var tombstonesCount = storage.GetNumberOfTombstones(context);
                    Assert.Equal(2, tombstonesCount);
                }

                var docsCount = storage.GetNumberOfDocuments();
                Assert.Equal(0, docsCount);

                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    srcNodes,
                    src.Database,
                    "users/2$users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(60)));

                foreach (var server in srcNodes)
                {
                    await Replication.EnsureNoReplicationLoopAsync(source.DatabaseMode, server, src.Database);
                }
                
                foreach (var server in dstNodes)
                {
                    await Replication.EnsureNoReplicationLoopAsync(destination.DatabaseMode, server, dst.Database);
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ReplicationShouldResumeAfterDeletingAndRestartingShardDatabase()
        {
            var src = GetDocumentStore(options: new Options
            {
                ModifyDatabaseName = s => "Src_" + s
            });
            var dst = Sharding.GetDocumentStore(options: new Options
            {
                ModifyDatabaseName = s => "Dst_" + s
            });

            var watcher = new ExternalReplication(dst.Database, "connection");
            await AddWatcherToReplicationTopology(src, watcher, dst.Urls);

            var dstServers = await ShardingCluster.GetShardsDocumentDatabaseInstancesFor(dst, new List<RavenServer> { Server });
            while (Sharding.AllShardHaveDocs(dstServers) == false)
            {
                using (var session = src.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Oren"
                    });
                    await session.SaveChangesAsync();
                }
            }

            var shardedDb = await GetDocumentDatabaseInstanceFor(dst, dst.Database + "$0");
            var environmentOptions = (StorageEnvironmentOptions.PureMemoryStorageEnvironmentOptions)shardedDb.DocumentsStorage.Environment.Options;
            using (await Server.ServerStore.DatabasesLandlord.UnloadAndLockDatabase(shardedDb.Name, "Want to test removal of data from disk"))
            {
                IOExtensions.DeleteDirectory(environmentOptions.BasePath.ToFullPath());
            }

            var databaseTask = shardedDb.ServerStore.DatabasesLandlord.TryGetOrCreateDatabase(shardedDb.Name).DatabaseTask;
            await databaseTask;

            dstServers = await ShardingCluster.GetShardsDocumentDatabaseInstancesFor(dst, new List<RavenServer>() { Server });
            using (var session = src.OpenSession())
            {
                var sourceDocs = session.Query<User>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Count();

                var dstDocs = WaitForValue(() => Sharding.GetDocsCountForCollectionInAllShards(dstServers, "users"), sourceDocs, interval: 333);

                Assert.Equal(sourceDocs, dstDocs);
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task ReplicationShouldResumeAfterDeletingAndRestartingShardDatabase2()
        {
            var clusterSize = 2;
            var replicationFactor = 2;

            var (srcNodes, srcLeader) = await CreateRaftCluster(clusterSize);
            var (dstNodes, dstLeader) = await CreateRaftCluster(clusterSize);

            var srcDB = GetDatabaseName();
            var dstDB = GetDatabaseName();

            var srcTopology = await ShardingCluster.CreateShardedDatabaseInCluster(srcDB, replicationFactor, (srcNodes, srcLeader), shards: 2);
            var dstTopology = await ShardingCluster.CreateShardedDatabaseInCluster(dstDB, replicationFactor, (dstNodes, dstLeader), shards: 2);

            using (var srcStore = new DocumentStore()
            {
                Urls = new[] { srcLeader.WebUrl },
                Database = srcDB
            }.Initialize())
            using (var dstStore = new DocumentStore()
            {
                Urls = new[] { dstLeader.WebUrl },
                Database = dstDB
            }.Initialize())
            {
                var replicationDst = await GetReplicationManagerAsync(dstStore, dstDB, RavenDatabaseMode.Sharded, breakReplication: true, dstTopology.Servers);

                await SetupReplicationAsync(srcStore, dstStore);

                var sharding = await Sharding.GetShardingConfigurationAsync(dstStore);
                var id0 = Sharding.GetRandomIdForShard(sharding, 0);
                var id1 = Sharding.GetRandomIdForShard(sharding, 1);

                using (var session = srcStore.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), id0);
                    await session.StoreAsync(new User(), id1);
                    await session.SaveChangesAsync();
                }

                await Sharding.Replication.EnsureReplicatingAsyncForShardedDestination(srcStore, dstStore);

                var record = await dstStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dstDB));
                var shardTopology = record.Sharding.Shards[0];
                var nodeContainingShard = shardTopology.Members.First();

                var res = dstStore.Maintenance.Server.Send(new DeleteDatabasesOperation(dstDB, shardNumber: 0, hardDelete: true, fromNode: nodeContainingShard));
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(res.RaftCommandIndex);

                replicationDst.Mend();

                await WaitForValueAsync(async () =>
                {
                    using (var session = dstStore.OpenAsyncSession())
                    {
                        var u1 = await session.LoadAsync<User>(id0);
                        return u1 != null;
                    }
                }, true, 30_000, 333);

                using (var session = dstStore.OpenSession())
                {
                    var u1 = session.Load<User>(id0);
                    Assert.NotNull(u1);

                    var u2 = session.Load<User>(id1);
                    Assert.NotNull(u2);
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ReplicationWithReshardingShouldWorkFromNonShardedToSharded()
        {
            using (var store = GetDocumentStore())
            using (var replica = Sharding.GetDocumentStore())
            {
                await InsertData(store);

                var suffix = "$usa";
                var id = $"users/1{suffix}";

                await SetupReplicationAsync(store, replica);

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    user.AddressId = "New";
                    await session.SaveChangesAsync();
                }

                Assert.True(WaitForDocument<User>(replica, id, u => u.AddressId == "New"));

                await CheckData(store);

                var oldLocation = await Sharding.GetShardNumberForAsync(replica, id);

                await Sharding.Resharding.MoveShardForId(replica, id);

                await Task.Delay(3000);

                var db = await GetDocumentDatabaseInstanceFor(replica, ShardHelper.ToShardName(replica.Database, oldLocation));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = db.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Assert.Equal(24, tombs.Count);
                }

                var newLocation = await Sharding.GetShardNumberForAsync(replica, id);

                await CheckData(replica, ShardHelper.ToShardName(replica.Database, newLocation));

                await CheckData(store);
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ShouldNotReplicateTombstonesCreatedByBucketDeletionFromShardedToSharded()
        {
            using (var store = Sharding.GetDocumentStore())
            using (var replica = Sharding.GetDocumentStore())
            {
                await InsertData(store);

                var suffix = "$usa";
                var id = $"users/1{suffix}";

                await SetupReplicationAsync(store, replica);
                await SetupReplicationAsync(replica, store);

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    user.AddressId = "New";
                    await session.SaveChangesAsync();
                }

                Assert.True(WaitForDocument<User>(replica, id, u => u.AddressId == "New"));

                var oldLocation = await Sharding.GetShardNumberForAsync(store, id);
                await CheckData(store, ShardHelper.ToShardName(store.Database, oldLocation));

                await Sharding.Resharding.MoveShardForId(store, id);

                oldLocation = await Sharding.GetShardNumberForAsync(replica, id);
                await CheckData(replica, ShardHelper.ToShardName(replica.Database, oldLocation));

                await Sharding.Resharding.MoveShardForId(replica, id);

                await WaitForValueAsync(async () =>
                {
                    var newLocation = await Sharding.GetShardNumberForAsync(replica, id);
                    return newLocation != oldLocation;
                }, true);

                var newLocation = await Sharding.GetShardNumberForAsync(replica, id);
                await CheckData(replica, ShardHelper.ToShardName(replica.Database, newLocation));

                await ShardingCluster.EnsureNoReplicationLoopForSharding(Server, store.Database);
                await ShardingCluster.EnsureNoReplicationLoopForSharding(Server, replica.Database);
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ShouldNotReplicateTombstonesCreatedByBucketDeletionFromShardedToSharded2()
        {
            using (var store = Sharding.GetDocumentStore())
            using (var replica = Sharding.GetDocumentStore())
            {
                var suffix1 = "usa";
                var suffix2 = $"isr${suffix1}";

                await InsertData(store, suffix1);
                await InsertData(replica, suffix2);

                var id1 = $"users/1${suffix1}";
                var id2 = $"users/1${suffix2}";

                await SetupReplicationAsync(store, replica);
                await SetupReplicationAsync(replica, store);

                using (var session = replica.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id2);
                    user.AddressId = "New";
                    await session.SaveChangesAsync();
                }

                Assert.True(WaitForDocument<User>(store, id2, u => u.AddressId == "New"));

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id1);
                    user.AddressId = "New";
                    await session.SaveChangesAsync();
                }

                Assert.True(WaitForDocument<User>(replica, id1, u => u.AddressId == "New"));

                var oldLocation1 = await Sharding.GetShardNumberForAsync(store, id1);

                var db1 = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, oldLocation1));
                var storage1 = db1.DocumentsStorage;

                var docsCount = storage1.GetNumberOfDocuments();
                Assert.Equal(8, docsCount);

                var oldLocation2 = await Sharding.GetShardNumberForAsync(replica, id2);
                var db2 = await GetDocumentDatabaseInstanceFor(replica, ShardHelper.ToShardName(replica.Database, oldLocation2));
                var storage2 = db2.DocumentsStorage;

                docsCount = storage2.GetNumberOfDocuments();
                Assert.Equal(8, docsCount);

                await Sharding.Resharding.MoveShardForId(store, id1);
                await Sharding.Resharding.MoveShardForId(replica, id2);

                await WaitForValueAsync(async () =>
                {
                    var newLocation1 = await Sharding.GetShardNumberForAsync(replica, id1);
                    var newLocation2 = await Sharding.GetShardNumberForAsync(replica, id2);
                    return newLocation1 != oldLocation1 && newLocation2 != oldLocation2;
                }, true, 30_000, 333);

                await ShardingCluster.EnsureNoReplicationLoopForSharding(Server, store.Database);
                await ShardingCluster.EnsureNoReplicationLoopForSharding(Server, replica.Database);

                docsCount = storage1.GetNumberOfDocuments();
                Assert.Equal(0, docsCount);

                docsCount = storage2.GetNumberOfDocuments();
                Assert.Equal(0, docsCount);
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ShouldNotReplicateTombstonesCreatedByBucketDeletionFromShardedToNonSharded()
        {
            using (var store = Sharding.GetDocumentStore())
            using (var replica = GetDocumentStore())
            {
                await InsertData(store);

                var suffix = "$usa";
                var id = $"users/1{suffix}";

                await SetupReplicationAsync(store, replica);

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

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ShouldNotReplicateTombstonesCreatedByBucketDeletionFromShardedToNonSharded2()
        {
            var record = new DatabaseRecord("dummy")
            {
                Sharding = new ShardingConfiguration
                {
                    Shards = new Dictionary<int, DatabaseTopology>()
                    {
                        {0, new DatabaseTopology()},
                        {1, new DatabaseTopology()},
                        {2, new DatabaseTopology()}
                    },
                    BucketMigrations = new Dictionary<int, ShardBucketMigration>(),
                    BucketRanges = new List<ShardBucketRange> { new ShardBucketRange { ShardNumber = 2, BucketRangeStart = 0 } }
                }
            };

            using (var store = Sharding.GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = r =>
                {
                    r.Sharding = record.Sharding;
                },
            }, shards: record.Sharding.Shards))
            using (var replica = GetDocumentStore())
            {
                var suffix1 = "usa";
                var suffix2 = "isr";
                var id1 = $"users/1${suffix1}";
                var id2 = $"users/2${suffix1}";
                var id3 = $"users/3${suffix2}";
                var id4 = $"users/4${suffix2}";
                var id5 = $"users/5${suffix2}";

                var ids = new List<string> { id1, id2, id3, id4, id5 };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1", Age = 5 }, id1);
                    await session.StoreAsync(new User { Name = "Name2", LastName = "LastName2", Age = 78 }, id2);
                    await session.StoreAsync(new User { Name = "Name3", LastName = "LastName3", Age = 4 }, id3);
                    await session.StoreAsync(new User { Name = "Name4", LastName = "LastName4", Age = 15 }, id4);
                    await session.StoreAsync(new User { Name = "Name4", LastName = "LastName5", Age = 29 }, id5);
                    await session.SaveChangesAsync();
                }

                await Sharding.Resharding.MoveShardForId(store, id3);

                await SetupReplicationAsync(store, replica);

                foreach (var id in ids)
                    Assert.True(WaitForDocument(replica, id));

                var changeVector = "";
                using (var session = replica.OpenSession())
                {
                    foreach (var id in ids)
                    {
                        var user = session.Load<User>(id);
                        Assert.NotNull(user);

                        var userChangeVector = session.Advanced.GetChangeVectorFor(user);
                        Assert.True(changeVector.CompareTo(userChangeVector) < 0);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ExternalReplicationWithRevisionTombstones_NonShardedToSharded()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore())
            {
                await store1.Maintenance.ForDatabase(store1.Database).SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        PurgeOnDelete = true
                    }
                }));

                var id1 = "foo/bar/0";

                await SetupReplicationAsync(store1, store2);

                using (var s1 = store1.OpenSession())
                {
                    for (int i = 0; i < 50; i++)
                    {
                        s1.Store(new User(), $"foo/bar/{i}");
                    }

                    s1.SaveChanges();
                }

                for (int i = 0; i < 50; i++)
                {
                    var id = $"foo/bar/{i}";
                    Assert.True(WaitForDocument<User>(store2, id, predicate: null, timeout: 30_000));
                }

                var location = await Sharding.GetShardNumberForAsync(store2, id1);

                using (var s1 = store1.OpenSession())
                {
                    s1.Delete(id1);
                    s1.SaveChanges();
                }

                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                await CheckData(store2, location);
            }
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ExternalReplicationWithRevisionTombstones_NonShardedAndSharded()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore())
            {
                await store1.Maintenance.ForDatabase(store1.Database).SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        PurgeOnDelete = true
                    }
                }));

                var id1 = "foo/bar/0";

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                using (var s1 = store1.OpenSession())
                {
                    for (int i = 0; i < 50; i++)
                        s1.Store(new User(), $"foo/bar/{i}");

                    s1.SaveChanges();
                }

                for (int i = 0; i < 50; i++)
                {
                    var id = $"foo/bar/{i}";
                    Assert.True(WaitForDocument<User>(store2, id, predicate: null, timeout: 30_000));
                }

                var location = await Sharding.GetShardNumberForAsync(store2, id1);

                using (var s1 = store1.OpenSession())
                {
                    s1.Delete(id1);
                    s1.SaveChanges();
                }

                await Sharding.Replication.EnsureReplicatingAsyncForShardedDestination(store1, store2);

                await CheckData(store2, location);

                await EnsureNoReplicationLoop(Server, store1.Database);
                await ShardingCluster.EnsureNoReplicationLoopForSharding(Server, store2.Database);
            }
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ExternalReplicationWithRevisionTombstones_ShardedToSharded()
        {
            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore())
            {
                await store1.Maintenance.ForDatabase(store1.Database).SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        PurgeOnDelete = true
                    }
                }));

                var id1 = "foo/bar/0";

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                using (var s1 = store1.OpenSession())
                {
                    for (int i = 0; i < 50; i++)
                        s1.Store(new User(), $"foo/bar/{i}");

                    s1.SaveChanges();
                }

                for (int i = 0; i < 50; i++)
                {
                    var id = $"foo/bar/{i}";
                    Assert.True(WaitForDocument<User>(store2, id, predicate: null, timeout: 30_000));
                }

                var location = await Sharding.GetShardNumberForAsync(store2, id1);

                using (var s1 = store1.OpenSession())
                {
                    s1.Delete(id1);
                    s1.SaveChanges();
                }

                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                await EnsureReplicatingAsync(store2, store1);
                await EnsureReplicatingAsync(store2, store1);
                await EnsureReplicatingAsync(store2, store1);

                await CheckData(store2, location);

                await ShardingCluster.EnsureNoReplicationLoopForSharding(Server, store1.Database);
                await ShardingCluster.EnsureNoReplicationLoopForSharding(Server, store2.Database);
            }
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication | RavenTestCategory.Sharding)]
        public async Task ExternalReplicationWithRevisionTombstonesAndResharding_ShardedToSharded()
        {
            using (var store1 = Sharding.GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore())
            {
                var id1 = "users/1$usa";
                var id2 = "users/2$usa";

                await InsertData(store1);
                await InsertData(store2, "isr");

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store2, store1);

                var location = await Sharding.GetShardNumberForAsync(store2, id1);

                using (var s1 = store1.OpenSession())
                {
                    s1.Delete(id1);
                    s1.SaveChanges();
                }

                await EnsureReplicatingAsync(store1, store2);

                using (var s2 = store2.OpenSession())
                {
                    var u1 = s2.Load<User>(id1);
                    Assert.Null(u1);
                }

                var databaseRecord = store2.Maintenance.ForDatabase(store2.Database).Server.Send(new GetDatabaseRecordOperation(store2.Database));
                List<string> shardNames = ShardHelper.GetShardNames(store2.Database, databaseRecord.Sharding.Shards.Keys.AsEnumerable()).ToList();

                shardNames.Remove(ShardHelper.ToShardName(store2.Database, location));
                foreach (var name in shardNames)
                {
                    var db = await GetDocumentDatabaseInstanceFor(store2, name);
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var tombstonesCount = db.DocumentsStorage.GetNumberOfTombstones(context);
                        Assert.Equal(0, tombstonesCount);
                    }
                }

                await Sharding.Resharding.MoveShardForId(store2, id2);

                await ShardingCluster.EnsureNoReplicationLoopForSharding(Server, store1.Database);
                await ShardingCluster.EnsureNoReplicationLoopForSharding(Server, store2.Database);
            }
        }

        // RavenDB-20369
        [Fact]
        public async Task ShouldNotDelayReplicationForDifferentMissingAttachments()
        {
            using (var source = GetDocumentStore())
            using (var destination = Sharding.GetDocumentStore())
            {
                var id = "FoObAr/1";
                await SetupReplicationAsync(source, destination);

                using (var session = source.OpenAsyncSession())
                using (var fooStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    await session.StoreAsync(new User { Name = "Foo" }, id);
                    session.Advanced.Attachments.Store(id, "foo.png", fooStream, "image/png");
                    await session.SaveChangesAsync();
                }

                WaitForDocumentWithAttachmentToReplicate<User>(destination, id, "foo.png", 10_000);

                var shardedDb = Sharding.GetOrchestrator(destination.Database);

                // trigger first MissingAttachmentException

                using (var session = destination.OpenAsyncSession())
                {
                    session.Delete(id);
                    await session.SaveChangesAsync();
                }

                using (var session = source.OpenAsyncSession())
                using (var fooStream2 = new MemoryStream(new byte[] { 4, 5, 6 }))
                {
                    session.Advanced.Attachments.Store(id, "foo2.png", fooStream2, "image/png");
                    await session.SaveChangesAsync();
                }

                WaitForDocumentWithAttachmentToReplicate<User>(destination, id, "foo2.png", 10_000);

                // trigger second MissingAttachmentException
                // at this point, the destination recovered from the first MissingAttachmentException 
                // 'MissingAttachmentsRetries' should be 0

                using (var session = destination.OpenAsyncSession())
                {
                    session.Delete(id);
                    await session.SaveChangesAsync();
                }

                using (var session = source.OpenAsyncSession())
                using (var fooStream3 = new MemoryStream(new byte[] { 7, 8, 9 }))
                {
                    session.Advanced.Attachments.Store(id, "foo3.png", fooStream3, "image/png");
                    await session.SaveChangesAsync();
                }

                WaitForDocumentWithAttachmentToReplicate<User>(destination, id, "foo3.png", 10_000);

                Assert.False(shardedDb.NotificationCenter.Exists("AlertRaised/Replication"));
            }
        }

        public class GetReplicationActiveConnectionsInfoOperation : IMaintenanceOperation<ReplicationActiveConnectionsPreview>
        {
            public RavenCommand<ReplicationActiveConnectionsPreview> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new GetReplicationActiveConnectionsInfoCommand();
            }
        }

        private static async Task InsertData(IDocumentStore store, string suffix = "usa", bool purgeOnDelete = false)
        {
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
                        Disabled = false,
                        PurgeOnDelete = purgeOnDelete
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

        private async Task CheckData(IDocumentStore store, string database = null, long expectedRevisionsCount = 12, long expectedTombstoneCount = 0)
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
                Assert.Equal(expectedTombstoneCount, tombstonesCount);

                //revisions
                var revisionsCount = storage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
                Assert.Equal(expectedRevisionsCount, revisionsCount);
            }

            //docs
            Assert.Equal(4, docsCount);

            var suffix = "usa";
            using (var session = store.OpenSession(database))
            {
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

        private async Task CheckData(DocumentStore store, int location)
        {
            var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, location));
            var storage = db.DocumentsStorage;
            using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var tombs = storage.GetTombstonesFrom(context, 0).ToList();
                Assert.Equal(2, tombs.Count);

                int revisionTombsCount = 0, documentTombsCount = 0;
                foreach (var item in tombs)
                {
                    if (item is RevisionTombstoneReplicationItem)
                        revisionTombsCount++;
                    else if (item is DocumentReplicationItem)
                        documentTombsCount++;
                }

                Assert.Equal(1, revisionTombsCount);
                Assert.Equal(1, documentTombsCount);
            }
        }
    }
}

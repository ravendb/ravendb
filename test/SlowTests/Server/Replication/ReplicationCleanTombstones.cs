using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Session;
using Raven.Client.Json.Converters;
using Raven.Server;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class ReplicationCleanTombstones : ReplicationTestBase
    {
        public ReplicationCleanTombstones(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CleanTombstones()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var storage1 = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database).Result;

                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                Assert.True(WaitForDocument(store2, "foo/bar"));

                using (var session = store1.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                await storage1.TombstoneCleaner.ExecuteCleanup();

                Assert.Equal(1, WaitUntilHasTombstones(store2).Count);
                //Assert.Equal(4, WaitForValue(() => storage1.ReplicationLoader.MinimalEtagForReplication, 4));

                EnsureReplicating(store1, store2);

                await storage1.TombstoneCleaner.ExecuteCleanup();

                Assert.Equal(0, WaitForValue(() => WaitUntilHasTombstones(store1, 0).Count, 0));
            }
        }

        [Fact]
        public async Task CleanTombstonesInTheClusterWithBackup()
        {
            var cluster = await CreateRaftCluster(3);
            var database = GetDatabaseName();
            await CreateDatabaseInCluster(database, 3, cluster.Leader.WebUrl);

            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore(new Options
            {
                CreateDatabase = false,
                Server = cluster.Leader,
                ModifyDatabaseName = _ => database
            }))
            {
                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "incremental",
                    IncrementalBackupFrequency = "0 0 1 1 *",
                    FullBackupFrequency = "0 0 1 1 *",
                    BackupType = BackupType.Backup
                };

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Store(new User { Name = "Karmel" }, "marker");
                    session.SaveChanges();

                    await WaitForDocumentInClusterAsync<User>((DocumentSession)session, store.Database, (u) => u.Id == "marker", TimeSpan.FromSeconds(15));
                }

                var total = 0L;
                foreach (var server in cluster.Nodes)
                {
                    var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    await storage.TombstoneCleaner.ExecuteCleanup();
                    using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        total += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    }
                }

                Assert.Equal(3, total);

                await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: true, result.TaskId));

                Assert.True(await WaitForValueAsync(() =>
                {
                    var operation = new GetPeriodicBackupStatusOperation(result.TaskId);
                    var getPeriodicBackupResult = store.Maintenance.Send(operation);
                    return getPeriodicBackupResult.Status?.LastEtag > 0;
                }, true));

                await WaitForRaftCommandToBeAppliedInCluster(cluster.Leader, nameof(UpdatePeriodicBackupStatusCommand));

                total = 0L;
                foreach (var server in cluster.Nodes)
                {
                    var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    await storage.TombstoneCleaner.ExecuteCleanup();
                    using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        total += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    }
                }

                Assert.Equal(0, total);
            }
        }

        [Fact]
        public async Task CleanTombstonesInTheClusterWithOnlyFullBackup()
        {
            var cluster = await CreateRaftCluster(3);
            var database = GetDatabaseName();
            await CreateDatabaseInCluster(database, 3, cluster.Leader.WebUrl);

            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore(new Options
            {
                CreateDatabase = false,
                Server = cluster.Leader,
                ModifyDatabaseName = _ => database
            }))
            {
                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "full",
                    FullBackupFrequency = "0 0 1 1 *",
                    BackupType = BackupType.Backup
                };

                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Store(new User { Name = "Karmel" }, "marker");
                    session.SaveChanges();

                    await WaitForDocumentInClusterAsync<User>((DocumentSession)session, store.Database, (u) => u.Id == "marker", TimeSpan.FromSeconds(15));
                }

                var total = 0L;
                foreach (var server in cluster.Nodes)
                {
                    var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    await storage.TombstoneCleaner.ExecuteCleanup();
                    using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        total += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    }
                }

                Assert.Equal(0, total);
            }
        }

        [Fact]
        public async Task CleanTombstonesInTheClusterWithExternalReplication()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            var database = GetDatabaseName();
            await CreateDatabaseInCluster(database, 3, cluster.Leader.WebUrl);
            var external = GetDatabaseName();
            using (var store = GetDocumentStore(new Options
            {
                CreateDatabase = false,
                Server = cluster.Leader,
                ModifyDatabaseName = _ => database
            }))
            {
                var replication = new ExternalReplication(external, "Connection");

                await AddWatcherToReplicationTopology(store, replication);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Store(new User { Name = "Karmel" }, "marker");
                    session.SaveChanges();

                    await WaitForDocumentInClusterAsync<User>((DocumentSession)session, store.Database, (u) => u.Id == "marker", TimeSpan.FromSeconds(15));
                }

                var total = 0L;
                foreach (var server in cluster.Nodes)
                {
                    var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    await storage.TombstoneCleaner.ExecuteCleanup();
                    using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        total += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    }
                }

                Assert.Equal(3, total);

                await CreateDatabaseInCluster(external, 3, cluster.Leader.WebUrl);
                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Store(new User(), "marker2");
                    session.SaveChanges();
                }

                using (var externalSession = store.OpenSession(external))
                {
                    Assert.True(await WaitForDocumentInClusterAsync<User>((DocumentSession)externalSession, "marker2", (m) => m.Id == "marker2", TimeSpan.FromSeconds(15)));
                }

                await WaitForRaftCommandToBeAppliedInCluster(cluster.Leader, nameof(UpdateExternalReplicationStateCommand));

                foreach (var server in cluster.Nodes)
                {
                    var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    await storage.TombstoneCleaner.ExecuteCleanup();
                    using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        Assert.True(storage.DocumentsStorage.GetNumberOfTombstones(context) == 0);
                    }
                }
            }
        }

        [Fact]
        public async Task EtlTombstonesInTheCluster()
        {
            var cluster = await CreateRaftCluster(3);

            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions(),
                ReplicationFactor = 3
            }))
            using (var dest = GetDocumentStore())
            {
                var connectionString = await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = "test",
                    TopologyDiscoveryUrls = dest.Urls,
                    Database = dest.Database,
                }));
                Assert.NotNull(connectionString.RaftCommandIndex);

                var configuration = new RavenEtlConfiguration()
                {
                    ConnectionStringName = "test",
                    Name = "myConfiguration",
                    MentorNode = "A",
                    Transforms = new List<Transformation>
                    {
                        new Transformation
                        {
                            ApplyToAllDocuments = true,
                            Name = "blah"
                        }
                    }
                };

                await store.Maintenance.SendAsync(new AddEtlOperation<RavenConnectionString>(configuration));

                var etlStorage = await cluster.Nodes.Single(n => n.ServerStore.NodeTag == "A").ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                var mre = new ManualResetEventSlim(false);
                var sent = new ManualResetEventSlim(false);
                etlStorage.EtlLoader.BatchCompleted += _ =>
                {
                    sent.Set();
                    mre.Wait(TimeSpan.FromSeconds(30));
                };

                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Store(new User { Name = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }

                if (sent.Wait(TimeSpan.FromSeconds(30)) == false)
                    Assert.False(true, "timeout!");

                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Store(new User { Name = "Karmel" }, "marker");
                    session.SaveChanges();

                    Assert.True(await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "marker", (u) => u.Id == "marker", TimeSpan.FromSeconds(15)));
                }

                var total = 0L;
                foreach (var server in cluster.Nodes)
                {
                    var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        total += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    }
                }
                Assert.Equal(3, total);

                total = 0L;
                foreach (var server in cluster.Nodes)
                {
                    var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    await storage.TombstoneCleaner.ExecuteCleanup();
                    using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        total += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    }
                }
                Assert.Equal(3, total); // we didn't send the tombstone so we must not purge it

                await WaitForLastReplicationEtag(cluster, store);

                await DisposeServerAndWaitForFinishOfDisposalAsync(etlStorage.ServerStore.Server);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "marker2");
                    session.SaveChanges();
                }

                WaitForDocument(dest, "marker2");
                string changeVectorMarker2;

                using (var session = dest.OpenSession())
                {
                    Assert.Null(session.Load<User>("foo/bar"));
                }
                using (var session = store.OpenSession())
                {
                    var marker = session.Load<User>("marker2");
                    changeVectorMarker2 = session.Advanced.GetChangeVectorFor(marker);
                }

                await ActionWithLeader((l) => WaitForRaftCommandToBeAppliedInCluster(l, nameof(UpdateEtlProcessStateCommand)));
                Assert.True(await WaitForEtlState(cluster, store, changeVectorMarker2));

                total = 0;
                foreach (var server in cluster.Nodes)
                {
                    if (server.Disposed)
                        continue;
                    var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    await storage.TombstoneCleaner.ExecuteCleanup();
                    using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        total += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    }

                }
                Assert.Equal(0, total);
            }
        }

        private static async Task WaitForLastReplicationEtag((List<RavenServer> Nodes, RavenServer Leader) cluster, DocumentStore store)
        {
            foreach (var server in cluster.Nodes)
            {
                if (server.Disposed)
                    continue;
                var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                var sw = Stopwatch.StartNew();
                while (sw.ElapsedMilliseconds < 5000)
                {
                    if (storage.ReplicationLoader.GetMinimalEtagForReplication() > 1)
                        break;
                    await Task.Delay(TimeSpan.FromMilliseconds(50));
                }

                Assert.True(storage.ReplicationLoader.GetMinimalEtagForReplication() > 1);
            }
        }

        private static async Task<bool> WaitForEtlState((List<RavenServer> Nodes, RavenServer Leader) cluster, DocumentStore store, string changeVectorMarker2)
        {
            EtlProcess etlP = null;
            foreach (var server in cluster.Nodes)
            {
                if (( server.Disposed ) || (server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).Result.EtlLoader.Processes.Length == 0))
                    continue;
                etlP = server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database).Result.EtlLoader.Processes[0];
            }

            var total = 0;
            foreach (var server in cluster.Nodes.Where(server => (server.Disposed  == false)))
            {
                var sw = Stopwatch.StartNew();
                while (sw.ElapsedMilliseconds < 10000)
                {
                    using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context2))
                    using (context2.OpenReadTransaction())
                    {
                        var stateBlittable = server.ServerStore.Cluster.Read(context2,
                            EtlProcessState.GenerateItemName(store.Database, etlP.ConfigurationName, etlP.TransformationName));
                        if (stateBlittable != null)
                        {
                            if (JsonDeserializationClient.EtlProcessState(stateBlittable).ChangeVector == changeVectorMarker2)
                            {
                                total++;
                                break;
                            }
                        }
                    }

                    await Task.Delay(100);
                }
            }
            return total == 2;
        }

        [Fact]
        public async Task CanReplicateTombstonesFromDifferentCollections()
        {
            var id = "Oren\r\nEini";

            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var storage1 = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database).Result;
                var storage2 = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store2.Database).Result;

                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, id);
                    session.SaveChanges();
                }

                var results = await SetupReplicationAsync(store1, store2);
                Assert.True(WaitForDocument(store2, id));

                using (var session = store1.OpenSession())
                {
                    session.Delete(id);
                    session.SaveChanges();
                }
                EnsureReplicating(store1, store2);

                using (var session = store1.OpenSession())
                {
                    session.Store(new Company { Name = "Karmel" }, id);
                    session.SaveChanges();
                }
                Assert.True(WaitForDocument(store2, id));

                using (var session = store1.OpenSession())
                {
                    session.Delete(id);
                    session.SaveChanges();
                }
                EnsureReplicating(store1, store2);

                using (storage1.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    Assert.Equal(2, storage1.DocumentsStorage.GetNumberOfTombstones(ctx));
                }

                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    Assert.Equal(2, storage2.DocumentsStorage.GetNumberOfTombstones(ctx));
                }
                
                var val = await WaitForValueAsync(() =>
                {
                    var state = ReplicationLoader.GetExternalReplicationState(Server.ServerStore, store1.Database, results[0].TaskId);
                    return state.LastSentEtag;
                }, 7);

                Assert.Equal(7, val);

                await storage1.TombstoneCleaner.ExecuteCleanup();
                await storage2.TombstoneCleaner.ExecuteCleanup();

                using (storage1.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    Assert.Equal(0, storage1.DocumentsStorage.GetNumberOfTombstones(ctx));
                }

                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    Assert.Equal(0, storage2.DocumentsStorage.GetNumberOfTombstones(ctx));
                }
            }
        }

        [Fact]
        public async Task CanReplicateTombstonesFromDifferentCollections2()
        {
            var id = "my-great-id";
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var storage1 = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database);
                var storage2 = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store2.Database);

                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, id);
                    session.SaveChanges();
                }
                using (var session = store1.OpenSession())
                {
                    session.Delete(id);
                    session.SaveChanges();
                }
                using (var session = store1.OpenSession())
                {
                    session.Store(new Company { Name = "Karmel" }, id);
                    session.SaveChanges();
                }
                using (var session = store1.OpenSession())
                {
                    session.Delete(id);
                    session.SaveChanges();
                }
                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1,store2);

                using (storage1.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    Assert.Equal(2, storage1.DocumentsStorage.GetNumberOfTombstones(ctx));
                }

                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    Assert.Equal(2, storage2.DocumentsStorage.GetNumberOfTombstones(ctx));
                }

                await storage1.TombstoneCleaner.ExecuteCleanup();
                await storage2.TombstoneCleaner.ExecuteCleanup();

                using (storage1.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    Assert.Equal(0, storage1.DocumentsStorage.GetNumberOfTombstones(ctx));
                }

                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    Assert.Equal(0, storage2.DocumentsStorage.GetNumberOfTombstones(ctx));
                }
            }
        }

        [Fact]
        public async Task CanDeleteFromDifferentCollections()
        {
            using (var store = GetDocumentStore())
            {
                var storage = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenSession())
                using (var ms = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    session.Store(new User { Name = "Karmel" }, "foo/bar");
                    session.Advanced.Attachments.Store("foo/bar", "dummy", ms);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                using (var ms = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    session.Store(new User { Name = "Karmel" }, "foo/bar");
                    session.Advanced.Attachments.Store("foo/bar", "dummy", ms);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new Employee { FirstName = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                await storage.TombstoneCleaner.ExecuteCleanup();
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    Assert.Equal(0, storage.DocumentsStorage.GetNumberOfTombstones(ctx));
                }
            }
        }

        [Fact]
        public async Task CanDeleteFromDifferentCollections2()
        {
            using (var store = GetDocumentStore())
            {
                var storage = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }

                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    Assert.Equal(2, storage.DocumentsStorage.GetNumberOfTombstones(ctx));
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }

                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    Assert.Equal(1, storage.DocumentsStorage.GetNumberOfTombstones(ctx));
                }
            }
        }

        [Fact]
        public async Task CanBackupAndRestoreTombstonesWithSameId()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var id = "oren\r\nEini";
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Karmel" }, "marker");
                    session.SaveChanges();
                }
                var config = new PeriodicBackupConfiguration
                {
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    },
                    Name = "incremental",
                    IncrementalBackupFrequency = "* * */6 * *",
                    FullBackupFrequency = "* */6 * * *",
                    BackupType = BackupType.Backup
                };

                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: false, result.TaskId));
                Assert.Equal(1, await WaitForValueAsync(() =>
                 {
                     var operation = new GetPeriodicBackupStatusOperation(result.TaskId);
                     var getPeriodicBackupResult = store.Maintenance.Send(operation);
                     return getPeriodicBackupResult.Status?.LastEtag;
                 }, 1));

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, id);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Delete(id);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Karmel" }, id);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Delete(id);
                    session.SaveChanges();
                }

                await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: false, result.TaskId));
                Assert.Equal(5, await WaitForValueAsync(() =>
                 {
                     var operation = new GetPeriodicBackupStatusOperation(result.TaskId);
                     var getPeriodicBackupResult = store.Maintenance.Send(operation);
                     Assert.Null(getPeriodicBackupResult.Status.Error);
                     return getPeriodicBackupResult.Status?.LastEtag;
                 }, 5));

                var databaseName = GetDatabaseName();

                using (RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = Directory.GetDirectories(backupPath).First(),
                    DatabaseName = databaseName
                }))
                {
                    var stats = store.Maintenance.ForDatabase(databaseName).Send(new GetStatisticsOperation());
                    Assert.Equal(1, stats.CountOfDocuments); // the marker
                    Assert.Equal(2, stats.CountOfTombstones);

                    var storage = await GetDocumentDatabaseInstanceFor(store);
                    using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        foreach (var tombstone in storage.DocumentsStorage.GetTombstonesFrom(ctx, 0))
                        {
                            Assert.Equal("oren\r\neini", tombstone.Id);
                        }
                    }
                }
            }
        }
    }
}

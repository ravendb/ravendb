using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Session;
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
            var cluster = await CreateRaftCluster(3);
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

                WaitForDocument(store, "marker", database: external);
                WaitForDocumentDeletion(store, "foo/bar", database: external);

                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Store(new User (), "marker2");
                    session.SaveChanges();
                }

                WaitForDocument(store, "marker2", database: external);

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
        public async Task EtlTombstonesInTheCluster()
        {
            var cluster = await CreateRaftCluster(3);

            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions() ,
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
                    mre.Wait();
                };

                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Store(new User { Name = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }

                if (sent.Wait(TimeSpan.FromSeconds(15)) == false)
                    Assert.False(true,"timeout!");

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
                Assert.Equal(3, total); // we didn't send the tombstone so we must not purge it

                await DisposeServerAndWaitForFinishOfDisposalAsync(etlStorage.ServerStore.Server);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "marker2");
                    session.SaveChanges();
                }

                WaitForDocument(dest, "marker2");

                using (var session = dest.OpenSession())
                {
                    Assert.Null(session.Load<User>("foo/bar"));
                }

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

        [Fact(Skip = "RavenDB-14325")]
        public async Task CanReplicateTombstonesFromDifferentCollections()
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
                EnsureReplicating(store1, store2);

                using (var session = store1.OpenSession())
                {
                    session.Store(new Company { Name = "Karmel" }, "foo/bar");
                    session.SaveChanges();
                }
                Assert.True(WaitForDocument(store2, "foo/bar"));

                await storage1.TombstoneCleaner.ExecuteCleanup();

                using (var session = store1.OpenSession())
                {
                    session.Delete("foo/bar");
                    session.SaveChanges();
                }
                EnsureReplicating(store1, store2);
            }
        }

    }
}

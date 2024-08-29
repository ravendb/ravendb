using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class ReplicationCleanTombstones : ReplicationTestBase
    {
        public ReplicationCleanTombstones(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformTheory(RavenTestCategory.Replication, RavenArchitecture.X64)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CleanTombstones(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                var storage1 = await GetDocumentDatabaseInstanceForAsync(store1, options.DatabaseMode, "foo/bar");

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

                await EnsureReplicatingAsync(store1, store2);

                await storage1.TombstoneCleaner.ExecuteCleanup();

                Assert.Equal(0, WaitForValue(() => WaitUntilHasTombstones(store1, 0).Count, 0));
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Cluster | RavenTestCategory.BackupExportImport)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CleanTombstonesInTheClusterWithBackup(Options options)
        {
            var cluster = await CreateRaftCluster(3);
            var database = GetDatabaseName();

            var modifyDatabaseRecord = options.ModifyDatabaseRecord;
            var record = new DatabaseRecord(database);
            modifyDatabaseRecord?.Invoke(record);

            await CreateDatabaseInCluster(record, 3, cluster.Leader.WebUrl);

            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore(new Options(options)
            {
                CreateDatabase = false,
                Server = cluster.Leader,
                ModifyDatabaseName = _ => database,
                // Backup.RunBackupInClusterAsync uses node tag to wait for backup occurrence
                ModifyDocumentStore = s => s.Conventions.DisableTopologyUpdates = false
            }))
            {
                var databaseName = options.DatabaseMode == RavenDatabaseMode.Single ? database : await Sharding.GetShardDatabaseNameForDocAsync(store, "foo/bar");
                var config = Backup.CreateBackupConfiguration(backupPath, incrementalBackupFrequency: "0 0 1 1 *");
                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                if (options.DatabaseMode == RavenDatabaseMode.Single)
                {
                    Backup.WaitForResponsibleNodeUpdateInCluster(store, cluster.Nodes, result.TaskId);
                }
                else
                {
                    Sharding.Backup.WaitForResponsibleNodeUpdateInCluster(store, cluster.Nodes, result.TaskId);
                }

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
                    session.Store(new User { Name = "Karmel" }, "marker$foo/bar");
                    session.SaveChanges();

                    Assert.True(await WaitForDocumentInClusterAsync<User>(cluster.Nodes, databaseName, "marker$foo/bar", (u) => u.Id == "marker$foo/bar", TimeSpan.FromSeconds(15)));
                }

                // wait for CV to merge after replication
                Assert.True(await WaitForChangeVectorInClusterAsync(cluster.Nodes, databaseName));

                var total = 0L;
                foreach (var server in cluster.Nodes)
                {
                    var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                    await storage.TombstoneCleaner.ExecuteCleanup();
                    using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        total += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    }
                }

                Assert.Equal(3, total);

                await Backup.RunBackupInClusterAsync(store, result.TaskId, isFullBackup: true);
                await ActionWithLeader(async x => await Cluster.WaitForRaftCommandToBeAppliedInClusterAsync(x, nameof(UpdatePeriodicBackupStatusCommand)), cluster.Nodes);

                var res = await WaitForValueAsync(async () =>
                {
                    var c = 0L;
                    foreach (var server in cluster.Nodes)
                    {
                        var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                        await storage.TombstoneCleaner.ExecuteCleanup();
                        using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            c += storage.DocumentsStorage.GetNumberOfTombstones(context);
                        }
                    }
                    return c;
                }, 0, interval: 333);
                Assert.Equal(0, res);
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Cluster | RavenTestCategory.BackupExportImport)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CleanTombstonesInTheClusterWithOnlyFullBackup(Options options)
        {
            var cluster = await CreateRaftCluster(3);
            var database = GetDatabaseName();

            var modifyDatabaseRecord = options.ModifyDatabaseRecord;
            var record = new DatabaseRecord(database);
            modifyDatabaseRecord?.Invoke(record);

            await CreateDatabaseInCluster(record, 3, cluster.Leader.WebUrl);

            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore(new Options
            {
                CreateDatabase = false,
                Server = cluster.Leader,
                ModifyDatabaseName = _ => database
            }))
            {
                var databaseName = options.DatabaseMode == RavenDatabaseMode.Single ? database : await Sharding.GetShardDatabaseNameForDocAsync(store, "foo/bar");
                var config = Backup.CreateBackupConfiguration(backupPath);
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
                    session.Store(new User { Name = "Karmel" }, "marker$foo/bar");
                    session.SaveChanges();

                    Assert.True(await WaitForDocumentInClusterAsync<User>(cluster.Nodes, databaseName, "marker$foo/bar", (u) => u.Id == "marker$foo/bar", TimeSpan.FromSeconds(15)));
                }

                // wait for CV to merge after replication
                Assert.True(await WaitForChangeVectorInClusterAsync(cluster.Nodes, databaseName));

                var res = await WaitForValueAsync(async () =>
                {
                    var c = 0L;
                    foreach (var server in cluster.Nodes)
                    {
                        var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                        await storage.TombstoneCleaner.ExecuteCleanup();
                        using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            c += storage.DocumentsStorage.GetNumberOfTombstones(context);
                        }
                    }
                    return c;
                }, 0, interval: 333);
                Assert.Equal(0, res);
            }
        }

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Cluster)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CleanTombstonesInTheClusterWithExternalReplication(Options options)
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            var database = GetDatabaseName();

            var modifyDatabaseRecord = options.ModifyDatabaseRecord;
            var record = new DatabaseRecord(database);
            modifyDatabaseRecord?.Invoke(record);

            await CreateDatabaseInCluster(record, 3, cluster.Leader.WebUrl);

            var external = GetDatabaseName();
            using (var store = GetDocumentStore(new Options(options)
            {
                CreateDatabase = false,
                Server = cluster.Leader,
                ModifyDatabaseName = _ => database
            }))
            {
                var databaseName = options.DatabaseMode == RavenDatabaseMode.Single ? database : await Sharding.GetShardDatabaseNameForDocAsync(store, "foo/bar");
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

                    await WaitForDocumentInClusterAsync<User>(cluster.Nodes, databaseName, "marker", (u) => u.Id == "marker", TimeSpan.FromSeconds(15));
                }

                // wait for CV to merge after replication
                Assert.True(await WaitForChangeVectorInClusterAsync(cluster.Nodes, databaseName));

                var total = 0L;
                foreach (var server in cluster.Nodes)
                {
                    var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                    await storage.TombstoneCleaner.ExecuteCleanup();
                    using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        total += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    }
                }

                Assert.Equal(3, total);

                var record2 = new DatabaseRecord(external);
                modifyDatabaseRecord(record2);

                await CreateDatabaseInCluster(record2, 3, cluster.Leader.WebUrl);
                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Store(new User(), "marker2$foo/bar");
                    session.SaveChanges();
                }

                using (var externalSession = store.OpenSession(external))
                {
                    Assert.True(await WaitForDocumentInClusterAsync<User>(cluster.Nodes, external, "marker2$foo/bar", (m) => m.Id == "marker2$foo/bar", TimeSpan.FromSeconds(15)));
                }

                await ActionWithLeader(async x => await Cluster.WaitForRaftCommandToBeAppliedInClusterAsync(x, nameof(UpdateExternalReplicationStateCommand)), cluster.Nodes);
                var res = await WaitForValueAsync(async () =>
                {
                    var c = 0L;
                    foreach (var server in cluster.Nodes)
                    {
                        var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                        await storage.TombstoneCleaner.ExecuteCleanup();
                        using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            c += storage.DocumentsStorage.GetNumberOfTombstones(context);
                        }
                    }
                    return c;
                }, 0, interval: 333);
                Assert.Equal(0, res);
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Cluster | RavenTestCategory.Etl)]
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

                    Assert.True(await WaitForDocumentInClusterAsync<User>(cluster.Nodes, store.Database, "foo/bar", (u) => u.Name == "Karmel", TimeSpan.FromSeconds(15)));
                }

                if (sent.Wait(TimeSpan.FromSeconds(30)) == false)
                    Assert.Fail("timeout!");

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

                    Assert.True(await WaitForDocumentInClusterAsync<User>(cluster.Nodes, store.Database, "marker", (u) => u.Id == "marker", TimeSpan.FromSeconds(15)));
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

                var res = WaitForDocument(dest, "marker2");
                Assert.True(res);
                Assert.True(await WaitForChangeVectorInClusterAsync(cluster.Nodes.Where(s => (s.Disposed == false)).ToList(), store.Database));

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

                await ActionWithLeader((l) => Cluster.WaitForRaftCommandToBeAppliedInClusterAsync(l, nameof(UpdateEtlProcessStateCommand)));
                Assert.True(await WaitForEtlState(cluster, store, changeVectorMarker2));

                foreach (var server in cluster.Nodes)
                {
                    if (server.Disposed)
                        continue;

                    var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    long cleanerRes = 0;
                    using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        var val = await WaitForValueAsync(async () =>
                        {
                            cleanerRes = await storage.TombstoneCleaner.ExecuteCleanup();
                            using (context.OpenReadTransaction())
                            {
                                return storage.DocumentsStorage.GetNumberOfTombstones(context);
                            }
                        }, 0);
                        Assert.True(0 == val, $"TombstoneCleaner result = {cleanerRes}, actual number of existing tombstones = {val}" +
                                              $"{Environment.NewLine}current server: {server.ServerStore.NodeTag}");
                    }
                }
            }
        }

        private static async Task WaitForLastReplicationEtag((List<RavenServer> Nodes, RavenServer Leader) cluster, DocumentStore store, string databaseName = null)
        {
            foreach (var server in cluster.Nodes)
            {
                if (server.Disposed)
                    continue;
                var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName ?? store.Database);

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
                if (server.Disposed)
                    continue;

                var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                if (db.EtlLoader.Processes.Length == 0)
                    continue;

                etlP = db.EtlLoader.Processes[0];
            }

            var total = 0;
            foreach (var server in cluster.Nodes.Where(server => (server.Disposed == false)))
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

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanReplicateTombstonesFromDifferentCollections(Options options)
        {
            var id = "Oren\r\nEini";

            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                var storage1 = await GetDocumentDatabaseInstanceForAsync(store1, options.DatabaseMode, id);
                var storage2 = await GetDocumentDatabaseInstanceForAsync(store2, options.DatabaseMode, id);

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
                await EnsureReplicatingAsync(store1, store2);

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
                await EnsureReplicatingAsync(store1, store2);

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
                    var state = ReplicationLoader.GetExternalReplicationState(Server.ServerStore, storage1.Name, results[0].TaskId);
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

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanReplicateTombstonesFromDifferentCollections2(Options options)
        {
            var id = "my-great-id";
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                var storage1 = await GetDocumentDatabaseInstanceForAsync(store1, options.DatabaseMode, id);
                var storage2 = await GetDocumentDatabaseInstanceForAsync(store2, options.DatabaseMode, id);

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
                await EnsureReplicatingAsync(store1, store2);

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

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanDeleteFromDifferentCollections(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var storage = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "foo/bar");
                await RevisionsHelper.SetupRevisionsAsync(store);

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

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanDeleteFromDifferentCollections2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var storage = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "foo/bar");

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

        [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.BackupExportImport)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanBackupAndRestoreTombstonesWithSameId(Options options)
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var id = "oren\r\nEini";

            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Karmel" }, "marker");
                    session.SaveChanges();
                }

                var backupTaskId = await Backup.CreateAndRunBackupAsync(store, options.DatabaseMode, backupPath);

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

                var configuration = await Backup.RunBackupAndCreateRestoreConfigurationAsync(store, options.DatabaseMode, backupTaskId, backupPath);

                using (Backup.RestoreDatabase(store, configuration))
                {
                    var stats = await GetDatabaseStatisticsAsync(store, configuration.DatabaseName);
                    Assert.Equal(1, stats.CountOfDocuments); // the marker
                    Assert.Equal(2, stats.CountOfTombstones);

                    var dbName = options.DatabaseMode == RavenDatabaseMode.Single
                        ? configuration.DatabaseName
                        : await Sharding.GetShardDatabaseNameForDocAsync(store, id, configuration.DatabaseName);
                    var storage = (await GetDocumentDatabaseInstanceForAsync(dbName)).DocumentsStorage;

                    using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        foreach (var tombstone in storage.GetTombstonesFrom(ctx, 0))
                        {
                            var documentTombstone = (DocumentReplicationItem)tombstone;
                            Assert.Equal("oren\r\neini", documentTombstone.Id);
                        }
                    }
                }
            }
        }
    }
}

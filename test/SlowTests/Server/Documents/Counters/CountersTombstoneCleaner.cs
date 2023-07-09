using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Counters
{
    public class CountersTombstoneCleaner : ReplicationTestBase
    {
        public CountersTombstoneCleaner(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Counters | RavenTestCategory.Indexes)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task IndexCleanCounterTombstones(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.CountersFor(company).Increment("HeartRate", 7);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.CountersFor(company).Delete("HeartRate");

                    session.SaveChanges();
                }

                var storage = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "companies/1");

                await store.Maintenance.ForDatabase(storage.Name).SendAsync(new StopIndexingOperation());

                var countersIndex = new MyCounterIndex();
                await store.ExecuteIndexAsync(countersIndex);

                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                    Assert.Equal(1, c2);
                }

                var cleaner = storage.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                var c = 0L;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    c += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    c += storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }
                Assert.True(c > 0);

                await store.Maintenance.ForDatabase(storage.Name).SendAsync(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                await cleaner.ExecuteCleanup();

                c = 0;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    c += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    c += storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }

                Assert.Equal(0, c);
            }
        }

        [RavenTheory(RavenTestCategory.Counters | RavenTestCategory.Indexes)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task IndexCleanCounterTombstones2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var suffix = "foo/bar";

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var user = new User();
                        session.Store(user, $"users/{i}${suffix}");
                        session.CountersFor(user).Increment("HeartRate", 180 + i);
                    }

                    session.SaveChanges();
                }

                var storage = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, suffix);

                await store.Maintenance.ForDatabase(storage.Name).SendAsync(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var user = new User();
                        session.Store(user, $"users/{i}${suffix}");

                        session.CountersFor(user).Delete("HeartRate");
                    }

                    session.SaveChanges();
                }

                var countersIndex = new AverageHeartRate();
                await countersIndex.ExecuteAsync(store);

                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                    Assert.Equal(10, c2);
                }

                var cleaner = storage.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                var c = 0L;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    c += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    c += storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }
                Assert.True(c > 0);

                await store.Maintenance.ForDatabase(storage.Name).SendAsync(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                await cleaner.ExecuteCleanup();

                c = 0;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {

                    c += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    c += storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }

                Assert.Equal(0, c);
            }
        }

        [RavenTheory(RavenTestCategory.Counters | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ReplicationCleanCounterTombstones(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "EGR" }, "user/322");
                    session.SaveChanges();
                }

                using (var session = store1.OpenSession())
                {
                    var cf = session.CountersFor("user/322");
                    cf.Increment("Likes", 100);
                    cf.Increment("Likes2", 200);
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                using (var session = store1.OpenSession())
                {
                    var cf = session.CountersFor("user/322");
                    cf.Delete("Likes");
                    session.SaveChanges();
                }

                await EnsureReplicatingAsync(store1, store2);

                var storage = await GetDocumentDatabaseInstanceForAsync(store1, options.DatabaseMode, "user/322");
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                    Assert.Equal(1, c2);
                }

                await EnsureReplicatingAsync(store1, store2);

                var cleaner = storage.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                var c = 0L;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    c += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    c += storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }
                Assert.Equal(0, c);

                long count1 = 0;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    count1 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }

                var storage2 = await GetDocumentDatabaseInstanceForAsync(store2, options.DatabaseMode, "user/322");
                cleaner = storage2.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                long count2 = 0;
                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    count2 = storage2.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }
                Assert.Equal(count1, count2);
            }
        }

        [RavenTheory(RavenTestCategory.Counters | RavenTestCategory.BackupExportImport)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task IncrementalBackupCleanCounterTombstones(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var backupPath = NewDataPath(suffix: "BackupFolder");
                var config = Backup.CreateBackupConfiguration(backupPath, incrementalBackupFrequency: "0 0 1 1 *");
                var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "EGR" }, "user/322");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var cf = session.CountersFor("user/322");
                    cf.Increment("Likes", 100);
                    cf.Increment("Likes2", 200);
                    cf.Increment("Likes3", 300);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("user/322");
                    var cf = session.CountersFor(user);
                    cf.Delete("Likes");
                    cf.Delete("Likes2");
                    session.SaveChanges();
                }

                var storage = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "user/322");

                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                    Assert.Equal(2, c2);
                }

                var cleaner = storage.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                var c = 0L;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    c += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    c += storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }
                Assert.True(c > 0);

                await Backup.RunBackupInClusterAsync(store, result.TaskId, isFullBackup: true);
                await cleaner.ExecuteCleanup();

                c = 0;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    c += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    c += storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }

                Assert.Equal(0, c);
            }
        }

        [RavenTheory(RavenTestCategory.Counters | RavenTestCategory.BackupExportImport | RavenTestCategory.Cluster | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CleanCounterTombstonesInTheClusterWithOnlyFullBackup(Options options)
        {
            var cluster = await CreateRaftCluster(3);
            var database = GetDatabaseName();
            await CreateDatabaseInClusterForMode(database, 3, cluster, options.DatabaseMode);
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore(new Options(options)
            {
                CreateDatabase = false,
                Server = cluster.Leader,
                ModifyDatabaseName = _ => database
            }))
            {
                var config = Backup.CreateBackupConfiguration(backupPath);
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "EGR" }, "user/322");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var cf = session.CountersFor("user/322");
                    cf.Increment("Likes", 100);
                    cf.Increment("Likes2", 200);
                    cf.Increment("Likes3", 300);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var markerId = $"marker/{Guid.NewGuid()}";
                    session.Store(new User { Name = "Karmel" }, markerId);
                    session.SaveChanges();
                    Assert.True(await WaitForDocumentInClusterAsync<User>(cluster.Nodes, store.Database, markerId, (u) => u.Id == markerId, Debugger.IsAttached ? TimeSpan.FromSeconds(60) : TimeSpan.FromSeconds(15)));
                }

                Assert.True(await WaitForChangeVectorInClusterForModeAsync(cluster.Nodes, database, options.DatabaseMode), "await WaitForChangeVectorInClusterAsync(cluster.Nodes, database)");

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("user/322");
                    var cf = session.CountersFor(user);
                    cf.Delete("Likes");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var markerId = $"marker2/{Guid.NewGuid()}";
                    session.Store(new User { Name = "Karmel" }, markerId);
                    session.SaveChanges();
                    Assert.True(await WaitForDocumentInClusterAsync<User>(cluster.Nodes, store.Database, markerId, (u) => u.Id == markerId, Debugger.IsAttached ? TimeSpan.FromSeconds(60) : TimeSpan.FromSeconds(15)));
                }

                Assert.True(await WaitForChangeVectorInClusterForModeAsync(cluster.Nodes, database, options.DatabaseMode), "await WaitForChangeVectorInClusterAsync(cluster.Nodes, database)");

                foreach (var server in cluster.Nodes)
                {
                    var storage = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "user/322", server: server);
                    using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var c2 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                        Assert.Equal(1, c2);
                    }
                }

                var res = await WaitForValueAsync(async () =>
                {
                    var c = 0L;
                    foreach (var server in cluster.Nodes)
                    {
                        var storage = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "user/322", server: server);
                        await storage.TombstoneCleaner.ExecuteCleanup();
                        using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            c += storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                        }
                    }
                    return c;
                }, 0, interval: 333);
                Assert.Equal(0, res);
            }
        }

        [RavenTheory(RavenTestCategory.Counters | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldDeleteCounterFromStorageWhenExecuteCleanup_RegularReplication(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "EGR" }, "user/322");
                    session.SaveChanges();
                }

                using (var session = store1.OpenSession())
                {
                    var cf = session.CountersFor("user/322");
                    cf.Increment("Likes", 100);
                    cf.Increment("Likes2", 200);
                    session.SaveChanges();
                }
                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                using (var session = store1.OpenSession())
                {
                    var cf = session.CountersFor("user/322");
                    cf.Delete("Likes");
                    session.SaveChanges();
                }

                await EnsureReplicatingAsync(store1, store2);

                var storage = await GetDocumentDatabaseInstanceForAsync(store1, options.DatabaseMode, "user/322");
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                    Assert.Equal(1, c2);
                }

                await EnsureReplicatingAsync(store1, store2);

                var cleaner = storage.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var cv = storage.DocumentsStorage.CountersStorage.GetNumberOfCountersAndDeletedCountersForDocument(context, "user/322");
                    Assert.Equal(1, cv);
                }

                long count1 = 0;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    count1 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }
                Assert.Equal(0, count1);

                var storage2 = await GetDocumentDatabaseInstanceForAsync(store2, options.DatabaseMode, "user/322");
                long count2 = 0;
                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    count2 = storage2.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }
                Assert.Equal(1, count2);

                cleaner = storage2.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    count2 = storage2.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }
                Assert.Equal(count1, count2);
            }
        }

        [RavenTheory(RavenTestCategory.Counters | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldDeleteCounterFromStorageWhenExecuteCleanup_ConflictReplication(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "EGR" }, "user/322");
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    session.Store(new User { Name = "EGR" }, "user/322");
                    session.SaveChanges();
                }

                using (var session = store1.OpenSession())
                {
                    var cf = session.CountersFor("user/322");
                    cf.Increment("Likes", 100);
                    cf.Increment("Likes2", 200);
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    var cf = session.CountersFor("user/322");
                    cf.Increment("Likes", 500);
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    var cf = session.CountersFor("user/322");
                    cf.Delete("Likes");
                    session.SaveChanges();
                }

                using (var session = store1.OpenSession())
                {
                    var cf = session.CountersFor("user/322");
                    cf.Delete("Likes");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await SetupReplicationAsync(store2, store1);

                await EnsureReplicatingAsync(store1, store2);
                await EnsureReplicatingAsync(store2, store1);

                var storage = await GetDocumentDatabaseInstanceForAsync(store1, options.DatabaseMode, "user/322");
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                    Assert.Equal(1, c2);
                }

                var storage2 = await GetDocumentDatabaseInstanceForAsync(store2, options.DatabaseMode, "user/322");
                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage2.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                    Assert.Equal(1, c2);
                }

                await EnsureReplicatingAsync(store1, store2);

                var cleaner = storage.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var cv = storage.DocumentsStorage.CountersStorage.GetNumberOfCountersAndDeletedCountersForDocument(context, "user/322");
                    Assert.Equal(1, cv);
                }

                cleaner = storage2.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var cv = storage2.DocumentsStorage.CountersStorage.GetNumberOfCountersAndDeletedCountersForDocument(context, "user/322");
                    Assert.Equal(1, cv);
                }

                long count1 = 0;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    count1 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }
                Assert.Equal(0, count1);


                long count2 = 0;
                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    count2 = storage2.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }
                Assert.Equal(count1, count2);
            }
        }

        [RavenTheory(RavenTestCategory.Counters | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldDeleteCounterFromStorageWhenExecuteCleanup_ManyCountersAndCounterDelete(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "EGR" }, "user/322");
                    session.SaveChanges();
                }

                using (var session = store1.OpenSession())
                {
                    var cf = session.CountersFor("user/322");
                    for (var i = 0; i < 1024; i++)
                    {
                        cf.Increment($"Likes/{i}", i);
                    }
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                using (var session = store1.OpenSession())
                {
                    // delete the first counter
                    var cf = session.CountersFor("user/322");
                    cf.Delete("Likes/0");
                    session.SaveChanges();
                }

                await EnsureReplicatingAsync(store1, store2);

                var storage = await GetDocumentDatabaseInstanceForAsync(store1, options.DatabaseMode, "user/322");
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                    Assert.Equal(1, c2);
                }

                await EnsureReplicatingAsync(store1, store2);

                var storage2 = await GetDocumentDatabaseInstanceForAsync(store2, options.DatabaseMode, "user/322");
                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage2.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                    Assert.Equal(1, c2);
                }

                var cleaner = storage.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var cv = storage.DocumentsStorage.CountersStorage.GetNumberOfCountersAndDeletedCountersForDocument(context, "user/322");
                    Assert.Equal(1023, cv);
                }

                cleaner = storage2.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var cv = storage2.DocumentsStorage.CountersStorage.GetNumberOfCountersAndDeletedCountersForDocument(context, "user/322");
                    Assert.Equal(1023, cv);
                }

                long count1 = 0;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    count1 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }
                Assert.Equal(0, count1);


                long count2 = 0;
                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    count2 = storage2.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }
                Assert.Equal(count1, count2);
            }
        }

        [RavenTheory(RavenTestCategory.Counters | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldDeleteCounterFromStorageWhenExecuteCleanup_ManyCountersAndCountersDeleteReplication2(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "EGR" }, "user/322");
                    session.SaveChanges();
                }

                using (var session = store1.OpenSession())
                {
                    var cf = session.CountersFor("user/322");
                    for (var i = 0; i < 1024; i++)
                    {
                        cf.Increment($"Likes/{i}", i);
                    }
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                using (var session = store1.OpenSession())
                {
                    // delete 512 counters
                    var cf = session.CountersFor("user/322");
                    for (var i = 0; i < 1024; i += 2)
                    {
                        cf.Delete($"Likes/{i}");
                    }
                    session.SaveChanges();
                }

                await EnsureReplicatingAsync(store1, store2);

                var storage = await GetDocumentDatabaseInstanceForAsync(store1, options.DatabaseMode, "user/322");
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                    Assert.Equal(512, c2);
                }

                await EnsureReplicatingAsync(store1, store2);

                var storage2 = await GetDocumentDatabaseInstanceForAsync(store2, options.DatabaseMode, "user/322");
                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage2.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                    Assert.Equal(512, c2);
                }

                var cleaner = storage.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                await EnsureReplicatingAsync(store1, store2);

                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var cv = storage.DocumentsStorage.CountersStorage.GetNumberOfCountersAndDeletedCountersForDocument(context, "user/322");
                    Assert.Equal(512, cv);
                }

                cleaner = storage2.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var cv = storage2.DocumentsStorage.CountersStorage.GetNumberOfCountersAndDeletedCountersForDocument(context, "user/322");
                    Assert.Equal(512, cv);
                }

                long count1 = 0;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    count1 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }
                Assert.Equal(0, count1);


                long count2 = 0;
                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    count2 = storage2.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }
                Assert.Equal(count1, count2);
            }
        }

        [RavenTheory(RavenTestCategory.Counters | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldDeleteCounterFromStorageWhenExecuteCleanup_ManyCountersAndCountersDelete2(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "EGR" }, "user/322");
                    session.SaveChanges();
                }

                using (var session = store1.OpenSession())
                {
                    var cf = session.CountersFor("user/322");
                    for (var i = 0; i < 1024; i++)
                    {
                        cf.Increment($"Likes/{i}", i);
                    }
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                using (var session = store1.OpenSession())
                {
                    // delete the first 512 counters
                    var cf = session.CountersFor("user/322");
                    for (var i = 0; i < 512; i++)
                    {
                        cf.Delete($"Likes/{i}");
                    }
                    session.SaveChanges();
                }

                await EnsureReplicatingAsync(store1, store2);

                var storage = await GetDocumentDatabaseInstanceForAsync(store1, options.DatabaseMode, "user/322");
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                    Assert.Equal(512, c2);
                }

                await EnsureReplicatingAsync(store1, store2);

                var storage2 = await GetDocumentDatabaseInstanceForAsync(store2, options.DatabaseMode, "user/322");
                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage2.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                    Assert.Equal(512, c2);
                }

                var cleaner = storage.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                await EnsureReplicatingAsync(store1, store2);

                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var cv = storage.DocumentsStorage.CountersStorage.GetNumberOfCountersAndDeletedCountersForDocument(context, "user/322");
                    Assert.Equal(512, cv);
                }

                cleaner = storage2.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var cv = storage2.DocumentsStorage.CountersStorage.GetNumberOfCountersAndDeletedCountersForDocument(context, "user/322");
                    Assert.Equal(512, cv);
                }

                long count1 = 0;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    count1 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }
                Assert.Equal(0, count1);


                long count2 = 0;
                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    count2 = storage2.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }
                Assert.Equal(count1, count2);
            }
        }

        [RavenTheory(RavenTestCategory.Counters | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldDeleteCounterFromStorageWhenExecuteCleanup_ManyCountersAndCountersDelete3(Options options)
        {
            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "EGR" }, "user/322");
                    session.SaveChanges();
                }

                using (var session = store1.OpenSession())
                {
                    var cf = session.CountersFor("user/322");
                    for (var i = 0; i < 4; i++)
                    {
                        cf.Increment($"Likes/{i}", i);
                    }
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                await EnsureReplicatingAsync(store1, store2);

                using (var session = store1.OpenSession())
                {
                    // delete the first counter
                    var cf = session.CountersFor("user/322");
                    cf.Delete("Likes/0");
                    session.SaveChanges();
                }

                await EnsureReplicatingAsync(store1, store2);

                using (var session = store1.OpenSession())
                {
                    // delete the first counter
                    var cf = session.CountersFor("user/322");
                    cf.Increment("Likes/0");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(store2, store1);
                await EnsureReplicatingAsync(store2, store1);

                var storage = await GetDocumentDatabaseInstanceForAsync(store1, options.DatabaseMode, "user/322");
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                    Assert.Equal(1, c2);
                }

                await EnsureReplicatingAsync(store1, store2);

                var storage2 = await GetDocumentDatabaseInstanceForAsync(store2, options.DatabaseMode, "user/322");
                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage2.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                    Assert.Equal(1, c2);
                }

                var cleaner = storage.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var cv = storage.DocumentsStorage.CountersStorage.GetNumberOfCountersAndDeletedCountersForDocument(context, "user/322");
                    Assert.Equal(4, cv);
                }

                cleaner = storage2.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                // ensures that we don't delete the counter but we do clean the counter tombstones table

                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var cv = storage2.DocumentsStorage.CountersStorage.GetNumberOfCountersAndDeletedCountersForDocument(context, "user/322");
                    Assert.Equal(4, cv);
                }

                long count1 = 0;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    count1 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }
                Assert.Equal(0, count1);


                long count2 = 0;
                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    count2 = storage2.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }
                Assert.Equal(count1, count2);
            }
        }

        [RavenTheory(RavenTestCategory.Counters)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldCleanCounterTombstonesWhenBatchSizeSmallerThanCountersToDelete(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "EGR" }, "user/322");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var cf = session.CountersFor("user/322");
                    for (var i = 0; i < 3000; i++)
                    {
                        cf.Increment($"Likes/{i}", i);
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var cf = session.CountersFor("user/322");
                    for (var i = 0; i < 2024; i++)
                    {
                        cf.Delete($"Likes/{i}");
                    }

                    session.SaveChanges();
                }

                var storage = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "user/322");
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                    Assert.Equal(2024, c2);
                }

                var cleaner = storage.TombstoneCleaner;
                await cleaner.ExecuteCleanup(1024);

                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var cv = storage.DocumentsStorage.CountersStorage.GetNumberOfCountersAndDeletedCountersForDocument(context, "user/322");
                    Assert.Equal(976, cv);
                }

                long count1 = 0;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    count1 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                }
                Assert.Equal(0, count1);
            }
        }

        private class MyCounterIndex : AbstractCountersIndexCreationTask<Company>
        {
            public MyCounterIndex()
            {
                AddMap("HeartRate", counters => from counter in counters
                                                select new
                                                {
                                                    HeartBeat = counter.Value,
                                                    Name = counter.Name,
                                                    User = counter.DocumentId
                                                });
            }
        }

        private class AverageHeartRate : AbstractCountersIndexCreationTask<User, AverageHeartRate.Result>
        {
            public class Result
            {
                public double HeartBeat { get; set; }

                public string Name { get; set; }

                public long Count { get; set; }
            }

            public AverageHeartRate()
            {
                AddMap("HeartRate", counters => from counter in counters
                                                select new Result
                                                {
                                                    HeartBeat = counter.Value,
                                                    Count = 1,
                                                    Name = counter.Name
                                                });

                Reduce = results => from r in results
                                    group r by r.Name into g
                                    let sumHeartBeat = g.Sum(x => x.HeartBeat)
                                    let sumCount = g.Sum(x => x.Count)
                                    select new Result
                                    {
                                        HeartBeat = sumHeartBeat / sumCount,
                                        Name = g.Key,
                                        Count = sumCount
                                    };
            }
        }
    }
}

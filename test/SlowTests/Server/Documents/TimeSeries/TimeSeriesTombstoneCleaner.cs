using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.TimeSeries
{
    public class TimeSeriesTombstoneCleaner : ReplicationTestBase
    {
        public TimeSeriesTombstoneCleaner(ITestOutputHelper output) : base(output)
        {
        }

        private readonly DateTime _baseline = RavenTestHelper.UtcToday;

        [Fact]
        public async Task IndexCleanTimeSeriesTombstones()
        {
            using (var store = GetDocumentStore(new Options()))
            {
                new MyTsIndex().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "EGR" }, "user/322");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("user/322", "raven");
                    for (int i = 0; i <= 3000; i++)
                    {
                        tsf.Append(_baseline.AddMinutes(i), new[] { (double)i }, "watches/apple");
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("user/322");
                    var tsf = session.TimeSeriesFor(user, "raven");

                    for (int i = 0; i < 1500; i++)
                    {
                        if (i % 2 == 0)
                            tsf.Delete(_baseline.AddMinutes(i));
                    }

                    session.SaveChanges();
                }

                var storage = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRanges(context);
                    var c3 = storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesPendingDeletionSegments(context);
                    Assert.Equal(750, c2);
                    Assert.Equal(2, c3);
                }

                var cleaner = storage.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                var c = 0l;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    c += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    c += storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRanges(context);
                    c += storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesPendingDeletionSegments(context);
                }
                Assert.True(c >= 0);

                WaitForIndexing(store);
                await cleaner.ExecuteCleanup();

                c = 0;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {

                    c += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    c += storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRanges(context);
                    c += storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesPendingDeletionSegments(context);
                }

                Assert.Equal(0, c);
            }
        }

        [Fact]
        public async Task ReplicationCleanTimeSeriesTombstones()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new User { Name = "EGR" }, "user/322");
                    session.SaveChanges();
                }

                using (var session = store1.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("user/322", "raven");
                    for (int i = 0; i <= 3000; i++)
                    {
                        tsf.Append(_baseline.AddMinutes(i), new[] { (double)i }, "watches/apple");
                    }

                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);
                EnsureReplicating(store1, store2);

                using (var session = store1.OpenSession())
                {
                    var user = session.Load<User>("user/322");
                    var tsf = session.TimeSeriesFor(user, "raven");

                    for (int i = 0; i < 1500; i++)
                    {
                        if (i % 2 == 0)
                            tsf.Delete(_baseline.AddMinutes(i));
                    }

                    session.SaveChanges();
                }

                var storage = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database);
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRanges(context);
                    var c3 = storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesPendingDeletionSegments(context);
                    Assert.Equal(750, c2);
                    Assert.Equal(2, c3);
                }

                EnsureReplicating(store1, store2);

                var cleaner = storage.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                var c = 0l;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    c += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    c += storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRanges(context);
                    c += storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesPendingDeletionSegments(context);
                }

                Assert.Equal(0, c);
                long tsCount1 = 0;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    tsCount1 = storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRanges(context);
                }

                var storage2 = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store1.Database);
                long tsCount2 = 0;
                using (storage2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    tsCount2 = storage2.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRanges(context);
                }

                Assert.Equal(tsCount1, tsCount2);
            }
        }

        [Fact]
        public async Task IncrementalBackupCleanTimeSeriesTombstones()
        {
            using (var store = GetDocumentStore(new Options()))
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
                    var tsf = session.TimeSeriesFor("user/322", "raven");
                    for (int i = 0; i <= 3000; i++)
                    {
                        tsf.Append(_baseline.AddMinutes(i), new[] { (double)i }, "watches/apple");
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("user/322");
                    var tsf = session.TimeSeriesFor(user, "raven");

                    for (int i = 0; i < 1500; i++)
                    {
                        if (i % 2 == 0)
                            tsf.Delete(_baseline.AddMinutes(i));
                    }

                    session.SaveChanges();
                }

                var storage = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRanges(context);
                    var c3 = storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesPendingDeletionSegments(context);
                    Assert.Equal(750, c2);
                    Assert.Equal(2, c3);
                }

                var cleaner = storage.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                var c = 0l;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    c += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    c += storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRanges(context);
                    c += storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesPendingDeletionSegments(context);
                }
                Assert.True(c > 0);

                await Backup.RunBackupInClusterAsync(store, result.TaskId, isFullBackup: true);
                await cleaner.ExecuteCleanup();

                c = 0;
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {

                    c += storage.DocumentsStorage.GetNumberOfTombstones(context);
                    c += storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRanges(context);
                    c += storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesPendingDeletionSegments(context);
                }

                Assert.Equal(0, c);
            }
        }

        [Fact]
        public async Task CleanTimeSeriesTombstonesInTheClusterWithOnlyFullBackup()
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
                var config = Backup.CreateBackupConfiguration(backupPath);
                await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(config));

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "EGR" }, "user/322");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var tsf = session.TimeSeriesFor("user/322", "raven");
                    for (int i = 0; i <= 3000; i++)
                    {
                        tsf.Append(_baseline.AddMinutes(i), new[] { (double)i }, "watches/apple");
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("user/322");
                    var tsf = session.TimeSeriesFor(user, "raven");

                    for (int i = 0; i < 1500; i++)
                    {
                        if (i % 2 == 0)
                            tsf.Delete(_baseline.AddMinutes(i));
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var markerId = $"marker/{Guid.NewGuid()}";
                    session.Store(new User { Name = "Karmel" }, markerId);
                    session.SaveChanges();
                    Assert.True(await WaitForDocumentInClusterAsync<User>(cluster.Nodes, store.Database, markerId, (u) => u.Id == markerId, Debugger.IsAttached ? TimeSpan.FromSeconds(60) : TimeSpan.FromSeconds(15)));
                }

                // TODO: RavenDB-16914
                //  Assert.True(await WaitForChangeVectorInClusterAsync(cluster.Nodes, database));

                foreach (var server in cluster.Nodes)
                {
                    var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var c2 = storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRanges(context);
                        var c3 = storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesPendingDeletionSegments(context);


                        Assert.Equal(750, c2);
                        Assert.Equal(2, c3);
                    }
                }

                var res = await WaitForValueAsync(async () =>
                {
                    var c = 0L;
                    foreach (var server in cluster.Nodes)
                    {
                        var storage = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                        await storage.TombstoneCleaner.ExecuteCleanup();
                        using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            c += storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRanges(context);
                            c += storage.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesPendingDeletionSegments(context);
                        }
                    }
                    return c;
                }, 0, interval: 333);
                Assert.Equal(0, res);
            }
        }

        private class MyTsIndex : AbstractTimeSeriesIndexCreationTask<User>
        {
            public MyTsIndex()
            {
                AddMap(
                    "raven",
                    timeSeries => from ts in timeSeries
                                  from entry in ts.Entries
                                  select new
                                  {
                                      HeartBeat = entry.Values[0],
                                      entry.Timestamp.Date,
                                      User = ts.DocumentId
                                  });
            }
        }

    }
}

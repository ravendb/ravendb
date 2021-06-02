using System.IO;
using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Storage.Schema.Updates.Documents;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12022 : RavenTestBase
    {
        public RavenDB_12022(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanMigrateLegacyCounters()
        {
            From41016.NumberOfCountersToMigrateInSingleTransaction = 20;

            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, "northwind.ravendb-snapshot");

            ExtractFile(fullBackupPath);

            using (var store = GetDocumentStore())
            {
                var databaseName = GetDatabaseName();

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = backupPath,
                    DatabaseName = databaseName
                }))
                {
                    var stats = store.Maintenance.ForDatabase(databaseName).Send(new GetStatisticsOperation());

                    Assert.Equal(6, stats.CountOfDocuments);
                    Assert.Equal(6, stats.CountOfCounterEntries);

                    using (var session = store.OpenSession(databaseName))
                    {
                        for (var i = 1; i <= 5; i++)
                        {
                            var doc = session.Load<User>($"users/{i}");
                            Assert.NotNull(doc);
                            Assert.Equal(i.ToString(), doc.Name);

                            var all = store.Operations
                                .ForDatabase(databaseName)
                                .Send(new GetCountersOperation(doc.Id, returnFullResults: true));
                            
                            Assert.Equal(10, all.Counters.Count);
                            // rearrange as likes/1, likes/2, ..., likes/10
                            var tmp = all.Counters[1];                               
                            all.Counters.Remove(tmp);
                            all.Counters.Add(tmp);

                            for (int j = 1; j <= 10; j++)
                            {
                                var counterDetail = all.Counters[j-1];
                                Assert.Equal($"likes/{j}", counterDetail.CounterName);
                                Assert.Equal(j * 2, counterDetail.TotalValue);
                                Assert.Equal(2, counterDetail.CounterValues.Count);
                            }
                        }

                        var order = session.Load<Order>("orders/1");
                        Assert.NotNull(order);
                        Assert.Equal("HR", order.Company);

                        var details = store.Operations
                            .ForDatabase(databaseName)
                            .Send(new GetCountersOperation(order.Id, returnFullResults: true));

                        Assert.Equal(1, details.Counters.Count);
                        Assert.Equal("downloads/3", details.Counters[0].CounterName);
                        Assert.Equal(3, details.Counters[0].TotalValue);

                        // verify that we removed the counter-tombstones from tombstones table
                        var db = GetDocumentDatabaseInstanceFor(store, databaseName).Result;
                        using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                        using (ctx.OpenReadTransaction())
                        {
                            var tombstones = db.DocumentsStorage.GetTombstonesFrom(ctx, 0, 0, int.MaxValue).ToList();
                            Assert.Equal(1, tombstones.Count);
                            Assert.Equal(Tombstone.TombstoneType.Document, tombstones[0].Type);
                        }

                    }
                }

            }
        }

        private static void ExtractFile(string path)
        {
            using (var file = File.Create(path))
            using (var stream = typeof(RavenDB_10404).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_12022.counters.4.1.4.ravendb-snapshot"))
            {
                stream.CopyTo(file);
            }
        }
    }
}

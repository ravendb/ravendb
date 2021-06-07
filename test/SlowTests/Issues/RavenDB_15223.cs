using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Storage.Schema.Updates.Documents;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15223 : RavenTestBase
    {
        public RavenDB_15223(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanMigrateCountersFrom42017()
        {
            From42017.NumberOfCounterGroupsToMigrateInSingleTransaction = 10;

            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, "northwind.ravendb-snapshot");
            var resource = "SlowTests.Data.RavenDB_15223.counters.4.2.103.ravendb-snapshot";

            ExtractFile(fullBackupPath, resource);

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

                    Assert.Equal(100, stats.CountOfDocuments);
                    Assert.Equal(600, stats.CountOfCounterEntries);

                    for (var i = 1; i <= 100; i++)
                    {
                        using (var session = store.OpenSession(databaseName))
                        {
                            var doc = session.Load<User>($"users/{i}");
                            Assert.NotNull(doc);
                            Assert.Equal("aviv", doc.Name);

                            var all = store.Operations
                                .ForDatabase(databaseName)
                                .Send(new GetCountersOperation(doc.Id, returnFullResults: true));

                            Assert.Equal(200, all.Counters.Count);

                            var dict = all.Counters.ToDictionary(keySelector: c => c.CounterName, elementSelector: c => c.TotalValue);

                            for (int j = 1; j <= 100; j++)
                            {
                                Assert.True(dict.TryGetValue("Likes/" + j, out var val));
                                Assert.Equal(j, val);
                                Assert.True(dict.TryGetValue("DisLikes/" + j, out val));
                                Assert.Equal(1, val);
                            }
                        }
                    }
                }
            }
        }

        [Fact]
        public void CanMigrateCountersFrom42017_SingleDocManyCounters()
        {
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, "northwind.ravendb-snapshot");
            var resource = "SlowTests.Data.RavenDB_15223.manycounters.4.2.103.ravendb-snapshot";

            ExtractFile(fullBackupPath, resource);

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

                    Assert.Equal(1, stats.CountOfDocuments);
                    Assert.Equal(339, stats.CountOfCounterEntries);

                    using (var session = store.OpenSession(databaseName))
                    {
                        var u = session.Load<User>("users/1");
                        Assert.NotNull(u);

                        var countersMetadata = session.Advanced.GetCountersFor(u);
                        Assert.Equal(10_000, countersMetadata.Count);
                    }
                    
                    var all = store.Operations
                        .ForDatabase(databaseName)
                        .Send(new GetCountersOperation("users/1", returnFullResults: true));

                    Assert.Equal(10_000, all.Counters.Count);
                }
            }
        }

        private static void ExtractFile(string path, string resourceName)
        {
            using (var file = File.Create(path))
            using (var stream = typeof(RavenDB_15223).Assembly.GetManifestResourceStream(resourceName))
            {
                stream.CopyTo(file);
            }
        }
    }
}

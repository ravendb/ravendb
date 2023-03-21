using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13512 : RavenTestBase
    {
        public RavenDB_13512(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_restore_legacy_counters_from_full_backup()
        {
            var backupPath = NewDataPath(forceCreateDir: true);
            var fullBackupPath = Path.Combine(backupPath, "RavenDB_13512.counters.ravendb-full-backup");
            RavenDB_13468.ExtractFile(fullBackupPath, "SlowTests.Data.RavenDB_13512.counters.test.4.1.6.ravendb-full-backup");

            using (var store = GetDocumentStore())
            {
                var databaseName = $"restored_database-{Guid.NewGuid()}";

                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    BackupLocation = backupPath,
                    DatabaseName = databaseName
                }))
                {
                    await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), backupPath);

                    using (var session = store.OpenAsyncSession(databaseName))
                    {
                        var user = await session.LoadAsync<User>("users/1");
                        Assert.NotNull(user);

                        var counters = await session.CountersFor(user).GetAllAsync();
                        Assert.Equal(100, counters.Count);
                        for (int i = 0; i < counters.Count; i++)
                        {
                            Assert.True(counters.TryGetValue("counter/"+i, out var val));
                            Assert.Equal(i, val);
                        }
                    }
                }
            }
        }
    }
}

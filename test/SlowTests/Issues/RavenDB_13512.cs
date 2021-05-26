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

        [Fact]
        public async Task Can_restore_legacy_counters_from_incremental_backup()
        {
            var assemblyPrefix = "SlowTests.Data.RavenDB_13512.Incremental.";

            var backupPath = NewDataPath(forceCreateDir: true);

            var fullBackupPath = Path.Combine(backupPath, "17.ravendb-full-backup");
            var incrementalBackupPath1 = Path.Combine(backupPath, "18.ravendb-incremental-backup");
            var incrementalBackupPath2 = Path.Combine(backupPath, "18-01.ravendb-incremental-backup");

            RavenDB_13468.ExtractFile(fullBackupPath, assemblyPrefix + "17.ravendb-full-backup");
            RavenDB_13468.ExtractFile(incrementalBackupPath1, assemblyPrefix + "18.ravendb-incremental-backup");
            RavenDB_13468.ExtractFile(incrementalBackupPath2, assemblyPrefix + "18-01.ravendb-incremental-backup");

            using (var store = GetDocumentStore())
            {
                var importOptions = new DatabaseSmugglerImportOptions();
#pragma warning disable 618
                importOptions.OperateOnTypes |= DatabaseItemType.Counters;
#pragma warning restore 618

                await store.Smuggler.ImportIncrementalAsync(importOptions, backupPath);

                using (var session = store.OpenAsyncSession())
                {
                    var user1 = await session.LoadAsync<User>("users/1");
                    Assert.NotNull(user1);

                    var metadata = session.Advanced.GetMetadataFor(user1);
                    Assert.True(metadata.ContainsKey("@counters"));

                    var counters = await session.CountersFor(user1).GetAllAsync();
                    Assert.Equal(100, counters.Count);
                    for (int i = 0; i < counters.Count; i++)
                    {
                        Assert.True(counters.TryGetValue("counter/" + i, out var val));
                        Assert.Equal(i * 3, val);
                    }

                    var user2 = await session.LoadAsync<User>("users/2");
                    Assert.NotNull(user2);

                    counters = await session.CountersFor(user2).GetAllAsync();
                    Assert.Equal(100, counters.Count);
                    for (int i = 0; i < counters.Count; i++)
                    {
                        Assert.True(counters.TryGetValue("counter/" + i, out var val));
                        Assert.Equal(i * 2, val);
                    }
                }
            }
        }
    }
}

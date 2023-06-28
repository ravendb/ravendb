using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20776 : RavenTestBase
{
    public RavenDB_20776(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Import_Should_Disable_Subscriptions()
    {
        var path = NewDataPath();
        var exportPath = Path.Combine(path, "export.ravendbdump");
        using (var store = GetDocumentStore())
        {
            await store.Subscriptions.CreateAsync<Company>();

            var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportPath);
            var result = await operation.WaitForCompletionAsync<SmugglerResult>(TimeSpan.FromSeconds(30));

            Assert.Equal(1, result.Subscriptions.ReadCount);
        }

        using (var store = GetDocumentStore())
        {
            var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportPath);
            var result = await operation.WaitForCompletionAsync<SmugglerResult>(TimeSpan.FromSeconds(30));

            Assert.Equal(1, result.Subscriptions.ReadCount);

            var subscriptions = await store.Subscriptions.GetSubscriptionsAsync(0, int.MaxValue);

            Assert.NotNull(subscriptions);
            Assert.Equal(1, subscriptions.Count);

            Assert.True(subscriptions[0].Disabled);

            var backupOperation = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
            {
                BackupType = BackupType.Backup,
                LocalSettings = new LocalSettings
                {
                    FolderPath = path
                }
            }));

            var backupResult = await backupOperation.WaitForCompletionAsync<BackupResult>(TimeSpan.FromSeconds(30));

            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
            {
                BackupLocation = Path.Combine(path, backupResult.LocalBackup.BackupDirectory),
                DatabaseName = $"{store.Database}_restore_1",
                DisableOngoingTasks = false
            }))
            using (var restoreStore = GetDocumentStore(new Options
            {
                CreateDatabase = false,
                ModifyDatabaseName = _ => $"{store.Database}_restore_1"
            }))
            {
                subscriptions = await restoreStore.Subscriptions.GetSubscriptionsAsync(0, int.MaxValue);

                Assert.NotNull(subscriptions);
                Assert.Equal(1, subscriptions.Count);

                Assert.False(subscriptions[0].Disabled);
            }

            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
            {
                BackupLocation = Path.Combine(path, backupResult.LocalBackup.BackupDirectory),
                DatabaseName = $"{store.Database}_restore_2",
                DisableOngoingTasks = true
            }))
            using (var restoreStore = GetDocumentStore(new Options
            {
                CreateDatabase = false,
                ModifyDatabaseName = _ => $"{store.Database}_restore_2"
            }))
            {
                subscriptions = await restoreStore.Subscriptions.GetSubscriptionsAsync(0, int.MaxValue);

                Assert.NotNull(subscriptions);
                Assert.Equal(1, subscriptions.Count);

                Assert.True(subscriptions[0].Disabled);
            }
        }
    }
}

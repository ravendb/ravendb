using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_28134(ITestOutputHelper output) : RavenTestBase(output)
{
    private class User
    {
        public string Name { get; set; }
        public int Age { get; set; }
    }

    [RavenTheory(RavenTestCategory.Subscriptions)]
    [RavenData(false, DatabaseMode = RavenDatabaseMode.Single)]
    [RavenData(true, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task GenericCreateKeepsPinToMentorNodeAndArchivedDataProcessingBehavior(Options options, bool useAsync)
    {
        using var store = GetDocumentStore(options);

        var creationOptions = new SubscriptionCreationOptions<User>
        {
            MentorNode = "A",
            PinToMentorNode = true,
            ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior.ArchivedOnly
        };

        var name = useAsync
            ? await store.Subscriptions.CreateAsync(creationOptions)
            : store.Subscriptions.Create(creationOptions);

        var state = await store.Subscriptions.GetSubscriptionStateAsync(name);
        Assert.Equal("A", state.MentorNode);
        Assert.True(state.PinToMentorNode);
        Assert.Equal(ArchivedDataProcessingBehavior.ArchivedOnly, state.ArchivedDataProcessingBehavior);
    }

    [RavenTheory(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task CreateKeepsArchivedDataProcessingBehavior(Options options)
    {
        using var store = GetDocumentStore(options);

        var name = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
        {
            Query = "from Users",
            ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior.ArchivedOnly
        });

        var state = await store.Subscriptions.GetSubscriptionStateAsync(name);
        Assert.Equal(ArchivedDataProcessingBehavior.ArchivedOnly, state.ArchivedDataProcessingBehavior);
    }

    [RavenTheory(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task UpdateKeepsStoredArchivedDataProcessingBehavior(Options options)
    {
        using var store = GetDocumentStore(options);

        var name = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
        {
            Query = "from Users",
            ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior.ArchivedOnly
        });

        await store.Subscriptions.UpdateAsync(new SubscriptionUpdateOptions { Name = name, Query = "from Users where Age > 1" });

        var state = await store.Subscriptions.GetSubscriptionStateAsync(name);
        Assert.Equal("from Users where Age > 1", state.Query);
        Assert.Equal(ArchivedDataProcessingBehavior.ArchivedOnly, state.ArchivedDataProcessingBehavior);
    }

    [RavenTheory(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task UpdateWithCreateNewKeepsArchivedDataProcessingBehavior(Options options)
    {
        using var store = GetDocumentStore(options);

        await store.Subscriptions.UpdateAsync(new SubscriptionUpdateOptions
        {
            Name = "NewSubscription",
            Query = "from Users",
            CreateNew = true,
            ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior.ArchivedOnly
        });

        var state = await store.Subscriptions.GetSubscriptionStateAsync("NewSubscription");
        Assert.Equal(ArchivedDataProcessingBehavior.ArchivedOnly, state.ArchivedDataProcessingBehavior);
    }

    [RavenTheory(RavenTestCategory.Subscriptions | RavenTestCategory.Sharding)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task UpdateCanChangeArchivedDataProcessingBehavior(Options options)
    {
        using var store = GetDocumentStore(options);

        var name = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions { Query = "from Users" });

        await store.Subscriptions.UpdateAsync(new SubscriptionUpdateOptions
        {
            Name = name,
            ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior.ArchivedOnly
        });

        var state = await store.Subscriptions.GetSubscriptionStateAsync(name);
        Assert.Equal("from Users", state.Query);
        Assert.Equal(ArchivedDataProcessingBehavior.ArchivedOnly, state.ArchivedDataProcessingBehavior);
    }

    [RavenTheory(RavenTestCategory.Subscriptions | RavenTestCategory.Smuggler)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task ImportKeepsSettingsButDropsMentorNode(Options options)
    {
        var file = GetTempFileName();
        try
        {
            using (var source = GetDocumentStore(options))
            {
                await CreateSubscriptionWithAllSettingsAsync(source);

                var export = await source.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions { OperateOnTypes = DatabaseItemType.Subscriptions }, file);
                await export.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }

            using (var target = GetDocumentStore(options))
            {
                var import = await target.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions { OperateOnTypes = DatabaseItemType.Subscriptions }, file);
                var result = await import.WaitForCompletionAsync<SmugglerResult>(TimeSpan.FromMinutes(1));

                var state = await target.Subscriptions.GetSubscriptionStateAsync(SubscriptionName);
                Assert.True(state.Disabled);
                Assert.Equal(ArchivedDataProcessingBehavior.ArchivedOnly, state.ArchivedDataProcessingBehavior);

                // the mentor node refers to a node of the source cluster
                Assert.Null(state.MentorNode);
                Assert.False(state.PinToMentorNode);
                Assert.Contains(result.Messages, m => m.Contains($"Subscription '{SubscriptionName}' was imported without its mentor node 'A'"));
            }
        }
        finally
        {
            File.Delete(file);
        }
    }

    [RavenTheory(RavenTestCategory.Subscriptions | RavenTestCategory.BackupExportImport)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task RestoreFromBackupKeepsAllSubscriptionSettings(Options options)
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");

        using var store = GetDocumentStore(options);
        await CreateSubscriptionWithAllSettingsAsync(store);

        var backupTaskId = await Backup.CreateAndRunBackupAsync(store, options.DatabaseMode, backupPath);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "John" });
            await session.SaveChangesAsync();
        }

        var configuration = await Backup.RunBackupAndCreateRestoreConfigurationAsync(store, options.DatabaseMode, backupTaskId, backupPath);

        // one backup folder per shard; the document changed a single shard, so only that one has an incremental backup
        var expectedFolders = options.DatabaseMode == RavenDatabaseMode.Single ? 1 : (await Sharding.GetShardingConfigurationAsync(store)).Shards.Count;
        AssertBackupFiles(backupPath, Constants.Documents.PeriodicBackup.FullBackupExtension, expectedFolders, expectedIncrementalBackups: 1);

        using (Backup.RestoreDatabase(store, configuration))
        {
            var state = await store.Subscriptions.GetSubscriptionStateAsync(SubscriptionName, configuration.DatabaseName);
            Assert.True(state.Disabled);
            Assert.Equal(ArchivedDataProcessingBehavior.ArchivedOnly, state.ArchivedDataProcessingBehavior);
            Assert.Equal("A", state.MentorNode);
            Assert.True(state.PinToMentorNode);
        }
    }

    [RavenTheory(RavenTestCategory.Subscriptions | RavenTestCategory.BackupExportImport)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SnapshotRestoreKeepsAllSubscriptionSettings(bool withIncrementalBackup)
    {
        var backupPath = NewDataPath(suffix: "BackupFolder");

        using var store = GetDocumentStore();
        await CreateSubscriptionWithAllSettingsAsync(store);

        var config = Backup.CreateBackupConfiguration(backupPath, BackupType.Snapshot, incrementalBackupFrequency: "0 0 1 1 *");
        var taskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store);

        if (withIncrementalBackup)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "John" });
                await session.SaveChangesAsync();
            }

            await Backup.RunBackupAsync(Server, taskId, store, isFullBackup: false);
        }

        AssertBackupFiles(backupPath, Constants.Documents.PeriodicBackup.SnapshotExtension, expectedFolders: 1, expectedIncrementalBackups: withIncrementalBackup ? 1 : 0);

        var restoredDatabaseName = GetDatabaseName();
        using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
        {
            BackupLocation = Directory.GetDirectories(backupPath).First(),
            DatabaseName = restoredDatabaseName
        }))
        {
            var state = await store.Subscriptions.GetSubscriptionStateAsync(SubscriptionName, restoredDatabaseName);
            Assert.True(state.Disabled);
            Assert.Equal(ArchivedDataProcessingBehavior.ArchivedOnly, state.ArchivedDataProcessingBehavior);
            Assert.Equal("A", state.MentorNode);
            Assert.True(state.PinToMentorNode);
        }
    }

    private const string SubscriptionName = "AllSettings";

    private static void AssertBackupFiles(string backupPath, string fullBackupExtension, int expectedFolders, int expectedIncrementalBackups)
    {
        var folders = Directory.GetDirectories(backupPath);
        Assert.Equal(expectedFolders, folders.Length);

        var incrementalBackups = 0;
        foreach (var folder in folders)
        {
            var files = Directory.GetFiles(folder);
            Assert.Single(files, file => Path.GetExtension(file) == fullBackupExtension);
            incrementalBackups += files.Count(file => Path.GetExtension(file) == Constants.Documents.PeriodicBackup.IncrementalBackupExtension);
        }

        Assert.Equal(expectedIncrementalBackups, incrementalBackups);
    }

    private static async Task CreateSubscriptionWithAllSettingsAsync(IDocumentStore store)
    {
        await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
        {
            Name = SubscriptionName,
            Query = "from Users",
            MentorNode = "A",
            PinToMentorNode = true,
            Disabled = true,
            ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior.ArchivedOnly
        });
    }
}

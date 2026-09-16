using FastTests;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Quill;
using Raven.Quill.Infrastructure;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class RavenStoreFactoryTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task EnsureDatabase_creates_config_database_with_prevent_deletes_lock()
    {
        var store = GetDocumentStore();
        var name = "quill-config-" + Guid.NewGuid().ToString("N");

        try
        {
            var status = await RavenStoreFactory.EnsureDatabaseAsync(store, name, DatabaseLockMode.PreventDeletesError);

            Assert.True(status.Created);
            Assert.Equal(DatabaseLockMode.PreventDeletesError, await GetLockModeAsync(store, name));
        }
        finally
        {
            await UnlockAndDeleteAsync(store, name);
        }
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task EnsureDatabase_leaves_per_app_database_unlocked_by_default()
    {
        var store = GetDocumentStore();
        var name = "per-app-" + Guid.NewGuid().ToString("N");
        using var _ = Databases.EnsureDatabaseDeletion(name, store);

        var status = await RavenStoreFactory.EnsureDatabaseAsync(store, name);

        Assert.True(status.Created);

        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(name));
        Assert.Equal(DatabaseLockMode.Unlock, record.LockMode);

        // the never-unload setting belongs to the config database alone; application databases keep the
        // server's defaults, so they are still released when they go idle
        Assert.DoesNotContain(Constants.RavenSettings.MaxIdleTimeInSec, record.Settings.Keys);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task EnsureDatabase_creates_config_database_that_is_never_unloaded_when_idle()
    {
        var store = GetDocumentStore();
        var name = "quill-config-" + Guid.NewGuid().ToString("N");

        try
        {
            await RavenStoreFactory.EnsureDatabaseAsync(store, name, DatabaseLockMode.PreventDeletesError, RavenStoreFactory.ConfigDatabaseSettings);

            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(name));
            Assert.Equal(Constants.RavenSettings.NeverIdle, record.Settings[Constants.RavenSettings.MaxIdleTimeInSec]);

            // the setting is only worth anything if the server reads '-1' as "never": the config database is
            // the only record of which databases are Quill applications, and the cluster observer can only
            // read that list while it is loaded.
            var database = await GetDocumentDatabaseInstanceForAsync(name);
            Assert.Equal(TimeSpan.MaxValue, database.Configuration.Databases.MaxIdleTime.AsTimeSpan);
        }
        finally
        {
            await UnlockAndDeleteAsync(store, name);
        }
    }

    private static async Task<DatabaseLockMode> GetLockModeAsync(IDocumentStore store, string database)
    {
        var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database));
        Assert.NotNull(record);
        return record.LockMode;
    }

    private static async Task UnlockAndDeleteAsync(IDocumentStore store, string database)
    {
        // teardown can't delete a PreventDeletes db — unlock first
        await store.Maintenance.Server.SendAsync(new SetDatabasesLockOperation(database, DatabaseLockMode.Unlock));
        await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(database, hardDelete: true));
    }
}

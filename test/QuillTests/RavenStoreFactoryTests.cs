using FastTests;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
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
            var created = await RavenStoreFactory.EnsureDatabaseAsync(store, name, DatabaseLockMode.PreventDeletesError);

            Assert.True(created);
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

        var created = await RavenStoreFactory.EnsureDatabaseAsync(store, name);

        Assert.True(created);
        Assert.Equal(DatabaseLockMode.Unlock, await GetLockModeAsync(store, name));
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

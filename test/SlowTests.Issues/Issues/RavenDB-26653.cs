using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26653 : RavenTestBase
{
    public RavenDB_26653(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task GetAllStoragesEnvironment_must_include_SharedJournals_env()
    {
        using var store = GetDocumentStore(new Options
        {
            RunInMemory = false
        });

        await new Users_ByName().ExecuteAsync(store);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "Joe" });
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        Assert.NotNull(database.IndexStore.SharedJournals);

        var envs = database.GetAllStoragesEnvironment().ToList();

        var sharedJournals = envs.Where(x => x.Type == StorageEnvironmentWithType.StorageEnvironmentType.SharedJournals).ToList();
        Assert.Single(sharedJournals);

        var entry = sharedJournals[0];
        Assert.Equal(Raven.Server.Config.Categories.IndexingConfiguration.SharedJournalsStorageName, entry.Name);
        Assert.Same(database.IndexStore.SharedJournals.Env, entry.Environment);
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task GetAllStoragesEnvironment_must_not_include_SharedJournals_env_when_disabled()
    {
        using var store = GetDocumentStore(new Options
        {
            RunInMemory = false,
            ModifyDatabaseRecord = r =>
                r.Settings[RavenConfiguration.GetKey(c => c.Indexing.DisableSharedJournals)] = "true"
        });

        await new Users_ByName().ExecuteAsync(store);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "Joe" });
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        Assert.Null(database.IndexStore.SharedJournals);

        var envs = database.GetAllStoragesEnvironment().ToList();
        Assert.DoesNotContain(envs, x => x.Type == StorageEnvironmentWithType.StorageEnvironmentType.SharedJournals);
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task Backup_type_list_must_not_include_SharedJournals_env()
    {
        using var store = GetDocumentStore(new Options
        {
            RunInMemory = false
        });

        await new Users_ByName().ExecuteAsync(store);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "Joe" });
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        Assert.NotNull(database.IndexStore.SharedJournals);

        var backupTypes = new List<StorageEnvironmentWithType.StorageEnvironmentType>
        {
            StorageEnvironmentWithType.StorageEnvironmentType.Index,
            StorageEnvironmentWithType.StorageEnvironmentType.Documents,
            StorageEnvironmentWithType.StorageEnvironmentType.Configuration,
        };

        var envs = database.GetAllStoragesEnvironment(backupTypes).ToList();
        Assert.DoesNotContain(envs, x => x.Type == StorageEnvironmentWithType.StorageEnvironmentType.SharedJournals);
    }

    private class Users_ByName : AbstractIndexCreationTask<User>
    {
        public Users_ByName()
        {
            Map = users => from u in users select new { u.Name };
        }
    }
}

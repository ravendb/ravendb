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

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task Branch_index_env_journals_must_be_marked_as_hard_links_and_excluded_from_aggregated_size()
    {
        using var store = GetDocumentStore(new Options
        {
            RunInMemory = false
        });

        await new Users_ByName().ExecuteAsync(store);

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 100; i++)
                await session.StoreAsync(new User { Name = "user-" + i });
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        Assert.NotNull(database.IndexStore.SharedJournals);

        var rootEnv = database.IndexStore.SharedJournals.Env;
        Assert.All(rootEnv.Journal.Files, j => Assert.False(j.IsHardLinked,
            $"@SharedJournals root must not have hard-linked journals; journal #{j.Number} reported IsHardLinked=true"));

        var rootReport = rootEnv.GenerateSizeReport(includeTempBuffers: false);
        Assert.Equal(0, rootReport.HardLinkedJournalsInBytes);
        Assert.True(rootReport.JournalsInBytes > 0, "expected root env to have non-empty journals");

        var indexes = database.IndexStore.GetIndexes().ToList();
        Assert.NotEmpty(indexes);

        long branchHardLinkedTotal = 0;
        long branchJournalsTotal = 0;
        foreach (var index in indexes)
        {
            var branchEnv = index._indexStorage.Environment();

            Assert.All(branchEnv.Journal.Files, j => Assert.True(j.IsHardLinked,
                $"index '{index.Name}' branch env journal #{j.Number} must be hard-linked while shared journals enabled"));

            var branchReport = branchEnv.GenerateSizeReport(includeTempBuffers: false);
            Assert.Equal(branchReport.JournalsInBytes, branchReport.HardLinkedJournalsInBytes);
            branchJournalsTotal += branchReport.JournalsInBytes;
            branchHardLinkedTotal += branchReport.HardLinkedJournalsInBytes;
        }

        Assert.True(branchHardLinkedTotal > 0, "expected branch envs to have hard-linked journals");
        Assert.Equal(branchJournalsTotal, branchHardLinkedTotal);

        var totalsAfterDedup = database.GetSizeOnDisk();

        long expectedJournalsContribution = rootReport.JournalsInBytes;
        long branchDataFiles = indexes.Sum(i => i._indexStorage.Environment().GenerateSizeReport(includeTempBuffers: false).DataFilePhysicalSizeInBytes);
        long docsAndConfigDataFiles = 0;
        long docsAndConfigJournals = 0;
        foreach (var env in database.GetAllStoragesEnvironment(new List<StorageEnvironmentWithType.StorageEnvironmentType>
                 {
                     StorageEnvironmentWithType.StorageEnvironmentType.Documents,
                     StorageEnvironmentWithType.StorageEnvironmentType.Configuration
                 }))
        {
            var r = env.Environment.GenerateSizeReport(includeTempBuffers: false);
            docsAndConfigDataFiles += r.DataFilePhysicalSizeInBytes;
            docsAndConfigJournals += r.JournalsInBytes - r.HardLinkedJournalsInBytes;
        }

        var expectedPhysical = rootReport.DataFilePhysicalSizeInBytes
                               + expectedJournalsContribution
                               + branchDataFiles
                               + docsAndConfigDataFiles
                               + docsAndConfigJournals;

        Assert.Equal(expectedPhysical, totalsAfterDedup.Physical.SizeInBytes);

        long naiveJournalsSum = rootReport.JournalsInBytes + branchJournalsTotal + docsAndConfigJournals;
        long dedupedJournalsSum = rootReport.JournalsInBytes + docsAndConfigJournals;
        Assert.True(dedupedJournalsSum < naiveJournalsSum,
            $"de-duped journals total ({dedupedJournalsSum}) should be smaller than the naive sum ({naiveJournalsSum})");
    }

    private class Users_ByName : AbstractIndexCreationTask<User>
    {
        public Users_ByName()
        {
            Map = users => from u in users select new { u.Name };
        }
    }
}

using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Voron.Global;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25762 : RavenTestBase
{
    public RavenDB_25762(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task Compaction_should_ignore_errored_index()
    {
        using (var store = GetDocumentStore(new Options
               {
                   RunInMemory = false,
                   ModifyDatabaseRecord = databaseRecord =>
                   {
                       databaseRecord.Settings[RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "false";
                   }
        }))
        {
            var index1 = new Users_ByLastName();
            await store.ExecuteIndexAsync(index1);

            var index2 = new Users_ByName();
            await store.ExecuteIndexAsync(index2);

            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 10; i++)
                {
                    await session.StoreAsync(new User{ Name = "abc" + i});
                }

                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store, allowErrors: true);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var index1Path = database.IndexStore.GetIndex(index1.IndexName)._environment.Options.BasePath;

            Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

            // corrupting index data so we won't be able to open index hence compaction should skip it without erroring whole operation
            IOExtensions.DeleteFile(index1Path.Combine(Constants.DatabaseFilename).FullPath);
            IOExtensions.DeleteDirectory(index1Path.Combine("Journals").FullPath);

            var compactOperation = store.Maintenance.Server.Send(new CompactDatabaseOperation(new CompactSettings
            {
                DatabaseName = store.Database,
                Documents = true,
                Indexes = new[] { index1.IndexName, index2.IndexName }
            }));

            var result = await compactOperation.WaitForCompletionAsync<CompactionResult>(TimeSpan.FromSeconds(60));

            Assert.True(result.IndexesResults[index1.IndexName].Skipped);
            Assert.Contains($"Skipping data compaction of '{index1.IndexName}' index because of encountered error", result.IndexesResults[index1.IndexName].Message);
        }
    }

    private class Users_ByLastName : AbstractIndexCreationTask<User>
    {
        public Users_ByLastName()
        {
            Map = users => from user in users
                select new
                {
                    user.LastName
                };
        }
    }

    private class Users_ByName : AbstractIndexCreationTask<User>
    {
        public Users_ByName()
        {
            Map = users => from user in users
                select new
                {
                    Name = user.Name
                };
        }
    }
}

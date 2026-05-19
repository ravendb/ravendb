using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26654 : RavenTestBase
{
    public RavenDB_26654(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task DisableSharedJournals_true_must_not_create_dedicated_env()
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

        string sharedJournalsPath = database.Configuration.Indexing.SharedJournalsPath.FullPath;
        Assert.False(Directory.Exists(sharedJournalsPath),
            $"@SharedJournals directory must not exist when DisableSharedJournals=true, but found: {sharedJournalsPath}");
    }

    private class Users_ByName : AbstractIndexCreationTask<User>
    {
        public Users_ByName()
        {
            Map = users => from u in users select new { u.Name };
        }
    }
}

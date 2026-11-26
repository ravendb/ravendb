using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_28121 : RavenTestBase
{
    public RavenDB_28121(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task Idle_index_stats_write_must_not_bump_SharedJournals_LastWorkTime()
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
        var index = database.IndexStore.GetIndexes().Single();
        var indexEnv = index._indexStorage.Environment();
        var sharedJournalsEnv = database.IndexStore.SharedJournals.Env;

        var indexTxIdBefore = indexEnv.CurrentReadTransactionId;
        var lastWorkTimeBefore = sharedJournalsEnv.LastWorkTime;

        // what the idle indexing loop writes every Indexing.ElapsedSinceQueriedPersistIntervalInMin
        index._indexStorage.WriteElapsedSinceQueried(TimeSpan.FromMinutes(1));

        Assert.True(indexEnv.CurrentReadTransactionId > indexTxIdBefore, "the stats write must commit on the index env");
        Assert.Equal(lastWorkTimeBefore, sharedJournalsEnv.LastWorkTime);
    }

    private class Users_ByName : AbstractIndexCreationTask<User>
    {
        public Users_ByName()
        {
            Map = users => from u in users select new { u.Name };
        }
    }
}

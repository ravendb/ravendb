using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Voron.Issues;

// End-to-end companion of RavenDB_27156 (originally RavenDB-24520): a torn journal write on the
// shared @SharedJournals root faults every index sharing the journal, then the database must self-heal on
// reload with no document loss. Before the recovery fix this intermittently crashed the process with an
// AccessViolation (the least-flushed branch recovered with an undersized data pager).
public class RavenDB_27156_e2e(ITestOutputHelper output) : RavenTestBase(output)
{
    private class Item
    {
        public string Name { get; set; }
        public int Value { get; set; }
    }

    private static IndexDefinition MapIndex(string name) => new()
    {
        Name = name,
        Maps = { "from i in docs.Items select new { i.Name, i.Value }" }
    };

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task TornJournalWrite_OnSharedRoot_FaultsAllIndexes_ThenRecoversWithoutDataLoss()
    {
        using var store = GetDocumentStore(new Options
        {
            RunInMemory = false,
            ModifyDatabaseRecord = r =>
            {
                r.Settings[RavenConfiguration.GetKey(x => x.Storage.MaxJournalFileSize)] = "16";
            }
        });

        // three map indexes -> three branch envs sharing the root journal
        foreach (var n in new[] { "Idx/A", "Idx/B", "Idx/C" })
            await store.Maintenance.SendAsync(new PutIndexesOperation(MapIndex(n)));

        await InsertItems(store, 0, 500);
        Indexes.WaitForIndexing(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var shared = database.IndexStore.SharedJournals;
        Assert.NotNull(shared);

        var indexes = database.IndexStore.GetIndexes().ToList();
        Assert.All(indexes, idx => Assert.NotNull(idx._environment.Options.RootJournal));

        // arm a one-shot torn write on the shared root: write half the batch then throw
        var fired = 0;
        var injected = new IOException("RavenDB-27156 simulated torn journal write (disk full mid-write)");
        shared.Env.Options.ForTestingPurposesOnly().SimulatePartialJournalWriteFailure = total =>
        {
            if (Interlocked.CompareExchange(ref fired, 1, 0) != 0)
                return null;
            return new StorageEnvironmentOptions.TestingStuff.PartialJournalWriteFailure
            {
                NumberOf4KbsToWrite = total / 2,
                Error = injected
            };
        };

        // trigger a branch commit -> root merge write -> torn failure
        try
        {
            await InsertItems(store, 500, 200);
        }
        catch
        {
            // expected: the torn write faults the merged commit
        }

        // let the failure fault and unload the indexes before we force a reload
        await Task.Delay(TimeSpan.FromSeconds(10));

        shared.Env.Options.ForTestingPurposesOnly().SimulatePartialJournalWriteFailure = null;
        await Server.ServerStore.DatabasesLandlord.RestartDatabaseAsync(store.Database);

        await InsertItems(store, 700, 50); // a small post-recovery write must succeed
        Indexes.WaitForIndexing(store, timeout: TimeSpan.FromMinutes(2));

        long docCount;
        using (var session = store.OpenAsyncSession())
            docCount = await session.Query<Item>().CountAsync();

        var finalStats = await store.Maintenance.SendAsync(new GetIndexesStatisticsOperation());

        // self-heal: no document loss, every index back to Normal and fully re-indexed
        Assert.Equal(750, docCount);
        Assert.All(finalStats, s => Assert.Equal(IndexState.Normal, s.State));
        Assert.All(finalStats, s => Assert.Equal(docCount, s.EntriesCount));
    }

    private static async Task InsertItems(IDocumentStore store, int start, int count)
    {
        using var bulk = store.BulkInsert();
        for (int i = start; i < start + count; i++)
            await bulk.StoreAsync(new Item { Name = $"item-{i}", Value = i }, $"items/{i}");
    }
}

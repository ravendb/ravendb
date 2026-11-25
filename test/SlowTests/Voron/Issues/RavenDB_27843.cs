using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Tests.Infrastructure;
using Voron;
using Voron.Impl.FileHeaders;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_27843(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Indexes)]
    public async Task SharedJournalsMustKeepSyncingAfterTheOnlyIndexIsDeleted()
    {
        using var store = GetDocumentStore(new Options
        {
            RunInMemory = false,
            ModifyDatabaseRecord = r => r.Settings[RavenConfiguration.GetKey(x => x.Storage.MaxJournalFileSize)] = "1"
        });
        var shared = (await Databases.GetDocumentDatabaseInstanceFor(store)).IndexStore.SharedJournals.Env;

        // no flush or sync until Docs/A is deleted, so the first one deletes journals and syncs only its (already gone) Journals folder
        shared.Options.ManualFlushing = true;
        shared.Options.ManualSyncing = true;
        await IndexDocs(store, "Docs/A", 0);
        await store.Maintenance.SendAsync(new DeleteIndexOperation("Docs/A"));
        var committed = shared.CurrentReadTransactionId;
        Assert.True(await FlushAndSync(shared), "@SharedJournals sync after deleting Docs/A did not finish within 30 seconds");
        AssertSyncedUpTo(shared, committed);

        await IndexDocs(store, "Docs/B", 1);
        committed = shared.CurrentReadTransactionId;
        Assert.True(await FlushAndSync(shared), "@SharedJournals sync after indexing Docs/B did not finish within 30 seconds");
        AssertSyncedUpTo(shared, committed);
    }

    private async Task IndexDocs(IDocumentStore store, string index, int seed)
    {
        await store.Maintenance.SendAsync(new PutIndexesOperation(new IndexDefinition
        {
            Name = index, Maps = { "from d in docs.Docs select new { d.Text }" }, Fields = { ["Text"] = new IndexFieldOptions { Indexing = FieldIndexing.Search } }
        }));

        // several transactions -> several index batches -> the 1MB shared journals roll over
        var rnd = new Random(seed);
        for (int batch = 0; batch < 5; batch++)
        {
            using var s = store.OpenAsyncSession();
            for (int i = 0; i < 100; i++)
                await s.StoreAsync(new Doc { Text = string.Join(' ', Enumerable.Range(0, 300).Select(_ => rnd.Next().ToString("x"))) }, $"docs/{seed}/{batch}/{i}");
            await s.SaveChangesAsync();
            await Indexes.WaitForIndexingAsync(store);
        }
    }

    private static async Task<bool> FlushAndSync(StorageEnvironment env)
    {
        env.FlushLogToDataFile();
        var sync = Task.Run(env.SyncDataFileImmediately);
        return await WaitForValueAsync(() => sync.IsCompleted, true, timeout: 30_000);
    }

    private static void AssertSyncedUpTo(StorageEnvironment env, long committed)
    {
        var synced = LastSynced(env);
        Assert.True(synced >= committed, $"@SharedJournals synced only up to tx {synced}, but tx {committed} is committed");
    }

    private static long LastSynced(StorageEnvironment env) => env.HeaderAccessor.Get((in FileHeader h) => h.Journal.LastSyncedTransactionId);

    private class Doc
    {
        public string Text;
    }
}

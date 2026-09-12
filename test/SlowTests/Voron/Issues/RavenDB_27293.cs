using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Voron.Issues;

// A missing journal file in Indexes/@SharedJournals/Journals fails the WHOLE database load, because
// GetJournalFileInfo checks file presence unconditionally regardless of sync state. That is recoverable with
// Storage.Dangerous.IgnoreInvalidJournalErrors, but the shared root used to be opened with a bare
// `new StorageEnvironment(options)`, bypassing StorageLoader - so the operator got a raw Voron
// "No such journal" with no indication that a remedy exists. Found as RavenDB-24520 finding F-9.
public class RavenDB_27293(ITestOutputHelper output) : RavenTestBase(output)
{
    private class Item
    {
        public string Name { get; set; }
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task SharedJournalsRootMustReportTheRemedyWhenItsJournalIsMissing()
    {
        using var store = GetDocumentStore(new Options { RunInMemory = false });

        await store.Maintenance.SendAsync(new PutIndexesOperation(new IndexDefinition
        {
            Name = "Idx/A",
            Maps = { "from i in docs.Items select new { i.Name }" }
        }));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Item { Name = "a" }, "items/1");
            await session.SaveChangesAsync();
        }
        Indexes.WaitForIndexing(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        Assert.NotNull(database.IndexStore.SharedJournals);

        var rootEnv = database.IndexStore.SharedJournals.Env;
        string journalsDir = Path.Combine(rootEnv.Options.BasePath.FullPath, "Journals");

        // the journal recovery must start from - deleting a LATER one is tolerated as a torn tail
        long lastSynced = rootEnv.HeaderAccessor.CopyHeader().Journal.LastSyncedJournal;
        string victim = Path.Combine(journalsDir, StorageEnvironmentOptions.JournalName(lastSynced < 0 ? 0 : lastSynced));
        Output.WriteLine($"LastSyncedJournal={lastSynced}, deleting {Path.GetFileName(victim)}");

        await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, disable: true));

        Assert.True(File.Exists(victim), $"expected {victim} to exist before deleting it");
        await DeleteWithRetryAsync(victim); // the toggle returns before the unloaded database releases its handles

        // re-enabling already fails, because the toggle waits for the database to load - so both the toggle and
        // the explicit load have to be inside the assertion, whichever surfaces the failure first
        var e = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(store.Database, disable: false));
            await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
        });

        // ToString() carries the whole inner-exception and AggregateException chain
        string message = e.ToString();

        Assert.Contains("No such journal", message);
        Assert.Contains(RavenConfiguration.GetKey(x => x.Storage.IgnoreInvalidJournalErrors), message);
        Assert.Contains("shared index-journals storage", message);
    }

    private static async Task DeleteWithRetryAsync(string path)
    {
        for (int i = 0; ; i++)
        {
            try
            {
                File.Delete(path);
                return;
            }
            catch (IOException) when (i < 100)
            {
                await Task.Delay(100);
            }
        }
    }
}

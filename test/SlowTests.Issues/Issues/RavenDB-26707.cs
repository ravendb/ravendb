using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Sparrow.Server.Platform;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26707(ITestOutputHelper output) : RavenTestBase(output)
{
    // Toggle DisableSharedJournals ON -> OFF -> ON across restarts. OFF (standalone) journals survive back ON, but
    // the returning shared journals are renumbered higher, leaving a numbering gap above them. The pre-fix recovery cleanup scanned
    // contiguously down from LastSyncedJournal and stopped at the gap, so the synced journals below it leaked. Each phase forces the
    // index env's sync to advance LastSyncedJournal (the background sync won't fire in a fast test). Asserts no journal below
    // LastSyncedJournal survives after recovery, and that the journals a branch finds at startup are retired by the branch itself
    // once synced instead of waiting for the next recovery.
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Indexes)]
    public async Task Standalone_era_journals_must_be_cleaned_after_returning_to_shared_mode()
    {
        var disableKey = RavenConfiguration.GetKey(x => x.Indexing.DisableSharedJournals);
        var maxJournalKey = RavenConfiguration.GetKey(x => x.Storage.MaxJournalFileSize);
        const string dbName = nameof(Standalone_era_journals_must_be_cleaned_after_returning_to_shared_mode);
        string dataDirectory;
        string indexDataPath = null;

        var sharedOn = new Dictionary<string, string> { [disableKey] = "false", [maxJournalKey] = "4" };
        var sharedOff = new Dictionary<string, string> { [disableKey] = "true", [maxJournalKey] = "4" };

        // Phase 1 (ON): seed + index a Lucene index, then force the index env's sync.
        using (var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, CustomSettings = sharedOn }))
        {
            using var store = new DocumentStore { Urls = new[] { server.WebUrl }, Database = dbName }.Initialize();
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new Raven.Client.ServerWide.DatabaseRecord(dbName)));
            await new ByName().ExecuteAsync(store);
            await StoreDocs(store, 0, 5_000);
            Indexes.WaitForIndexing(store, databaseName: dbName);
            await ForceIndexSync(server, store, dbName);

            dataDirectory = server.Configuration.Core.DataDirectory.FullPath;
            indexDataPath = Path.Combine(dataDirectory, "Databases", dbName, "Indexes", nameof(ByName));
            Output.WriteLine($"P1 (ON):  {await DescribeBranch(server, store, dbName, indexDataPath)}");
        }

        // Phase 2 (OFF, standalone): write the index's own journals; capture them.
        long[] standaloneEra;
        using (var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, DataDirectory = dataDirectory, CustomSettings = sharedOff }))
        {
            using var store = new DocumentStore { Urls = new[] { server.WebUrl }, Database = dbName }.Initialize();
            await WaitForDatabaseStatsAsync(store, TimeSpan.FromMinutes(21));
            await StoreDocs(store, 5_000, 12_000);
            Indexes.WaitForIndexing(store, databaseName: dbName);
            await ForceIndexSync(server, store, dbName);
            standaloneEra = ListJournalNumbers(indexDataPath);
            Output.WriteLine($"P2 (OFF): {await DescribeBranch(server, store, dbName, indexDataPath)}");
        }
        Output.WriteLine($"standalone-era journals captured: [{string.Join(",", standaloneEra)}]");

        // Phase 3 (ON, branch): standalone-era journals survive; the returning shared journals are renumbered higher, opening a
        // numbering gap above them - the synced journals below that gap are what must be reclaimed.
        long p3EndJournal;
        using (var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, DataDirectory = dataDirectory, CustomSettings = sharedOn }))
        {
            using var store = new DocumentStore { Urls = new[] { server.WebUrl }, Database = dbName }.Initialize();
            await WaitForDatabaseStatsAsync(store, TimeSpan.FromMinutes(21));
            await StoreDocs(store, 12_000, 30_000);
            Indexes.WaitForIndexing(store, databaseName: dbName);
            await ForceIndexSync(server, store, dbName);
            Output.WriteLine($"P3 (ON):  {await DescribeBranch(server, store, dbName, indexDataPath)}");

            // P3 in-run: standalone-era journals are now below LastSyncedJournal; they must be retired at runtime, not left for P4.
            var p3OnDisk = ListJournalNumbers(indexDataPath);
            var p3Survivors = standaloneEra.Where(p3OnDisk.Contains).ToArray();
            Assert.True(p3Survivors.Length == 0,
                $"standalone-era journals must be retired at runtime; survivors: [{string.Join(",", p3Survivors)}] (onDisk=[{string.Join(",", p3OnDisk)}])");

            // leave the index with a synced journal and a newer unflushed one, so P4 starts with two journals: stop the background
            // flusher for this env (it would sync the tail and retire the older journal before the shutdown) and write until the index rolls
            var p3Env = await GetIndexEnv(server, store, dbName);
            p3Env.Options.ManualFlushing = true;
            var p3LastJournal = p3OnDisk.Max();
            for (int from = 30_000; ListJournalNumbers(indexDataPath).Max() == p3LastJournal; from += 1_000)
            {
                Assert.True(from < 130_000, $"the index did not roll past journal {p3LastJournal}");
                await StoreDocs(store, from, from + 1_000);
                Indexes.WaitForIndexing(store, databaseName: dbName);
            }
            p3EndJournal = ListJournalNumbers(indexDataPath).Max();

            // the leak needs the root to still hold its links when P4 recovers the index (a journal the root already retired is a
            // plain file that recovery tracks anyway), and the root's own sync after its recovery races the index's recovery for that.
            // Pin the journals with an extra link of our own, so the index sees them hard-linked no matter who wins.
            var pinned = Directory.CreateDirectory(Path.Combine(indexDataPath, "pinned")).FullName;
            foreach (var journal in Directory.GetFiles(Path.Combine(indexDataPath, "Journals"), "*.journal"))
            {
                var rc = Pal.rvn_hard_link_non_durable(journal, Path.Combine(pinned, Path.GetFileName(journal)), out var errorCode);
                Assert.True(rc == PalFlags.FailCodes.Success, $"could not pin {journal}: {rc} errno={errorCode}");
            }
            Output.WriteLine($"P3 end:   {await DescribeBranch(server, store, dbName, indexDataPath)}");
        }

        // Phase 4 (restart ON): branch recovery must reclaim every journal below LastSyncedJournal, and once the sync after the
        // recovery advances it past the journals the branch started with, those must go too.
        using (var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, DataDirectory = dataDirectory, CustomSettings = sharedOn }))
        {
            using var store = new DocumentStore { Urls = new[] { server.WebUrl }, Database = dbName }.Initialize();
            await WaitForDatabaseStatsAsync(store, TimeSpan.FromMinutes(21));
            Indexes.WaitForIndexing(store, databaseName: dbName);

            var branchEnv = await GetIndexEnv(server, store, dbName);

            // wait until LastSyncedJournal reached the last journal the index had at shutdown (the first flush and sync may run
            // before we get here, or need the forced sync), and the sync writes the header before it deletes the journals
            long lsj;
            long[] onDisk, belowLsj;
            var sw = Stopwatch.StartNew();
            do
            {
                branchEnv.ForceSyncDataFile();
                await Task.Delay(250);
                lsj = branchEnv.Journal.GetCurrentJournalInfo().LastSyncedJournal;
                onDisk = ListJournalNumbers(indexDataPath);
                belowLsj = onDisk.Where(n => n < lsj).ToArray();
            } while ((lsj < p3EndJournal || belowLsj.Length > 0) && sw.Elapsed < TimeSpan.FromSeconds(60));
            var standaloneSurvivors = standaloneEra.Where(onDisk.Contains).ToArray();

            var diag = $"p3EndJournal={p3EndJournal}, lsj={lsj}, onDisk=[{string.Join(",", onDisk)}], standaloneEra=[{string.Join(",", standaloneEra)}], " +
                       $"belowLsj=[{string.Join(",", belowLsj)}], standaloneSurvivors=[{string.Join(",", standaloneSurvivors)}]";
            Output.WriteLine("P4 (ON):  " + diag);

            Assert.True(lsj >= p3EndJournal && belowLsj.Length == 0,
                "the branch must retire every journal below LastSyncedJournal once the sync after the recovery passed the journals it started with: " + diag);
        }
    }

    private static async Task StoreDocs(IDocumentStore store, int from, int to)
    {
        using var bulk = store.BulkInsert();
        for (int i = from; i < to; i++)
            await bulk.StoreAsync(new Item { Id = $"items/{i}", Name = $"name-{i % 50}" });
    }

    private async Task<StorageEnvironment> GetIndexEnv(RavenServer server, IDocumentStore store, string dbName)
    {
        var database = await Databases.GetDocumentDatabaseInstanceFor(server, store, dbName);
        return database.IndexStore.GetIndex(nameof(ByName))._environment;
    }

    // Index envs aren't ManualFlushing, so FlushLogToDataFile() is unavailable. The background flusher applies
    // the journal; ForceSyncDataFile() then syncs + advances LastSyncedJournal. Poll until it advances.
    private async Task ForceIndexSync(RavenServer server, IDocumentStore store, string dbName)
    {
        var env = await GetIndexEnv(server, store, dbName);
        var before = env.Journal.GetCurrentJournalInfo().LastSyncedJournal;
        var sw = Stopwatch.StartNew();
        while (sw.Elapsed < TimeSpan.FromSeconds(30))
        {
            env.ForceSyncDataFile();
            await Task.Delay(250);
            if (env.Journal.GetCurrentJournalInfo().LastSyncedJournal > before)
                return;
        }
        // LSJ did not advance within the budget - leave it; diagnostics will show the resulting layout.
    }

    private async Task<string> DescribeBranch(RavenServer server, IDocumentStore store, string dbName, string indexDataPath)
    {
        long lsj = -2;
        try { lsj = (await GetIndexEnv(server, store, dbName)).Journal.GetCurrentJournalInfo().LastSyncedJournal; }
        catch { /* ignore */ }
        return $"LSJ={lsj}, journals=[{string.Join(",", ListJournalNumbers(indexDataPath))}]";
    }

    private static long[] ListJournalNumbers(string indexDataPath)
    {
        var jdir = Path.Combine(indexDataPath, "Journals");
        if (Directory.Exists(jdir) == false)
            return Array.Empty<long>();
        return Directory.EnumerateFiles(jdir, "*.journal")
            .Select(p => long.Parse(Path.GetFileNameWithoutExtension(p)))
            .OrderBy(n => n)
            .ToArray();
    }

    private static async Task<DatabaseStatistics> WaitForDatabaseStatsAsync(IDocumentStore store, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        Exception lastError = null;
        while (sw.Elapsed < timeout)
        {
            try { return await store.Maintenance.SendAsync(new GetStatisticsOperation()); }
            catch (Exception e) { lastError = e; }
            await Task.Delay(500);
        }
        throw new TimeoutException($"Database did not respond within {timeout}. Last: {lastError?.GetType().Name}: {lastError?.Message}");
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class ByName : AbstractIndexCreationTask<Item>
    {
        public ByName()
        {
            Map = items => from i in items select new { i.Name };
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Lucene;
        }
    }
}

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
    // LastSyncedJournal survives after recovery.
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
        }

        // Phase 4 (restart ON): branch recovery must reclaim every journal below LastSyncedJournal.
        using (var server = GetNewServer(new ServerCreationOptions { RunInMemory = false, DeletePrevious = false, DataDirectory = dataDirectory, CustomSettings = sharedOn }))
        {
            using var store = new DocumentStore { Urls = new[] { server.WebUrl }, Database = dbName }.Initialize();
            await WaitForDatabaseStatsAsync(store, TimeSpan.FromMinutes(21));
            Indexes.WaitForIndexing(store, databaseName: dbName);

            var branchEnv = await GetIndexEnv(server, store, dbName);
            var lsj = branchEnv.Journal.GetCurrentJournalInfo().LastSyncedJournal;
            var onDisk = ListJournalNumbers(indexDataPath);
            var belowLsj = onDisk.Where(n => n < lsj).ToArray();
            var standaloneSurvivors = standaloneEra.Where(onDisk.Contains).ToArray();

            var diag = $"reopenLsj={lsj}, onDisk=[{string.Join(",", onDisk)}], standaloneEra=[{string.Join(",", standaloneEra)}], " +
                       $"belowLsj=[{string.Join(",", belowLsj)}], standaloneSurvivors=[{string.Join(",", standaloneSurvivors)}]";
            Output.WriteLine("P4 (ON):  " + diag);

            Assert.True(belowLsj.Length == 0,
                "branch recovery must reclaim every synced journal (below LastSyncedJournal); survivors below a gap: " + diag);
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

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config.Settings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_24649(ITestOutputHelper output) : RavenTestBase(output)
{
    private static readonly int Timeout = (int)TimeSpan.FromMinutes(1).TotalMilliseconds;
    private const int Interval = 10;
    
    [RavenFact(RavenTestCategory.Indexes)]
    public async Task ElapsedSinceQueriedTimeIsPersistedAndIncreased()
    {
        string autoIndexName;
        string dbName;
        using var store = GetDocumentStore(new Options(){RunInMemory = false});
        dbName = store.Database;
        var db = await GetDatabase(dbName);
        db.Configuration.Indexing.ElapsedSinceQueriedPersistInterval = new TimeSetting(250, TimeUnit.Milliseconds);

        using (var session = store.OpenAsyncSession(database: dbName))
        {
            await session.StoreAsync(new Orders.Order() { Company = ":)" });
            await session.SaveChangesAsync();
            _ = await session.Query<Orders.Order>()
                .Customize(x => x.WaitForNonStaleResults())
                .Statistics(out var stats)
                .Where(x => x.Company == ":)")
                .ToListAsync();
            autoIndexName = stats.IndexName;
        }


        var index = db.IndexStore.GetIndex(autoIndexName);

        // Wait that elapsed time will be increased.
        var elapsed1 = index.GetElapsedTimeFromLastQuery();
        var elapsed2 = await WaitAndAssertForGreaterThanAsync(async () => await GetElapsedTimeFromLastQuery(), elapsed1, timeout: Timeout, interval: Interval);

        using (var session = store.OpenAsyncSession())
        {
            // Reset the elapsed time by querying the index.
            _ = await session.Query<Orders.Order>()
                .Where(x => x.Company == ":)")
                .ToListAsync();
            
            // Wait for changed elapsed time.
            var currentElapsed = await WaitForNotEqualsAsync(async () => await GetElapsedTimeFromLastQuery(), elapsed2, timeout: Timeout, interval: Interval);
            Assert.True(currentElapsed != elapsed2, $"{currentElapsed} != {elapsed2}");
        }

        db = null;
        index = null;
        await store.Maintenance.ForDatabase(dbName).SendAsync(new DisableIndexOperation(autoIndexName));
        var result = await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(dbName, disable: true));
        
        Assert.True(result.Disabled, DisableDatabaseToggleResultToString(result));
        result = await store.Maintenance.Server.SendAsync(new ToggleDatabasesStateOperation(dbName, disable: false));
        Assert.False(result.Disabled, DisableDatabaseToggleResultToString(result));

        db = await GetDatabase(dbName);
        index = db.IndexStore.GetIndex(autoIndexName);
        db.Configuration.Indexing.ElapsedSinceQueriedPersistInterval = new TimeSetting(2, TimeUnit.Seconds);
        await store.Maintenance.ForDatabase(dbName).SendAsync(new EnableIndexOperation(autoIndexName));
        var elapsedOnInit = index.GetElapsedTimeFromLastQuery();
        
        await GetElapsedTimeFromLastQuery(); // force update the index. 
        await Indexes.WaitForIndexingAsync(store, dbName);
        var lastQueryTime = await WaitAndAssertForLessThanAsync(() => Task.FromResult(index.GetLastQueryingTime()!.Value), index.LastIndexingTime!.Value, timeout: Timeout, interval: Interval);
        Assert.True(lastQueryTime < index.LastIndexingTime, $"{lastQueryTime} < {index.LastIndexingTime}"); // 
        
        // Wait that elapsed time will be increased.
        var val = await WaitAndAssertForGreaterThanAsync(() => Task.FromResult((index.LastIndexingTime - index.GetLastQueryingTime())!.Value), elapsedOnInit, timeout: Timeout, interval: Interval);
        Assert.True(val > elapsedOnInit, $"{val} > {elapsedOnInit}");

        async Task<TimeSpan> GetElapsedTimeFromLastQuery()
        {
            using var session = store.OpenAsyncSession(database: dbName);
            await session.StoreAsync(new Orders.Order());
            await session.SaveChangesAsync();
            await Indexes.WaitForIndexingAsync(store, dbName);
            return index.GetElapsedTimeFromLastQuery();
        }
    }

    private static string DisableDatabaseToggleResultToString(DisableDatabaseToggleResult result) =>
        $"Disabled: {result.Disabled} | Success: {result.Success} | Reason: {result.Reason ?? string.Empty} | Name: {result.Name ?? string.Empty}";

    [RavenFact(RavenTestCategory.Querying)]
    public async Task ElapsedTimeWillBePersistedInBackup()
    {
        var backupPath = NewDataPath(forceCreateDir: true);
        var fullBackupPath = Path.Combine(backupPath, "RavenDB_24649.ravendb-snapshot");
        ExtractFile(fullBackupPath);
        using (var store = GetDocumentStore())
        {
            var databaseName = GetDatabaseName();
            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration { BackupLocation = backupPath, DatabaseName = databaseName }))
            {
                var db = await GetDatabase(databaseName);
                var indexNames = await store.Maintenance.ForDatabase(databaseName).SendAsync(new GetIndexesOperation(0, 2));
                Assert.Equal(1, indexNames.Length);
                var indexName = indexNames.First().Name;
                
                var index = db.IndexStore.GetIndex(indexName);
                index.Enable();

                await Indexes.WaitForIndexingAsync(store, databaseName);
                
                var elapsed = index.GetElapsedTimeFromLastQuery();
                var lastQueriedTime = index.GetLastQueryingTime();
                var lastIndexingTime = await WaitAndAssertForGreaterThanAsync(() => Task.FromResult(index.LastIndexingTime!.Value), lastQueriedTime!.Value, timeout: Timeout, interval: Interval);
                
                Assert.True(lastQueriedTime < lastIndexingTime, $"{lastQueriedTime} < {lastIndexingTime}");
            }
        }

        void ExtractFile(string path)
        {

            using (var file = File.Create(path))
            using (var stream = typeof(RavenDB_22937).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_24649.RavenDB_24649.ravendb-snapshot"))
            {
                stream.CopyTo(file);
            }
        }
    }
}

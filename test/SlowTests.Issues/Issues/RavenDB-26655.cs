using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26655(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Indexes)]
    public async Task RoundTripDisableSharedJournalsMustPreserveIndexes()
    {
        var disableKey = RavenConfiguration.GetKey(x => x.Indexing.DisableSharedJournals);
        const string dbName = nameof(RoundTripDisableSharedJournalsMustPreserveIndexes);
        const int seedCount = 10_000;
        long preToggleDocs;
        string dataDirectory;
        string url;

        // Phase 1: shared journals enabled, populate.
        var settingsSharedOn = new Dictionary<string, string>
        {
            [disableKey] = "false"
        };
        using (var server = GetNewServer(new ServerCreationOptions
        {
            RunInMemory = false,
            DeletePrevious = false,
            CustomSettings = settingsSharedOn
        }))
        {
            url = server.WebUrl;
            using var store = new DocumentStore { Urls = new[] { url }, Database = dbName }.Initialize();
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new Raven.Client.ServerWide.DatabaseRecord(dbName)));

            await new ByName().ExecuteAsync(store);
            await new ByNameMapReduce().ExecuteAsync(store);
            await new ByValue().ExecuteAsync(store);
            await new ByValueMapReduce().ExecuteAsync(store);

            using (var bulk = store.BulkInsert())
            {
                for (int i = 0; i < seedCount; i++)
                    await bulk.StoreAsync(new Item { Id = $"items/{i}", Name = $"name-{i % 50}", Value = i });
            }
            Indexes.WaitForIndexing(store, databaseName: dbName);

            await PatchManyAsync(store, idsFrom: 0, count: 5_000, script: "this.Value = (this.Value || 0) + 1;");
            Indexes.WaitForIndexing(store, databaseName: dbName);

            preToggleDocs = (await store.Maintenance.SendAsync(new GetStatisticsOperation())).CountOfDocuments;
            Output.WriteLine($"phase1 preToggleDocs={preToggleDocs}");
            Assert.True(preToggleDocs >= seedCount);

            dataDirectory = server.Configuration.Core.DataDirectory.FullPath;
            url = server.WebUrl;
        }

        // Phase 2: same data dir, DisableSharedJournals=true.
        var settingsSharedOff = new Dictionary<string, string>
        {
            [disableKey] = "true",
            [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = url
        };
        using (GetNewServer(new ServerCreationOptions
        {
            RunInMemory = false,
            DeletePrevious = false,
            DataDirectory = dataDirectory,
            CustomSettings = settingsSharedOff
        }))
        {
            using var store = new DocumentStore { Urls = new[] { url }, Database = dbName }.Initialize();

            var afterSecondStart = await store.Maintenance.SendAsync(new GetStatisticsOperation());
            Output.WriteLine($"phase2 docs={afterSecondStart.CountOfDocuments}");
            Assert.Equal(preToggleDocs, afterSecondStart.CountOfDocuments);

            await PatchManyAsync(store, idsFrom: 0, count: 5_000, script: "this.Name = 'updated-' + ((this.Value || 0) % 50);");
            Indexes.WaitForIndexing(store, databaseName: dbName);
        }

        // Phase 3: same data dir, shared journals re-enabled. Check for corruption.
        var settingsSharedOnAgain = new Dictionary<string, string>
        {
            [disableKey] = "false",
            [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = url
        };
        using (GetNewServer(new ServerCreationOptions
        {
            RunInMemory = false,
            DeletePrevious = false,
            DataDirectory = dataDirectory,
            CustomSettings = settingsSharedOnAgain
        }))
        {
            using var store = new DocumentStore { Urls = new[] { url }, Database = dbName }.Initialize();

            var stats = await WaitForDatabaseStatsAsync(store, TimeSpan.FromMinutes(2));
            Output.WriteLine($"phase3 docs={stats.CountOfDocuments}");
            Assert.Equal(preToggleDocs, stats.CountOfDocuments);

            var faulty = stats.Indexes.Where(i => i.State == IndexState.Error || i.Type == IndexType.Faulty).ToList();
            Assert.True(faulty.Count == 0,
                $"Indexes in Error/Faulty state after toggle cycle: {string.Join(", ", faulty.Select(f => $"{f.Name}({f.State}/{f.Type})"))}");
        }
    }

    private static async Task PatchManyAsync(IDocumentStore store, int idsFrom, int count, string script)
    {
        var req = new PatchRequest { Script = script };
        for (int i = 0; i < count; i++)
            await store.Operations.SendAsync(new PatchOperation($"items/{idsFrom + i}", null, req));
    }

    private static async Task<DatabaseStatistics> WaitForDatabaseStatsAsync(IDocumentStore store, TimeSpan timeout)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        Exception lastError = null;
        DatabaseStatistics last = null;
        while (sw.Elapsed < timeout)
        {
            try
            {
                last = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                return last;
            }
            catch (Exception e)
            {
                lastError = e;
            }
            await Task.Delay(500);
        }
        throw new TimeoutException(
            $"Database did not respond to GetStatisticsOperation within {timeout}. Last error: {lastError?.GetType().Name}: {lastError?.Message}");
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int Value { get; set; }
    }

    private class ByName : AbstractIndexCreationTask<Item>
    {
        public ByName()
        {
            Map = items => from i in items select new { i.Name, i.Value };
        }
    }

    private class ByNameMapReduce : AbstractIndexCreationTask<Item, ByNameMapReduce.Result>
    {
        public class Result
        {
            public string Name { get; set; }
            public int Count { get; set; }
            public int Sum { get; set; }
        }

        public ByNameMapReduce()
        {
            Map = items => from i in items
                           select new Result { Name = i.Name, Count = 1, Sum = i.Value };
            Reduce = results => from r in results
                                group r by r.Name into g
                                select new Result { Name = g.Key, Count = g.Sum(x => x.Count), Sum = g.Sum(x => x.Sum) };
        }
    }

    private class ByValue : AbstractIndexCreationTask<Item>
    {
        public ByValue()
        {
            Map = items => from i in items select new { i.Value };
        }
    }

    private class ByValueMapReduce : AbstractIndexCreationTask<Item, ByValueMapReduce.Result>
    {
        public class Result
        {
            public int Bucket { get; set; }
            public int Count { get; set; }
        }

        public ByValueMapReduce()
        {
            Map = items => from i in items
                           select new Result { Bucket = i.Value % 100, Count = 1 };
            Reduce = results => from r in results
                                group r by r.Bucket into g
                                select new Result { Bucket = g.Key, Count = g.Sum(x => x.Count) };
        }
    }
}

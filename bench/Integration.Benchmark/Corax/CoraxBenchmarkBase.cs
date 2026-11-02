using System;
using System.IO;
using System.Linq;
using BenchmarkDotNet.Attributes;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;

namespace Integration.Benchmark.Corax;

[SimpleJob(warmupCount: 3, iterationCount: 10)]
[MemoryDiagnoser]
public abstract class CoraxBenchmarkBase
{
    [Params(100_000, 1_000_000)]
    public int NumberOfDocuments { get; set; }

    private RavenDbInstance _instance;

    protected IDocumentStore Store => _instance.Store;

    [GlobalSetup]
    public void GlobalSetup()
    {
        // Every case runs in its own process, so the server with its indexed database is kept on disk and built only once per size.
        var path = Path.Combine(@"D:\temp", "CoraxIntegrationBenchmark", NumberOfDocuments.ToString());
        var builtMarker = path + ".built";
        var isBuilt = File.Exists(builtMarker);
        if (isBuilt == false && Directory.Exists(path))
            Directory.Delete(path, recursive: true);

        _instance = new RavenDbInstance();
        _instance.InitializeDatabase(path);
        if (isBuilt == false)
        {
            using (var bulkInsert = Store.BulkInsert())
            {
                for (var i = 0; i < NumberOfDocuments; i++)
                    bulkInsert.Store(new Item { Tag = $"tag{i % 10}", Category = $"category{i % 1000}", Price = i, Name = $"name{i}" }, $"items/{i}");
            }

            new Items().Execute(Store);
            new BoostedItems().Execute(Store);
        }

        using (var session = Store.OpenSession())
        {
            session.Query<Item, Items>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(30))).Take(0).ToList();
            session.Query<Item, BoostedItems>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(30))).Take(0).ToList();
        }

        if (isBuilt == false)
            File.WriteAllText(builtMarker, string.Empty);
    }

    [GlobalCleanup]
    public void GlobalCleanup() => _instance.Dispose();

    private class Items : AbstractIndexCreationTask<Item>
    {
        public Items()
        {
            Map = items => from item in items
                select new { item.Tag, item.Category, item.Price, item.Name };
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class BoostedItems : AbstractIndexCreationTask<Item>
    {
        public BoostedItems()
        {
            Map = items => from item in items
                select new { item.Tag, item.Category, item.Price, item.Name }.Boost(item.Price % 10 == 0 ? 2 : 1);
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    protected class Item
    {
        public string Tag { get; set; }
        public string Category { get; set; }
        public int Price { get; set; }
        public string Name { get; set; }
    }
}

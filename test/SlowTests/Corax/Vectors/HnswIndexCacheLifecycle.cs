using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax.Vectors;

public class HnswIndexCacheLifecycle(ITestOutputHelper output) : RavenTestBase(output)
{
    private const int DocCount = 200;

    // Vector search must remain consistent across writes that mutate cached HNSW nodes.
    // After the upper-level node cache has been warmed at index open, updating documents whose
    // vectors participate in the graph must not leave the cache pointing at stale topology:
    // a subsequent query must return correct results, not throw and not read freed slots.
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void VectorSearchRemainsConsistentAfterUpdatingCachedDocuments()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.RunInMemory = false;
        options.Path = NewDataPath();
        options.ModifyDocumentStore = s => s.Conventions.MaxNumberOfRequestsPerSession = 10_000;
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            for (int i = 1; i <= DocCount; i++)
                session.Store(new Item { Id = $"items/{i}", Description = i % 2 == 0 ? "cat" : "bike" });
            session.SaveChanges();

            _ = session.Query<Item>()
                .VectorSearch(f => f.WithText(x => x.Description), factory => factory.ByText("cat"))
                .ToList();
            Indexes.WaitForIndexing(store);
        }

        // Force the index-open path to seed its upper-level node cache from the populated graph.
        var off = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: true));
        Assert.True(off.Success && off.Disabled);
        var on = store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: false));
        Assert.True(on.Success && on.Disabled == false);

        using (var session = store.OpenSession())
        {
            for (int i = 1; i <= DocCount / 2; i++)
                session.Load<Item>($"items/{i}").Description = "fish";
            session.SaveChanges();
            Indexes.WaitForIndexing(store);

            var result = session.Query<Item>()
                .VectorSearch(f => f.WithText(x => x.Description), factory => factory.ByText("cat"), numberOfCandidates: 8)
                .ToList();

            Assert.NotEmpty(result);
        }
    }

    // The per-field HNSW node cache must be available to readers immediately after the first
    // commit that populates the graph. A database that started empty and then ingested vector
    // data should serve subsequent queries through the cache without needing an index or
    // database restart to materialize it.
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void VectorCacheIsAvailableAfterFirstCommitWithoutRestart()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.RunInMemory = false;
        options.Path = NewDataPath();
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            for (int i = 1; i <= DocCount; i++)
                session.Store(new Item { Id = $"items/{i}", Description = i % 2 == 0 ? "cat" : "bike" });
            session.SaveChanges();

            _ = session.Query<Item>()
                .VectorSearch(f => f.WithText(x => x.Description), factory => factory.ByText("cat"))
                .ToList();
            Indexes.WaitForIndexing(store);
        }

        var database = Databases.GetDocumentDatabaseInstanceFor(store).Result;
        var indexName = database.IndexStore.GetIndexes().Single().Name;
        var index = database.IndexStore.GetIndex(indexName);

        var persistenceField = index.GetType()
            .GetField("IndexPersistence", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic);
        Assert.NotNull(persistenceField);
        var persistence = persistenceField.GetValue(index);
        Assert.NotNull(persistence);

        var cachesField = persistence.GetType()
            .GetField("_hnswCaches", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert.NotNull(cachesField);

        var caches = cachesField.GetValue(persistence) as System.Collections.IDictionary;
        Assert.True(caches != null && caches.Count > 0,
            "Per-field HNSW node cache must be materialized after the first commit that populates the graph.");
    }

    private sealed class Item
    {
        public string Id { get; set; }
        public string Description { get; set; }
    }
}

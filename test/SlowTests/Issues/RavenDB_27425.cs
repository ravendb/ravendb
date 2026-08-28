using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_27425 : RavenTestBase
{
    public RavenDB_27425(ITestOutputHelper output) : base(output)
    {
    }

    // must be >= 1 and not a multiple of 128, otherwise the reference batch counter cannot freeze between the checks
    private const int MappedCount = 5;
    private const int TailCount = 16_000;
    private const int RefDocBodyBytes = 8 * 1024;
    private const int FilteredOutCount = 10_000;

    // fewer than 128 documents, so the per-document counter alone never opens the check gate
    private const int FanoutDocsCount = 30;
    private const int FanoutPerDocument = 2_000;

    private const int SkippedItemsCount = 20_000;

    // MapBatchSize is checked on every CanContinueBatch call, so anything above 128 tombstones works
    private const int CleanupTombstonesCount = 2_048;

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Encryption)]
    public async Task ReferenceHandlingShouldRespectTransactionSizeLimit()
    {
        var dbName = Encryption.SetupEncryptedDatabase(out var certificates, out _);

        using (var store = GetDocumentStore(new Options
        {
            AdminCertificate = certificates.ServerCertificateForCommunication.Value,
            ClientCertificate = certificates.ServerCertificateForCommunication.Value,
            ModifyDatabaseName = _ => dbName,
            Path = NewDataPath(),
            ModifyDatabaseRecord = r =>
            {
                r.Settings[RavenConfiguration.GetKey(x => x.Indexing.EncryptedTransactionSizeLimit)] = "1";
                r.Encrypted = true;
            }
        }))
        {
            using (var session = store.OpenAsyncSession())
            {
                for (var i = 0; i < MappedCount; i++)
                    await session.StoreAsync(new Entity { RefDocId = $"RefDocs/ref-{i}", Field1 = "v" + i }, $"entities/{i}");

                await session.SaveChangesAsync();
            }

            var index = new Entities_ByRefDoc();
            await index.ExecuteAsync(store);
            await Indexes.WaitForIndexingAsync(store);

            await store.Maintenance.SendAsync(new StopIndexOperation(index.IndexName));

            var filler = new string('x', RefDocBodyBytes);
            await using (var bulk = store.BulkInsert())
            {
                for (var i = 0; i < MappedCount; i++)
                    await bulk.StoreAsync(new RefDoc { NestedId = "nested/" + i, Filler = filler }, $"RefDocs/ref-{i}");

                for (var i = 0; i < TailCount; i++)
                    await bulk.StoreAsync(new RefDoc { Filler = filler }, $"RefDocs/tail-{i}");
            }

            await store.Maintenance.SendAsync(new StartIndexOperation(index.IndexName));
            await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromMinutes(5));

            using (var session = store.OpenAsyncSession())
            {
                var count = await session.Query<Entities_ByRefDoc.Result, Entities_ByRefDoc>()
                    .Where(x => x.RefNestedId != null)
                    .CountAsync();

                Assert.Equal(MappedCount, count);
            }

            var referenceDetails = await GetReferenceRunDetails(store, index.IndexName);

            Assert.NotEmpty(referenceDetails);
            Assert.Contains(referenceDetails, x => x.BatchCompleteReason?.Contains("Reached transaction size limit") == true);
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task MapShouldRespectManagedAllocationsBatchLimitWhenMostDocumentsAreFilteredOut()
    {
        using (var store = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = r =>
            {
                r.Settings[RavenConfiguration.GetKey(x => x.Indexing.ManagedAllocationsBatchLimit)] = "1";
            }
        }))
        {
            var index = new FilteredEntities_ByField1();
            await index.ExecuteAsync(store);

            var filler = new string('x', RefDocBodyBytes);
            await using (var bulk = store.BulkInsert())
            {
                await bulk.StoreAsync(new FilteredEntity { ShouldIndex = true, Field1 = "first", Filler = filler }, "filtered/first");

                for (var i = 0; i < FilteredOutCount; i++)
                    await bulk.StoreAsync(new FilteredEntity { ShouldIndex = false, Filler = filler }, $"filtered/skip-{i}");

                await bulk.StoreAsync(new FilteredEntity { ShouldIndex = true, Field1 = "last", Filler = filler }, "filtered/last");
            }

            await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromMinutes(5));

            using (var session = store.OpenAsyncSession())
            {
                var count = await session.Query<FilteredEntity, FilteredEntities_ByField1>().CountAsync();

                Assert.Equal(2, count);
            }

            var mapDetails = await GetMapRunDetails(store, index.IndexName);

            Assert.NotEmpty(mapDetails);
            Assert.Contains(mapDetails, x => x.BatchCompleteReason?.Contains("Reached managed allocations limit") == true);
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task FanoutMapShouldRespectManagedAllocationsBatchLimitWhenBatchHasFewerThan128Documents()
    {
        using (var store = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = r =>
            {
                r.Settings[RavenConfiguration.GetKey(x => x.Indexing.ManagedAllocationsBatchLimit)] = "1";
            }
        }))
        {
            var index = new FanoutEntities_ByItem();
            await index.ExecuteAsync(store);

            await store.Maintenance.SendAsync(new StopIndexOperation(index.IndexName));

            await using (var bulk = store.BulkInsert())
            {
                for (var i = 0; i < FanoutDocsCount; i++)
                {
                    var items = Enumerable.Range(0, FanoutPerDocument)
                        .Select(j => $"{i:D4}-{j:D6}-" + new string('x', 100))
                        .ToList();

                    await bulk.StoreAsync(new FanoutEntity { Items = items }, $"fanouts/{i}");
                }
            }

            await store.Maintenance.SendAsync(new StartIndexOperation(index.IndexName));
            await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromMinutes(5));

            using (var session = store.OpenAsyncSession())
            {
                var count = await session.Query<FanoutEntities_ByItem.Result, FanoutEntities_ByItem>().CountAsync();

                Assert.Equal(FanoutDocsCount * FanoutPerDocument, count);
            }

            var mapDetails = await GetMapRunDetails(store, index.IndexName);

            Assert.NotEmpty(mapDetails);
            Assert.Contains(mapDetails, x => x.BatchCompleteReason?.Contains("Reached managed allocations limit") == true);
        }
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.CompareExchange)]
    public async Task CompareExchangeReferenceHandlingShouldRespectManagedAllocationsBatchLimit()
    {
        using (var store = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = r =>
            {
                r.Settings[RavenConfiguration.GetKey(x => x.Indexing.ManagedAllocationsBatchLimit)] = "2";
            }
        }))
        {
            using (var session = store.OpenAsyncSession())
            {
                for (var i = 0; i < MappedCount; i++)
                    await session.StoreAsync(new CmpEntity { CmpKey = $"refs/{i}", Field1 = "v" + i }, $"cmpEntities/{i}");

                await session.SaveChangesAsync();
            }

            var index = new CmpEntities_ByRefValue();
            await index.ExecuteAsync(store);
            await Indexes.WaitForIndexingAsync(store);

            await store.Maintenance.SendAsync(new StopIndexOperation(index.IndexName));

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                for (var i = 0; i < MappedCount; i++)
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue($"refs/{i}", "nested/" + i);

                await session.SaveChangesAsync();
            }

            for (var i = 0; i < SkippedItemsCount; i += 1000)
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    for (var j = i; j < i + 1000; j++)
                        session.Advanced.ClusterTransaction.CreateCompareExchangeValue($"tail/{j}", "x");

                    await session.SaveChangesAsync();
                }
            }

            await store.Maintenance.SendAsync(new StartIndexOperation(index.IndexName));
            await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromMinutes(5));

            using (var session = store.OpenAsyncSession())
            {
                var count = await session.Query<CmpEntities_ByRefValue.Result, CmpEntities_ByRefValue>()
                    .Where(x => x.RefValue != null)
                    .CountAsync();

                Assert.Equal(MappedCount, count);
            }

            var referenceDetails = await GetReferenceRunDetails(store, index.IndexName);

            Assert.NotEmpty(referenceDetails);
            Assert.Contains(referenceDetails, x => x.BatchCompleteReason?.Contains("Reached managed allocations limit") == true);
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task ReferenceTombstoneHandlingShouldRespectManagedAllocationsBatchLimit()
    {
        using (var store = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = r =>
            {
                r.Settings[RavenConfiguration.GetKey(x => x.Indexing.ManagedAllocationsBatchLimit)] = "2";
            }
        }))
        {
            using (var session = store.OpenAsyncSession())
            {
                for (var i = 0; i < MappedCount; i++)
                    await session.StoreAsync(new Entity { RefDocId = $"RefDocs/ref-{i}", Field1 = "v" + i }, $"entities/{i}");

                await session.SaveChangesAsync();
            }

            var index = new Entities_ByRefDoc();
            await index.ExecuteAsync(store);
            await Indexes.WaitForIndexingAsync(store);

            await store.Maintenance.SendAsync(new StopIndexOperation(index.IndexName));

            await using (var bulk = store.BulkInsert())
            {
                for (var i = 0; i < MappedCount; i++)
                    await bulk.StoreAsync(new RefDoc { NestedId = "nested/" + i }, $"RefDocs/ref-{i}");

                for (var i = 0; i < SkippedItemsCount; i++)
                    await bulk.StoreAsync(new RefDoc(), $"RefDocs/tail-{i}");
            }

            using (var session = store.OpenAsyncSession())
            {
                for (var i = 0; i < MappedCount; i++)
                    session.Delete($"RefDocs/ref-{i}");

                await session.SaveChangesAsync();
            }

            for (var i = 0; i < SkippedItemsCount; i += 1000)
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (var j = i; j < i + 1000; j++)
                        session.Delete($"RefDocs/tail-{j}");

                    await session.SaveChangesAsync();
                }
            }

            await store.Maintenance.SendAsync(new StartIndexOperation(index.IndexName));
            await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromMinutes(5));

            var referenceDetails = await GetReferenceRunDetails(store, index.IndexName);

            Assert.NotEmpty(referenceDetails);
            Assert.Contains(referenceDetails, x => x.BatchCompleteReason?.Contains("Reached managed allocations limit") == true);
        }
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Attachments)]
    public async Task TombstoneCleanupShouldRespectMapBatchSizeWhenTombstonesAreSkippedByType()
    {
        const string indexName = "AllDocs/ByField1";

        using (var store = GetDocumentStore(new Options
        {
            ModifyDatabaseRecord = r =>
            {
                r.Settings[RavenConfiguration.GetKey(x => x.Indexing.MapBatchSize)] = "128";
            }
        }))
        {
            await using (var bulk = store.BulkInsert())
            {
                for (var i = 0; i < CleanupTombstonesCount; i++)
                    await bulk.StoreAsync(new CounterEntity { Field1 = "v" + i }, $"counterEntities/{i}");
            }

            for (var i = 0; i < CleanupTombstonesCount; i += 1024)
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (var j = i; j < i + 1024; j++)
                        session.Advanced.Attachments.Store($"counterEntities/{j}", "a", new System.IO.MemoryStream(new byte[] { 1 }));

                    await session.SaveChangesAsync();
                }
            }

            await store.Maintenance.SendAsync(new PutIndexesOperation(new IndexDefinition
            {
                Name = indexName,
                Maps = { "from doc in docs select new { doc.Field1 }" }
            }));
            await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromMinutes(5));

            await store.Maintenance.SendAsync(new StopIndexOperation(indexName));

            for (var i = 0; i < CleanupTombstonesCount; i += 1024)
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (var j = i; j < i + 1024; j++)
                        session.Advanced.Attachments.Delete($"counterEntities/{j}", "a");

                    await session.SaveChangesAsync();
                }
            }

            await store.Maintenance.SendAsync(new StartIndexOperation(indexName));
            await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromMinutes(5));

            var indexInstance = (await GetDatabase(store.Database)).IndexStore.GetIndex(indexName);

            var cleanupDetails = indexInstance.GetIndexingPerformance()
                .Where(x => x.Details != null)
                .SelectMany(x => Flatten(x.Details))
                .Where(x => x.CleanupDetails != null)
                .Select(x => x.CleanupDetails)
                .ToList();

            Assert.Contains(cleanupDetails, x => x.BatchCompleteReason?.Contains("Reached maximum configured map batch size") == true);
        }
    }

    private async Task<List<ReferenceRunDetails>> GetReferenceRunDetails(IDocumentStore store, string indexName)
    {
        var indexInstance = (await GetDatabase(store.Database)).IndexStore.GetIndex(indexName);

        return indexInstance.GetIndexingPerformance()
            .Where(x => x.Details?.Operations != null)
            .SelectMany(x => x.Details.Operations)
            .Where(x => x.ReferenceDetails != null)
            .Select(x => x.ReferenceDetails)
            .ToList();
    }

    private async Task<List<MapRunDetails>> GetMapRunDetails(IDocumentStore store, string indexName)
    {
        var indexInstance = (await GetDatabase(store.Database)).IndexStore.GetIndex(indexName);

        return indexInstance.GetIndexingPerformance()
            .Where(x => x.Details?.Operations != null)
            .SelectMany(x => x.Details.Operations)
            .Where(x => x.MapDetails != null)
            .Select(x => x.MapDetails)
            .ToList();
    }

    private static IEnumerable<IndexingPerformanceOperation> Flatten(IndexingPerformanceOperation operation)
    {
        yield return operation;

        if (operation.Operations == null)
            yield break;

        foreach (var child in operation.Operations)
        {
            foreach (var descendant in Flatten(child))
                yield return descendant;
        }
    }

    private class Entity
    {
        public string Id { get; set; }
        public string RefDocId { get; set; }
        public string Field1 { get; set; }
    }

    private class RefDoc
    {
        public string Id { get; set; }
        public string NestedId { get; set; }
        public string Filler { get; set; }
    }

    private class FilteredEntity
    {
        public string Id { get; set; }
        public bool ShouldIndex { get; set; }
        public string Field1 { get; set; }
        public string Filler { get; set; }
    }

    private class FanoutEntity
    {
        public string Id { get; set; }
        public List<string> Items { get; set; }
    }

    private class CmpEntity
    {
        public string Id { get; set; }
        public string CmpKey { get; set; }
        public string Field1 { get; set; }
    }

    private class CounterEntity
    {
        public string Id { get; set; }
        public string Field1 { get; set; }
    }

    private class Entities_ByRefDoc : AbstractIndexCreationTask<Entity>
    {
        public class Result
        {
            public string RefNestedId { get; set; }
        }

        public Entities_ByRefDoc()
        {
            Map = entities => from entity in entities
                              let refDoc = LoadDocument<RefDoc>(entity.RefDocId, "RefDocs")
                              select new
                              {
                                  entity.Field1,
                                  entity.RefDocId,
                                  RefNestedId = refDoc == null ? null : refDoc.NestedId
                              };
        }
    }

    private class FilteredEntities_ByField1 : AbstractIndexCreationTask<FilteredEntity>
    {
        public FilteredEntities_ByField1()
        {
            Map = entities => from entity in entities
                              where entity.ShouldIndex
                              select new
                              {
                                  entity.Field1
                              };
        }
    }

    private class FanoutEntities_ByItem : AbstractIndexCreationTask<FanoutEntity>
    {
        public class Result
        {
            public string Item { get; set; }
        }

        public FanoutEntities_ByItem()
        {
            Map = entities => from entity in entities
                              from item in entity.Items
                              select new
                              {
                                  Item = item
                              };
        }
    }

    private class CmpEntities_ByRefValue : AbstractIndexCreationTask<CmpEntity>
    {
        public class Result
        {
            public string RefValue { get; set; }
        }

        public CmpEntities_ByRefValue()
        {
            Map = entities => from entity in entities
                              let refValue = LoadCompareExchangeValue<string>(entity.CmpKey)
                              select new
                              {
                                  entity.Field1,
                                  RefValue = refValue
                              };
        }
    }
}

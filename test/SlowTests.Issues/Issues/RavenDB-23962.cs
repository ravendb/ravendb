using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Tests.Infrastructure.Operations;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_23962(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenMultiplatformFact(RavenTestCategory.Indexes, RavenArchitecture.AllX64)]
    public async Task ReferencedCollectionsShouldWorkWithStalenessHandling()
    {
        const string collectionName = "Dtos";
        var referencedCollection = EmbeddingsHelper.GetEmbeddingDocumentCollectionName(collectionName);

        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var dto = new Dto() { Name = "computer" };
                session.Store(dto);
                session.SaveChanges();
                
                var aiTaskDone = Etl.WaitForEtlToComplete(store);
                var (configuration, _) = AddEmbeddingsGenerationTask(store);
                Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
                var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
                Assert.True(queriesWorkerRegistered);
                Assert.True(indexingWorkerRegistered);

                var result = session.Query<Dto>()
                    .Customize(c => c.WaitForNonStaleResults())
                    .Statistics(out var stats)
                    .VectorSearch(x => x
                        .WithText(d => d.Name)
                        .UsingTask(configuration.Identifier), 
                        factory => factory.ByText("fruit"), 0.75f).ToList();
                
                var resultEtag1 = stats.ResultEtag;
                
                Assert.Empty(result);
                
                var database = GetDatabase(store.Database).GetAwaiter().GetResult();
                var index = database.IndexStore.GetIndex(stats.IndexName);

                long lastProcessedReferenceEtag;
                using (index._indexStorage._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    lastProcessedReferenceEtag = index._indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceEtag(tx.InnerTransaction, collectionName, new CollectionName(referencedCollection));
                }
                
                var stopIndexOp = new StopIndexOperation(stats.IndexName);
                
                await store.Maintenance.SendAsync(stopIndexOp);
                
                dto.Name = "strawberry";
                session.SaveChanges();

                result = session.Query<Dto>()
                    .Statistics(out stats)
                    .VectorSearch(x => x
                        .WithText(d => d.Name)
                        .UsingTask(configuration.Identifier),
                    factory => factory.ByText("fruit"), 0.75f).ToList();
                
                var resultEtag2 = stats.ResultEtag;
                
                Assert.True(stats.IsStale);
                Assert.Empty(result);
                Assert.NotEqual(resultEtag1, resultEtag2);
                
                using (index._indexStorage._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var lastProcessedReferenceEtag2 =
                        index._indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceEtag(tx.InnerTransaction, collectionName,
                            new CollectionName(referencedCollection));

                    Assert.Equal(lastProcessedReferenceEtag, lastProcessedReferenceEtag2);
                }
                
                WaitForValue(() =>
                {
                    var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(stats.IndexName));

                    return staleness.StalenessReasons.Any(x => x.Contains("There are still some document references to process from collection '@embeddings/Dtos'"));
                }, true);

                var startIndexOp = new StartIndexOperation(stats.IndexName);
                await store.Maintenance.SendAsync(startIndexOp);
                
                await Indexes.WaitForIndexingAsync(store);

                result = session.Query<Dto>()
                    .Statistics(out stats)
                    .VectorSearch(x => x
                        .WithText(d => d.Name)
                        .UsingTask(configuration.Identifier),
                    factory => factory.ByText("fruit"), 0.75f).ToList();
                
                var resultEtag3 = stats.ResultEtag;
                
                Assert.False(stats.IsStale);
                Assert.Single(result);
                Assert.NotEqual(resultEtag2, resultEtag3);
                
                using (index._indexStorage._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var lastProcessedReferenceEtag3 = index._indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceEtag(tx.InnerTransaction, collectionName, new CollectionName(referencedCollection));
                    
                    Assert.NotEqual(lastProcessedReferenceEtag, lastProcessedReferenceEtag3);
                }
            }
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Indexes, RavenArchitecture.AllX64)]
    public async Task TombstonesAreProcessed()
    {
        const string collectionName = "Dtos";
        var referencedCollection = EmbeddingsHelper.GetEmbeddingDocumentCollectionName(collectionName);
        
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var dto = new Dto() { Id = "dtos/1", Name = "computer" };
                session.Store(dto);
                session.SaveChanges();
                
                var aiTaskDone = Etl.WaitForEtlToComplete(store);
            
                var (configuration, _) = AddEmbeddingsGenerationTask(store);
                Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
                var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
                Assert.True(queriesWorkerRegistered);
                Assert.True(indexingWorkerRegistered);
                
                _ = session.Query<Dto>()
                    .Customize(c => c.WaitForNonStaleResults())
                    .Statistics(out var stats)
                    .VectorSearch(x => x
                            .WithText(d => d.Name)
                            .UsingTask(configuration.Identifier), 
                        factory => factory.ByText("fruit"), 0.75f).ToList();
                
                var index = GetDatabase(store.Database).GetAwaiter().GetResult().IndexStore.GetIndex(stats.IndexName);
                
                var tombstones = index.GetLastProcessedTombstonesPerCollection(ITombstoneAware.TombstoneType.Documents);

                Assert.Equal(0, tombstones[collectionName]);
                Assert.Equal(0, tombstones[referencedCollection]);
                
                var embeddingDocId = EmbeddingsHelper.GetEmbeddingDocumentId(dto.Id);
                
                session.Delete(embeddingDocId);
                session.SaveChanges();
                
                await Indexes.WaitForIndexingAsync(store);
                
                tombstones = index.GetLastProcessedTombstonesPerCollection(ITombstoneAware.TombstoneType.Documents);

                Assert.Equal(0, tombstones[collectionName]);
                Assert.NotEqual(0, tombstones[referencedCollection]);
            }
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Indexes, RavenArchitecture.AllX64)]
    public async Task DeletesAreHandled()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var dto = new Dto() { Id = "dtos/1", Name = "strawberry" };
                session.Store(dto);
                session.SaveChanges();
                
                var aiTaskDone = Etl.WaitForEtlToComplete(store);
            
                var (configuration, _) = AddEmbeddingsGenerationTask(store);
                
                Assert.True(await aiTaskDone.WaitAsync(DefaultEtlTimeout));
                var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, configuration);
                Assert.True(queriesWorkerRegistered);
                Assert.True(indexingWorkerRegistered);
                
                var result = session.Query<Dto>()
                    .Statistics(out var stats)
                    .Customize(c => c.WaitForNonStaleResults())
                    .VectorSearch(x => x
                            .WithText(d => d.Name)
                            .UsingTask(configuration.Identifier), 
                        factory => factory.ByText("fruit"), 0.75f).ToList();
                
                Assert.Single(result);
                
                var stopIndexOp = new StopIndexOperation(stats.IndexName);
                
                await store.Maintenance.SendAsync(stopIndexOp);
                
                var embeddingDocId = EmbeddingsHelper.GetEmbeddingDocumentId(dto.Id);
                
                session.Delete(embeddingDocId);
                session.SaveChanges();
                
                _ = session.Query<Dto>()
                    .VectorSearch(x => x
                            .WithText(d => d.Name)
                            .UsingTask(configuration.Identifier), 
                        factory => factory.ByText("fruit"), 0.75f).ToList();

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(stats.IndexName));

                Assert.True(staleness.IsStale);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still some tombstone references to process from collection '@embeddings/Dtos'")));
                
                var startIndexOp = new StartIndexOperation(stats.IndexName);
                await store.Maintenance.SendAsync(startIndexOp);
                
                await Indexes.WaitForIndexingAsync(store);
                
                result = session.Query<Dto>()
                    .Customize(c => c.WaitForNonStaleResults())
                    .VectorSearch(x => x
                            .WithText(d => d.Name)
                            .UsingTask(configuration.Identifier), 
                        factory => factory.ByText("fruit"), 0.75f).ToList();
                
                Assert.Empty(result);
            }
        }
    }

    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}

using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.ServerWide.Context;
using SlowTests.Server.Documents.AI.Embeddings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23962(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenFact(RavenTestCategory.Indexes)]
    public void ReferencedCollectionsShouldWorkWithStalenessHandling()
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
            
                Assert.True(aiTaskDone.Wait(DefaultEtlTimeout));
                
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
                
                store.Maintenance.Send(stopIndexOp);
                
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
                    var lastProcessedReferenceEtag2 = index._indexStorage.ReferencesForDocuments.ReadLastProcessedReferenceEtag(tx.InnerTransaction, collectionName, new CollectionName(referencedCollection));
                    
                    Assert.Equal(lastProcessedReferenceEtag, lastProcessedReferenceEtag2);
                }
                
                var startIndexOp = new StartIndexOperation(stats.IndexName);
                
                store.Maintenance.Send(startIndexOp);
                
                Indexes.WaitForIndexing(store);

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

    private class Dto
    {
        public string Name { get; set; }
    }
}

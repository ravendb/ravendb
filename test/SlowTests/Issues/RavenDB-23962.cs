using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Indexes;
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
                    .Statistics(out var stats)
                    .VectorSearch(x => x
                        .WithText(d => d.Name)
                        .UsingTask(configuration.Identifier), 
                        factory => factory.ByText("fruit"), 0.75f).ToList();
                
                Assert.Empty(result);
                
                var stopIndexOp = new StopIndexOperation(stats.IndexName);
                
                store.Maintenance.Send(stopIndexOp);
                
                dto.Name = "strawberry";
                session.SaveChanges();

                result = session.Query<Dto>().Statistics(out stats).VectorSearch(x => x
                        .WithText(d => d.Name)
                        .UsingTask(configuration.Identifier),
                    factory => factory.ByText("fruit"), 0.75f).ToList();
                
                Assert.True(stats.IsStale);
                Assert.Empty(result);
                
                var startIndexOp = new StartIndexOperation(stats.IndexName);
                
                store.Maintenance.Send(startIndexOp);
                
                Indexes.WaitForIndexing(store);

                result = session.Query<Dto>().Statistics(out stats).VectorSearch(x => x
                        .WithText(d => d.Name)
                        .UsingTask(configuration.Identifier),
                    factory => factory.ByText("fruit"), 0.75f).ToList();
                
                Assert.False(stats.IsStale);
                Assert.Single(result);
            }
        }
    }

    private class Dto
    {
        public string Name { get; set; }
    }
}

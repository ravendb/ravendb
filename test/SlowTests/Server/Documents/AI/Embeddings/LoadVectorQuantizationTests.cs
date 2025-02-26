using System;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class LoadVectorQuantizationTests(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Querying | RavenTestCategory.Vector)]
    public void CanIndexAlreadyQuantizedVectorAndQueryItProperly_Int8()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession())
        {
            session.Store(new Dto { Name = "car" });
            session.SaveChanges();
        }

        var etl = Etl.WaitForEtlToComplete(store);
        RegisterAiIntegration(store, embeddingsPaths: ["Name"], targetQuantization: VectorEmbeddingType.Int8);
        etl.Wait(DefaultEtlTimeout);
        
        new Index().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            
            QueryTimings timings = null;
            var results = session.Query<Dto, Index>()
                .Customize(x => x.Timings(out timings))
                .VectorSearch(f => f.WithField(s => s.Name), v => v.ByText("car"))
                .ToList();
            Assert.Equal(1, results.Count);
            Assert.NotNull(timings);
            var usedSimilarityMethod = ((QueryInspectionNode)timings.QueryPlan).Parameters["SimilarityMethod"];
            Assert.Equal("CosineSimilarityI8", usedSimilarityMethod);
        }
    }
    
    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Querying | RavenTestCategory.Vector)]
    public void CanIndexAlreadyQuantizedVectorAndQueryItProperly_Int1()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession())
        {
            session.Store(new Dto { Name = "car" });
            session.SaveChanges();
        }

        var etl = Etl.WaitForEtlToComplete(store);
        RegisterAiIntegration(store, embeddingsPaths: ["Name"], targetQuantization: VectorEmbeddingType.Binary);
        etl.Wait(DefaultEtlTimeout);
        
        new Index().Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            
            QueryTimings timings = null;
            var results = session.Query<Dto, Index>()
                .Customize(x => x.Timings(out timings))
                .VectorSearch(f => f.WithField(s => s.Name), v => v.ByText("car"))
                .ToList();
            Assert.Equal(1, results.Count);
            Assert.NotNull(timings);
            var usedSimilarityMethod = ((QueryInspectionNode)timings.QueryPlan).Parameters["SimilarityMethod"];
            Assert.Equal("HammingDistance", usedSimilarityMethod);
        }
    }
    
    private class Index : AbstractIndexCreationTask<Dto>
    {
        public Index()
        {
            Map = dtos => dtos.Select(x => new { Name = LoadVector("localaitask", "Name") });
        }
    }

    private class Dto
    {
        public object Vector { get; set; }
        public string Name { get; set; }
    }
}

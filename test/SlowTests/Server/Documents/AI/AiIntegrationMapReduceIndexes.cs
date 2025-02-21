using System;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.ETL.Providers.AI;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI;

public class AiIntegrationMapReduceIndexes(ITestOutputHelper output) : AiIntegrationTestBase(output)
{
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void CanMapReduceExistingVectors()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        
        using (var session = store.OpenSession())
        {
            session.Store(new Dto() { Name = "John", Description = "car" });
            session.Store(new Dto() { Name = "John", Description = "lake" });
            session.SaveChanges();
        }
        new SimpleMapReduceIndex().Execute(store);
        
        var etlDone = Etl.WaitForEtlToComplete(store);
        RegisterAiIntegration(store, embeddingsPaths: ["Description"]);
        etlDone.Wait(TimeSpan.FromSeconds(10));
        Indexes.WaitForIndexing(store);
        using (var session = store.OpenSession())
        {
            var result = session.Query<SimpleMapReduceIndex.Result, SimpleMapReduceIndex>().VectorSearch(f => f.WithField(s => s.Vector),
                v => v.ByText("car")).ToList();
            Assert.Equal(1, result.Count);
        }        
    }
    
    private class Dto
    {
        public string Name { get; set; }
        public string Description { get; set; }
        public object Vector { get; set; }
    }

    private class SimpleMapReduceIndex : AbstractIndexCreationTask<Dto, SimpleMapReduceIndex.Result>
    {
        public SimpleMapReduceIndex()
        {
            Map = dtos => from dto in dtos
                select new Result() { Name = dto.Name, Vector = LoadVector("localaitask", "Description") };
            Reduce = results => from result in results
                group result by result.Name into g
                select new Result() { Name = g.Key, Vector = CreateVector(g.Select(p => p.Vector)) };
        }
        
        public class Result
        {
            public string Name { get; set; }
            public object Vector { get; set; }
        }
    }
}

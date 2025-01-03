using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23509 : RavenTestBase
{
    public RavenDB_23509(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void CanCreateVectorFieldFromTextInMapReducePart()
    {
        using var store = GetDocumentStoreWithDocuments(out var ids);
        new MapReduceTextual().Execute(store);
        Indexes.WaitForIndexing(store);
        var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: false);
        Assert.Null(errors);
        
        using var session = store.OpenSession();
        var results = session.Query<Result, MapReduceTextual>()
            .VectorSearch(f => f.WithField(s => s.Vector),
                v => v.ByText("dog"))
            .ToList();
        
        Assert.Equal(1, results.Count);
        Assert.Equal(ids["dog"], results[0].Id);
    }
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void CanCreateVectorFieldFromNumericalInMapReducePart()
    {
        using var store = GetDocumentStoreWithDocuments(out var ids);
        new MapReduceNumerical().Execute(store);
        Indexes.WaitForIndexing(store);
        var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: false);
        Assert.Null(errors);
        
        using var session = store.OpenSession();
        var results = session.Query<Result, MapReduceNumerical>()
            .VectorSearch(f => f.WithField(s => s.Vector),
                v => v.ByEmbedding([-0.1f, -0.2f]))
            .ToList();
        
        Assert.Equal(1, results.Count);
        Assert.Equal(ids["car"], results[0].Id);
    }

    private IDocumentStore GetDocumentStoreWithDocuments(out Dictionary<string, string> identifies)
    {
        var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        var dogObject = new Dto() { Text = "dog", VectorSingles = [0.1f, 0.2f] };
        var carObject = new Dto() { Text = "car", VectorSingles = [-0.1f, -0.2f] };
        session.Store(dogObject);
        session.Store(carObject);
        session.SaveChanges();

        identifies = new Dictionary<string, string>();
        identifies.Add("dog", session.Advanced.GetDocumentId(dogObject));
        identifies.Add("car", session.Advanced.GetDocumentId(carObject));
        
        return store;
    }
    
    private class Dto
    {
        public float[] VectorSingles { get; set; }
        public string Text { get; set; }
        public string Id { get; set; }
    }
    
    private class Result
    {
        public string Id { get; set; }
        public object Vector { get; set; }
    }

    private class MapReduceTextual : AbstractIndexCreationTask<Dto, Result>
    {
        public MapReduceTextual()
        {
            Map = dtos => from doc in dtos
                select new Result() { Id = doc.Id, Vector = doc.Text };
            
            Reduce = results => from result in results
                group result by result.Id into g
                select new Result()
                {
                    Id = g.Key, Vector = CreateVector(g.Select(x => (string)x.Vector).ToArray()) 
                };
        }
    }
    
    private class MapReduceNumerical : AbstractIndexCreationTask<Dto, Result>
    {
        public MapReduceNumerical()
        {
            Map = dtos => from doc in dtos
                select new Result() { Id = doc.Id, Vector = doc.VectorSingles };
            
            Reduce = results => from result in results
                group result by result.Id into g
                select new Result()
                {
                    Id = g.Key, Vector = CreateVector(g.Select(x => (float[])x.Vector).ToArray()) 
                };
            Vector(p => p.Vector, f => f.SourceEmbedding(VectorEmbeddingType.Single).DestinationEmbedding(VectorEmbeddingType.Int8));
        }
    }
}

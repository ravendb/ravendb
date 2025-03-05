using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Client.Exceptions.Documents.Indexes;
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
    public void MapReduceIndexTextualCreateVectorInReduceShouldThrow()
    {
        using var store = GetDocumentStoreWithDocuments(out _);
        var exception = Assert.Throws<IndexCompilationException>(() => new MapReduceTextualCreateVectorInReduce().Execute(store));
        Assert.Contains("The 'CreateVector' method is not supported in map-reduce indexes.", exception.Message);
    }
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void MapReduceIndexLoadVectorInReduceShouldThrow()
    {
        using var store = GetDocumentStoreWithDocuments(out _);
        var exception = Assert.Throws<IndexCompilationException>(() => new MapReduceLoadVectorInReduce().Execute(store));
        Assert.Contains("The 'LoadVector' method is not supported in map-reduce indexes.", exception.Message);
    }
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void MapReduceIndexTextualCreateVectorInMapShouldThrow()
    {
        using var store = GetDocumentStoreWithDocuments(out _);
        var exception = Assert.Throws<IndexCompilationException>(() => new MapReduceIndexWithCreateVectorInMap().Execute(store));
        Assert.Contains("'CreateMethod' and 'LoadVector' are not supported in the map of a map-reduce index.", exception.Message);
    }
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void MapReduceIndexLoadVectorInMapShouldThrow()
    {
        using var store = GetDocumentStoreWithDocuments(out _);
        var exception = Assert.Throws<IndexCompilationException>(() => new MapReduceLoadVectorInMap().Execute(store));
        Assert.Contains("'CreateMethod' and 'LoadVector' are not supported in the map of a map-reduce index.", exception.Message);
    }
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void MapReduceIndexJsLoadVectorInMapShouldThrow()
    {
        using var store = GetDocumentStoreWithDocuments(out _);
        var exception = Assert.Throws<IndexCreationException>(() => new MapReduceLoadVectorInMapJs().Execute(store));
        Assert.Contains("Vector fields are not supported for map-reduces indexes.", exception.Message);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void MapReduceIndexJsTextualCreateVectorInReduceShouldThrow()
    {
        using var store = GetDocumentStoreWithDocuments(out _);
        new MapReduceTextualCreateVectorInReduceJs().Execute(store);
        var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: true);
        Assert.NotNull(errors);
        Assert.Contains("'createVector' is not supported in  MapReduce indexes.", errors[0].Errors[0].Error);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void MapReduceIndexJsLoadVectorInReduceShouldThrow()
    {
        using var store = GetDocumentStoreWithDocuments(out _);
        new MapReduceLoadVectorInReduceJs().Execute(store);
        var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: true);
        Assert.NotNull(errors);
        Assert.Contains("'loadVector' is not supported in MapReduce indexes.", errors[0].Errors[0].Error);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void MapReduceIndexJsNumericalCreateVectorInReduceShouldThrow()
    {
        using var store = GetDocumentStoreWithDocuments(out _);
        new MapReduceNumericalCreateVectorInReduceJs().Execute(store);
        var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: true);
        Assert.NotNull(errors);
        Assert.Contains("'createVector' is not supported in  MapReduce indexes.", errors[0].Errors[0].Error);
    }
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void MapReduceIndexJsNumericalCreateVectorInMapShouldThrow()
    {
        using var store = GetDocumentStoreWithDocuments(out _);
        
        var exception = Assert.Throws<IndexCreationException>(() => new MapReduceNumericalCreateVectorInMapJs().Execute(store));
        Assert.Contains("Vector fields are not supported for map-reduces indexes.", exception.Message);
    }
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    private void MapReduceIndexNumericalCreateVectorInReduceShouldThrow()
    {
        using var store = GetDocumentStoreWithDocuments(out _);
        var exception = Assert.Throws<IndexCompilationException>(() => new MapReduceNumericalCreateVectorInReduce().Execute(store));
        Assert.Contains("The 'CreateVector' method is not supported in map-reduce indexes.", exception.Message);
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

    private class MapReduceTextualCreateVectorInReduceJs : AbstractJavaScriptIndexCreationTask
    {
        public MapReduceTextualCreateVectorInReduceJs()
        {
            Maps = [$@"map('Dtos', function (dto) {{
                return {{
                    Id: id(dto),
                    Vector: dto.Text
                }};
            }})"];
            
            Reduce = @"groupBy(x => ({ Id: x.Id }))
.aggregate(g => { 
    return {
        Id: g.key.Id,
        Vector: createVector(g.values.map(x => x.Vector))
    };
})";
        }
    }
    
    private class MapReduceTextualCreateVectorInReduce : AbstractIndexCreationTask<Dto, Result>
    {
        public MapReduceTextualCreateVectorInReduce()
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
    
    private class MapReduceNumericalCreateVectorInReduce : AbstractIndexCreationTask<Dto, Result>
    {
        public MapReduceNumericalCreateVectorInReduce()
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
    
    private class MapReduceNumericalCreateVectorInMapJs : AbstractJavaScriptIndexCreationTask
    {
        public MapReduceNumericalCreateVectorInMapJs()
        {
            Maps = [$@"map('Dtos', function (dto) {{
                return {{
                    Id: id(dto),
                    Vector: createVector(dto.VectorSingles)
                }};
            }})"];
            
            Reduce = @"groupBy(x => ({ Id: x.Id }))
.aggregate(g => { 
    return {
        Id: g.key.Id,
        Vector: g.values.map(x => x.Vector)
    };
})";

            Fields = new();
            Fields.Add("Vector", new IndexFieldOptions(){Vector = new()
            {
                SourceEmbeddingType = VectorEmbeddingType.Single,
                DestinationEmbeddingType = VectorEmbeddingType.Int8
            }});
        }
    }
    
    private class MapReduceNumericalCreateVectorInReduceJs : AbstractJavaScriptIndexCreationTask
    {
        public MapReduceNumericalCreateVectorInReduceJs()
        {
            Maps = [$@"map('Dtos', function (dto) {{
                return {{
                    Id: id(dto),
                    Vector: dto.VectorSingles
                }};
            }})"];
            
            Reduce = @"groupBy(x => ({ Id: x.Id }))
.aggregate(g => { 
    return {
        Id: g.key.Id,
        Vector: createVector(g.values.map(x => x.Vector))
    };
})";

            Fields = new();
            Fields.Add("Vector", new IndexFieldOptions(){Vector = new()
            {
                SourceEmbeddingType = VectorEmbeddingType.Single,
                DestinationEmbeddingType = VectorEmbeddingType.Int8
            }});
        }
    }

    private class MapReduceIndexWithCreateVectorInMap : AbstractIndexCreationTask<Dto, Result>
    {
        public MapReduceIndexWithCreateVectorInMap()
        {
            Map = dtos => from doc in dtos
                select new Result() { Id = doc.Id, Vector = CreateVector(doc.Text) };
            
            Reduce = results => from result in results
                group result by result.Id into g
                select new Result()
                {
                    Id = g.Key, Vector = g.Select(x => (string)x.Vector).ToArray()
                };
        }
    }
    
    // HERE START 
        private class MapReduceLoadVectorInReduceJs : AbstractJavaScriptIndexCreationTask
    {
        public MapReduceLoadVectorInReduceJs()
        {
            Maps = [$@"map('Dtos', function (dto) {{
                return {{
                    Id: id(dto),
                    Vector: dto.Text
                }};
            }})"];
            
            Reduce = @"groupBy(x => ({ Id: x.Id }))
.aggregate(g => { 
    return {
        Id: g.key.Id,
        Vector: loadVector('test', 'abc')
    };
})";
        }
    }
    
    private class MapReduceLoadVectorInReduce : AbstractIndexCreationTask<Dto, Result>
    {
        public MapReduceLoadVectorInReduce()
        {
            Map = dtos => from doc in dtos
                select new Result() { Id = doc.Id, Vector = doc.Text };
            
            Reduce = results => from result in results
                group result by result.Id into g
                select new Result()
                {
                    Id = g.Key, Vector = LoadVector("test", "abc") 
                };
        }
    }
    
    private class MapReduceLoadVectorInMap : AbstractIndexCreationTask<Dto, Result>
    {
        public MapReduceLoadVectorInMap()
        {
            Map = dtos => from doc in dtos
                select new Result() { Id = doc.Id, Vector = LoadVector("test", "abc") };
            
            Reduce = results => from result in results
                group result by result.Id into g
                select new Result()
                {
                    Id = g.Key, Vector = g.Select(x => x.Vector).ToArray()
                };
        }
    }
    
    private class MapReduceLoadVectorInMapJs : AbstractJavaScriptIndexCreationTask
    {
        public MapReduceLoadVectorInMapJs()
        {
            Maps = [$@"map('Dtos', function (dto) {{
                return {{
                    Id: id(dto),
                    Vector: loadVector('test', 'abc')
                }};
            }})"];
            
            Reduce = @"groupBy(x => ({ Id: x.Id }))
.aggregate(g => { 
    return {
        Id: g.key.Id,
        Vector: g.values.map(x => x.Vector)
    };
})";

            Fields = new();
            Fields.Add("Vector", new IndexFieldOptions(){Vector = new()
            {
                SourceEmbeddingType = VectorEmbeddingType.Single,
                DestinationEmbeddingType = VectorEmbeddingType.Int8
            }});
        }
    }
}

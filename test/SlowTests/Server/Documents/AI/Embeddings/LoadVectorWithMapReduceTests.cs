using System;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Server.Documents.ETL.Providers.AI;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class LoadVectorWithMapReduceTests(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void CreateVectorInMapReduceWillThrow()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

        var ex = Assert.Throws<IndexCompilationException>(() => new SimpleMapReduceIndex().Execute(store));
        Assert.Contains("The 'CreateVector' method is not supported in map-reduce indexes.", ex.Message);
    }
    
    private class SimpleMapReduceIndex : AbstractIndexCreationTask<GenerateEmbeddingsTests.Dto, SimpleMapReduceIndex.Result>
    {
        public SimpleMapReduceIndex()
        {
            Map = dtos => from dto in dtos
                          select new Result() { Name = dto.Name, Vector = LoadVector("Description", "localaitask") };
            Reduce = results => from result in results
                                group result by result.Name into g
                                select new Result() { Name = g.Key, Vector = CreateVector(g.Select(p => (float[])p.Vector)) };
        }

        public class Result
        {
            public string Name { get; set; }
            public object Vector { get; set; }
        }
    }
}

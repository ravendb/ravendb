using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.Embeddings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class RemovalTests(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenFact(RavenTestCategory.Ai | RavenTestCategory.Etl)]
    public void TaskRemoveEmbeddingsDocumentOfRemovedDocument()
    {
        using var store = GetDocumentStore();
        string id0;
        string id1;
        using (var session = store.OpenSession())
        {
            var dto = new Dto() { Name = "Maciej" };
            var dto2 = new Dto() { Name = "Car" };
            session.Store(dto);
            session.Store(dto2);
            session.SaveChanges();
            id0 = dto.Id;
            id1 = dto2.Id;
        }
        
        var etlWait = Etl.WaitForEtlToComplete(store);
        var (config, connectionString) = AddEmbeddingsGenerationTask(store, embeddingsPaths: [new EmbeddingPathConfiguration() { Path = "Name", ChunkingOptions = DefaultChunkingOptions }], embeddingsGenerationTaskName: "eg");
        Assert.True(etlWait.Wait(DefaultEtlTimeout));
        AssertEmbeddingsForPath(store, config, connectionString, "Name", ["Maciej"], id0);
        AssertEmbeddingsForPath(store, config, connectionString, "Name", ["Car"], id1);

        using (var session = store.OpenSession())
        {
            session.Delete(id0);
            session.SaveChanges();
        }
        
        etlWait.Reset();
        Assert.True(etlWait.Wait(DefaultEtlTimeout));
        using (var session = store.OpenSession())
        {
            var removedDocumentEmbeddings = EmbeddingsHelper.GetEmbeddingDocumentId(id0);
            var doc0Embeddings = session.Load<object>(removedDocumentEmbeddings);
            Assert.Null(doc0Embeddings);
        }
        
        AssertEmbeddingsForPath(store, config, connectionString, "Name", ["Car"], id1);
    }
    
    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}

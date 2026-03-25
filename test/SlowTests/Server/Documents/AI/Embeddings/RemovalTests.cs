using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI.Embeddings;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.AI.Embeddings;

public class RemovalTests(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenMultiplatformFact(RavenTestCategory.Ai | RavenTestCategory.Etl, RavenArchitecture.AllX64)]
    public async Task TaskRemoveEmbeddingsDocumentOfRemovedDocument()
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
        Assert.True(await etlWait.WaitAsync(DefaultEtlTimeout));
        var (queriesWorkerRegistered, indexingWorkerRegistered) = await WaitForEmbeddingsGenerationWorkerToRegisterAsync(store, config);
        Assert.True(queriesWorkerRegistered);
        Assert.True(indexingWorkerRegistered);
        AssertEmbeddingsForPath(store, config, connectionString, "Name", ["Maciej"], id0);
        AssertEmbeddingsForPath(store, config, connectionString, "Name", ["Car"], id1);

        using (var session = store.OpenSession())
        {
            session.Delete(id0);
            session.SaveChanges();
        }
        
        etlWait.Reset();
        Assert.True(await etlWait.WaitAsync(DefaultEtlTimeout));
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

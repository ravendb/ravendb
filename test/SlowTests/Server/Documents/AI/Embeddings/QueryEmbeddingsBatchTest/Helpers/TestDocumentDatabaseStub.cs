using Raven.Server.Config.Categories;

namespace SlowTests.Server.Documents.AI.Embeddings.QueryEmbeddingsBatchTest.Helpers;

public class TestDocumentDatabaseStub(AiConfiguration aiConfig = null)
{
    public string Name { get; set; } = "test-db";
    public AiConfiguration Configuration { get; } = aiConfig ?? new AiConfiguration
    {
        EmbeddingsGenerationTaskMaxBatchSize = 128,
        QueryEmbeddingsMaxBatchSize = 100,
        QueryEmbeddingsMaxConcurrentBatches = 4
    };
}

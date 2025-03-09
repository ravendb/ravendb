using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;

namespace SlowTests.Server.Documents.AI.Embeddings.QueryEmbeddingsBatchTest.Helpers;

public class TestDocumentDatabaseStub(AiConfiguration aiConfig = null)
{
    public string Name { get; set; } = "test-db";
    public AiConfiguration Configuration { get; } = aiConfig ?? new AiConfiguration
    {
        EmbeddingsGenerationTaskMaxBatchSize = 128,
        QueryEmbeddingsBatchTimeout = 200,
        QueryEmbeddingsMaxBatchSize = 100,
        QueryEmbeddingsBatchMaxRetries = 3,
        QueryEmbeddingsBatchRetryDelay = new TimeSetting(200, TimeUnit.Milliseconds),
        QueryEmbeddingsMaxConcurrentBatches = 4
    };
}

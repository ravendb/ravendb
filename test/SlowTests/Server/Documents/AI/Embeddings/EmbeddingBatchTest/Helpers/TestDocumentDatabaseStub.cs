using System.Threading;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;
using Raven.Server.Logging;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace SlowTests.Server.Documents.AI.Embeddings.EmbeddingBatchTest.Helpers;

public class TestDocumentDatabaseStub
{
    public string Name { get; } = "test-db";
    public AiConfiguration Configuration { get; }
    public RavenLogger Logger { get; }
    public CancellationToken DatabaseShutdown { get; }

    public TestDocumentDatabaseStub(AiConfiguration aiConfig = null)
    {
        Configuration = aiConfig ?? new AiConfiguration
        {
            EmbeddingsGenerationMaxBatchSize = 128,
            BatchTimeoutInMs = 200,
            MaxBatchSize = 100,
            MaxRetries = 3,
            RetryDelay = new TimeSetting(200, TimeUnit.Milliseconds),
            MaxConcurrentBatches = 4
        };

        Logger = RavenLogManager.Instance.CreateNullLogger();
        DatabaseShutdown = CancellationToken.None;
    }
}

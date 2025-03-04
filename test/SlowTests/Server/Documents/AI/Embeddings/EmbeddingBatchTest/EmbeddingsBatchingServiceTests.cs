using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.Embeddings.EmbeddingBatchTest;

public class EmbeddingsBatchingServiceTests : EmbeddingsGenerationTestBase
{
    const int OnnxDefaultEmbeddingSize = 384;
    public EmbeddingsBatchingServiceTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task GetEmbeddingAsync_ReturnsValidEmbedding()
    {
        // Create a document store and register AI integration
        using var store = GetDocumentStore();
        (_, AiConnectionString connection) = AddEmbeddingsGenerationTask(store);
        
        // Get the database instance
        var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
        
        // Create the batching service
        var batchService = new EmbeddingsBatchingService(database.AiIntegrations);
        var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connection.Identifier);
        
        // Act
        var embeddings = await batchService.GetEmbeddingAsync(aiConnectionStringIdentifier, ["Test text for embedding generation"], CancellationToken.None);
        
        // Assert
        for (var i = 0; i < embeddings.Count; i++)
            Assert.True(embeddings[i].Length == OnnxDefaultEmbeddingSize, $"Embedding should have {OnnxDefaultEmbeddingSize} dimensions, but result #{i} has '{embeddings[i].Length}' dimensions");
    }
    
    [RavenFact(RavenTestCategory.Ai)]
    public async Task GetEmbeddingAsync_MultipleCalls_ShouldBatch()
    {
        // Create a document store and register AI integration
        using var store = GetDocumentStore();
        (_, AiConnectionString connection) = AddEmbeddingsGenerationTask(store);
        
        // Get the database instance
        var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
        
        // Modify database configuration to use smaller batch timeout for testing
        database.Configuration.Ai.BatchTimeoutInMs = 50;
        
        // Create the batching service
        var batchService = new EmbeddingsBatchingService(database.AiIntegrations);
        var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connection.Identifier);

        // Get the batch worker for testing purposes
        await batchService.GetEmbeddingAsync(aiConnectionStringIdentifier, ["For worker initialization"], CancellationToken.None);
        var worker = batchService.ForTestingPurposesOnly().GetBatchWorker(aiConnectionStringIdentifier);

        int batchCount = 0;
        var allRequestsCompleted = new TaskCompletionSource<bool>();
        int processedRequestCount = 0;
        const int totalRequests = 5;

        worker.ForTestingPurposesOnly().AfterBatchFlushed += () =>
        {
            Interlocked.Increment(ref batchCount);

            // If no more requests in queue AND all our tasks received their results,
            // we can be confident all batches have been processed
            if (Interlocked.CompareExchange(ref processedRequestCount, 0, 0) == totalRequests)
                allRequestsCompleted.TrySetResult(true);
        };

        // Act - Call the service multiple times in quick succession
        var tasks = new List<Task<IList<ReadOnlyMemory<float>>>>();
        for (int i = 0; i < totalRequests; i++)
            tasks.Add(batchService.GetEmbeddingAsync(aiConnectionStringIdentifier, [$"Test text {i} for embedding generation"], CancellationToken.None).AsTask());

        // Track when all individual requests complete
        _ = Task.WhenAll(tasks).ContinueWith(_ =>
            Interlocked.Exchange(ref processedRequestCount, totalRequests));

        // Wait for both:
        // 1. All individual requests to complete
        // 2. Processing to finish and signal allRequestsCompleted
        await Task.WhenAll(
            Task.WhenAll(tasks),
            Task.WhenAny(allRequestsCompleted.Task, Task.Delay(TimeSpan.FromSeconds(10)))
        );

        // Assert
        Assert.True(batchCount == 1, $"All requests should have been processed in a single batch, but '{batchCount}' batches were processed");
        Assert.True(tasks.All(t => t.IsCompletedSuccessfully));

        for (var i = 0; i < tasks.Count; i++)
            for (var j = 0; j < tasks[i].Result.Count; j++)
                Assert.True(tasks[i].Result[j].Length == OnnxDefaultEmbeddingSize, $"Embedding should have {OnnxDefaultEmbeddingSize} dimensions, but result #{i} of task #{j} has '{tasks[i].Result[j].Length}' dimensions");
    }
    
    [RavenFact(RavenTestCategory.Ai)]
    public async Task GetEmbeddingAsync_InvalidConnectionString_ThrowsException()
    {
        // Create a document store and register AI integration
        using var store = GetDocumentStore();
        AddEmbeddingsGenerationTask(store);
        
        // Get the database instance
        var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
        
        // Create the batching service
        var batchService = new EmbeddingsBatchingService(database.AiIntegrations);
        
        // Use an invalid connection string identifier
        var invalidConnectionStringId = new AiConnectionStringIdentifier("non-existent-connection");
        
        // Act & Assert
        var ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await batchService.GetEmbeddingAsync(invalidConnectionStringId, ["Test text"], CancellationToken.None);
        });
        
        Assert.True(ex.Message.Contains("Couldn't find Embeddings Generation task"), 
            $"Exception message should indicate the connection string wasn't found, but got: {ex.Message}");
    }
}

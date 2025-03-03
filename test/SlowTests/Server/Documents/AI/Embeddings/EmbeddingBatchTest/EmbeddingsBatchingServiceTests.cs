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
        var embedding = await batchService.GetEmbeddingAsync(
            aiConnectionStringIdentifier, 
            "Test text for embedding generation", 
            CancellationToken.None);
        
        // Assert
        Assert.True(embedding.Length == OnnxDefaultEmbeddingSize, $"Embedding should have {OnnxDefaultEmbeddingSize} dimensions, but got {embedding.Length}");
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
        await batchService.GetEmbeddingAsync(aiConnectionStringIdentifier, "For worker initialization", CancellationToken.None);
        var worker = batchService.ForTestingPurposesOnly().GetBatchWorker(aiConnectionStringIdentifier);
        var batchCount = 0;
        worker.ForTestingPurposesOnly().AfterBatchProcessed += () => batchCount++;
        
        // Act - Call the service multiple times in quick succession
        var tasks = new List<Task<ReadOnlyMemory<float>>>();
        for (int i = 0; i < 5; i++)
        {
            tasks.Add(batchService.GetEmbeddingAsync(
                aiConnectionStringIdentifier, 
                $"Test text {i} for embedding generation", 
                CancellationToken.None).AsTask());
        }

        // Wait for all embeddings to be generated
        await Task.WhenAll(tasks);

        // Assert
        Assert.True(batchCount == 1, $"All embeddings should be batched into a single request, but got {batchCount} batches");
        Assert.True(tasks.All(t => t.IsCompletedSuccessfully), "All embedding generation tasks should complete successfully");
        Assert.True(tasks.All(t => t.Result.Length == OnnxDefaultEmbeddingSize), $"All embeddings should have {OnnxDefaultEmbeddingSize} dimensions");
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
            await batchService.GetEmbeddingAsync(
                invalidConnectionStringId, 
                "Test text", 
                CancellationToken.None);
        });
        
        Assert.True(ex.Message.Contains("Couldn't find Embeddings Generation task"), 
            $"Exception message should indicate the connection string wasn't found, but got: {ex.Message}");
    }
}

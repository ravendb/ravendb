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
        for (var i = 0; i < embeddings.Length; i++)
            Assert.True(embeddings[i].Length == OnnxDefaultEmbeddingSize, $"Embedding should have {OnnxDefaultEmbeddingSize} dimensions, but result #{i} has '{embeddings[i].Length}' dimensions");
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
        
        Assert.True(ex.Message.Contains("Couldn't find embedding generation service for connection string 'non-existent-connection'"), 
            $"Exception message should indicate the connection string wasn't found, but got: {ex.Message}");
    }
}

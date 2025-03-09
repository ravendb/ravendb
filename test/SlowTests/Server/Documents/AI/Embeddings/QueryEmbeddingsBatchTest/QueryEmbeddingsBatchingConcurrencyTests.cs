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

namespace SlowTests.Server.Documents.AI.Embeddings.QueryEmbeddingsBatchTest;

public class QueryEmbeddingsBatchingConcurrencyTests(ITestOutputHelper output) : EmbeddingsGenerationTestBase(output)
{
    [RavenFact(RavenTestCategory.Ai)]
    public async Task ConcurrentRequests_ShouldRespectMaxConcurrentBatches()
    {
        // Create a document store and register AI integration
        using var store = GetDocumentStore();
        (_, AiConnectionString connection) = AddEmbeddingsGenerationTask(store);

        // Get the database instance
        var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

        // Configure limited concurrency for test
        int maxConcurrentBatches = 2;
        database.Configuration.Ai.QueryEmbeddingsMaxConcurrentBatches = maxConcurrentBatches;
        database.Configuration.Ai.QueryEmbeddingsMaxBatchSize = 3; // Small batch size to create multiple batches

        // Create the batching service
        var batchService = new QueryEmbeddingsBatchingService(database.AiIntegrations);
        var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connection.Identifier);

        // Setup manual control for batch processing
        var batchCompletionEvents = new List<ManualResetEventSlim>();
        int currentlyProcessingBatches = 0;
        int maxObservedConcurrentBatches = 0;

        // Submit a first request to ensure worker is created
        await batchService.GetEmbeddingAsync(aiConnectionStringIdentifier, ["Initial request"], CancellationToken.None);

        // Get the worker and set up testing hooks
        var worker = batchService.ForTestingPurposesOnly().GetBatchWorker(aiConnectionStringIdentifier);
        var workerTestHooks = worker.ForTestingPurposesOnly();

        // Set up the callback to monitor concurrent batch processing
        // We'll create events that we can manually trigger to control when batches complete
        const int totalBatches = 5;
        for (int i = 0; i < totalBatches; i++)
            batchCompletionEvents.Add(new ManualResetEventSlim(false));

        int batchCounter = 0;
        workerTestHooks.AfterBatchFlushed = () =>
        {
            int current = Interlocked.Increment(ref currentlyProcessingBatches);

            // Track the maximum number of concurrent batches we've observed
            int prevMax;
            do
            {
                prevMax = maxObservedConcurrentBatches;
                if (current <= prevMax)
                    break;
            } while (Interlocked.CompareExchange(ref maxObservedConcurrentBatches, current, prevMax) != prevMax);

            try
            {
                // Signal that a batch has started and is in progress
                int currentBatch = Interlocked.Increment(ref batchCounter) - 1;
                if (currentBatch < totalBatches)
                {
                    // Wait until test code allows this batch to complete
                    batchCompletionEvents[currentBatch].Wait();
                }
            }
            finally
            {
                // Decrement the counter when batch completes
                Interlocked.Decrement(ref currentlyProcessingBatches);
            }
        };

        // Act - Submit many requests that will be processed in batches
        var tasks = new List<Task<ReadOnlyMemory<float>[]>>();
        for (int i = 0; i < totalBatches * database.Configuration.Ai.QueryEmbeddingsMaxBatchSize; i++)
            tasks.Add(batchService.GetEmbeddingAsync(aiConnectionStringIdentifier, [$"Concurrent test text {i}"], CancellationToken.None).AsTask());

        // Allow some time for batches to start processing
        await Task.Delay(500);

        // First verification - we should observe no more than maxConcurrentBatches at once
        Assert.True(maxObservedConcurrentBatches <= maxConcurrentBatches,
            $"Should not exceed {maxConcurrentBatches} concurrent batches, but observed {maxObservedConcurrentBatches}");

        // Now verify we actually have batches blocked and waiting
        Assert.True(currentlyProcessingBatches > 0,
            $"Should have batches currently processing, but found {currentlyProcessingBatches}");

        // Release all waiting batches
        foreach (var evt in batchCompletionEvents)
            evt.Set();

        // Wait for all requests to complete
        await Task.WhenAll(tasks);

        // Final assertions
        Assert.True(maxObservedConcurrentBatches <= maxConcurrentBatches,
            $"At no point should we have exceeded {maxConcurrentBatches} concurrent batches, but observed {maxObservedConcurrentBatches}");

        Assert.True(tasks.All(t => t.IsCompletedSuccessfully),
            "All embedding tasks should complete successfully");

        for (var i = 0; i < tasks.Count; i++)
            for (var j = 0; j < tasks[i].Result.Length; j++)
                Assert.True(tasks[i].Result[j].Length > 0, $"All embedding tasks should have positive lengths, but result #{i} of task #{j} has '{tasks[i].Result[j]}' dimensions");

        // Clean up
        foreach (var evt in batchCompletionEvents)
            evt.Dispose();
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task Request_Cancellation_ShouldThrow()
    {
        // Create a document store and register AI integration
        using var store = GetDocumentStore();
        (_, AiConnectionString connection) = AddEmbeddingsGenerationTask(store);

        // Get the database instance
        var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

        // Configure longer batch timeout to ensure our cancellation happens during batch formation
        database.Configuration.Ai.QueryEmbeddingsBatchTimeout = (int)TimeSpan.FromSeconds(10).TotalMilliseconds;

        // Create the batching service
        var batchService = new QueryEmbeddingsBatchingService(database.AiIntegrations);
        var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connection.Identifier);

        // Create cancellation token source that we'll cancel immediately
        using var cts = new CancellationTokenSource();

        // Act
        var task = batchService.GetEmbeddingAsync(aiConnectionStringIdentifier, ["Text that should never be processed"], cts.Token);

        // Cancel immediately
        await cts.CancelAsync();

        // Assert
        await Assert.ThrowsAsync<TaskCanceledException>(async () => await task.AsTask());
    }
}

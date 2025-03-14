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
        const int maxConcurrentBatches = 2;
        database.Configuration.Ai.QueryEmbeddingsMaxConcurrentBatches = maxConcurrentBatches;
        database.Configuration.Ai.QueryEmbeddingsMaxBatchSize = 3; // Small batch size to create multiple batches

        // Create the batching service
        var batchService = new QueryEmbeddingsBatchingService(database.AiIntegrations);
        var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connection.Identifier);

        // Setup manual control for batch processing
        var batchCompletionEvents = new List<ManualResetEventSlim>();
        int currentlyProcessingBatches = 0;
        int maxObservedConcurrentBatches = 0;

        // Add a hook invocation counter to verify the hook is being called
        int hookInvocationCount = 0;

        // Add an event to signal when the first batch starts processing
        var firstBatchProcessingStarted = new ManualResetEventSlim(initialState: false);

        // Submit a first request to ensure worker is created
        await batchService.GetEmbeddingAsync(aiConnectionStringIdentifier, ["Initial request"], CancellationToken.None);

        // Get the worker and set up testing hooks
        var worker = batchService.ForTestingPurposesOnly().GetBatchWorker(aiConnectionStringIdentifier);

        try
        {
            // Set up the callback to monitor concurrent batch processing
            // We'll create events that we can manually trigger to control when batches complete
            const int totalBatches = 5;
            for (int i = 0; i < totalBatches; i++)
                batchCompletionEvents.Add(new ManualResetEventSlim(false));

            int batchCounter = 0;
            worker.ForTestingPurposesOnly().AfterBatchProcessed = () =>
            {
                // Increment the hook invocation counter to verify the hook is being called
                Interlocked.Increment(ref hookInvocationCount);

                // Signal that at least one batch has started processing
                // ReSharper disable once AccessToDisposedClosure
                firstBatchProcessingStarted.Set();

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

            // Wait for the first batch to start processing
            var isBatchStarted = firstBatchProcessingStarted.Wait(TimeSpan.FromSeconds(30));

            Assert.True(isBatchStarted, "No batches started processing within the timeout period");
            Assert.True(hookInvocationCount > 0, "The AfterBatchFlushed hook should have been invoked, but wasn't. This suggests the hook implementation is missing or broken.");
            Assert.True(maxObservedConcurrentBatches <= maxConcurrentBatches, $"Should not exceed {maxConcurrentBatches} concurrent batches, but observed {maxObservedConcurrentBatches}");
            Assert.True(currentlyProcessingBatches > 0, $"Should have batches currently processing, but found {currentlyProcessingBatches}. This suggests the hook implementation isn't properly tracking concurrent batches.");

            // Release all waiting batches
            foreach (var evt in batchCompletionEvents)
                evt.Set();

            // Wait for all requests to complete
            await Task.WhenAll(tasks);

            Assert.True(maxObservedConcurrentBatches <= maxConcurrentBatches, $"At no point should we have exceeded {maxConcurrentBatches} concurrent batches, but observed {maxObservedConcurrentBatches}");
            Assert.True(tasks.All(t => t.IsCompletedSuccessfully),
                "All embedding tasks should complete successfully");

            for (var i = 0; i < tasks.Count; i++)
                for (var j = 0; j < tasks[i].Result.Length; j++)
                    Assert.True(tasks[i].Result[j].Length > 0, $"All embedding tasks should have positive lengths, but result #{i} of task #{j} has '{tasks[i].Result[j]}' dimensions");
        }
        finally // Clean up
        {
            foreach (var evt in batchCompletionEvents)
                evt.Dispose();

            firstBatchProcessingStarted.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task Request_Cancellation_ShouldThrow()
    {
        // Create a document store and register AI integration
        using var store = GetDocumentStore();
        (_, AiConnectionString connection) = AddEmbeddingsGenerationTask(store);

        // Get the database instance
        var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

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

    [RavenFact(RavenTestCategory.Ai)]
    public async Task NoWaitForBatchTimeout_ImmediateProcessing()
    {
        // Create a document store and register AI integration
        using var store = GetDocumentStore();
        (_, AiConnectionString connection) = AddEmbeddingsGenerationTask(store);

        // Get the database instance
        var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

        // Create the batching service
        var batchService = new QueryEmbeddingsBatchingService(database.AiIntegrations);
        var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connection.Identifier);

        // Submit a first request to ensure worker is created
        await batchService.GetEmbeddingAsync(aiConnectionStringIdentifier, ["Initial request"], CancellationToken.None);

        // Get the worker
        var worker = batchService.ForTestingPurposesOnly().GetBatchWorker(aiConnectionStringIdentifier);

        // Set up tracking to measure time to first processing
        var processingStarted = new ManualResetEventSlim(false);

        worker.ForTestingPurposesOnly().AfterBatchProcessed = () => {
            processingStarted.Set();
        };

        // Act - measure time to processing
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var task = batchService.GetEmbeddingAsync(aiConnectionStringIdentifier, ["Test immediate processing"], CancellationToken.None);

        // Wait for processing to start
        var success = processingStarted.Wait(TimeSpan.FromSeconds(5));
        sw.Stop();

        // Complete the task
        await task;

        // Assert
        Assert.True(success, "Processing should have started within timeout");

        // Processing should start almost immediately (we allow 500ms for test reliability, but in practice it should be much faster)
        Assert.True(sw.ElapsedMilliseconds < 500, $"Processing should start immediately, took {sw.ElapsedMilliseconds}ms");
    }
}

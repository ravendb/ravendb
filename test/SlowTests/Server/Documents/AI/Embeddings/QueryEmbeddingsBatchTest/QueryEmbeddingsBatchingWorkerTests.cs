using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Logging;
using SlowTests.Server.Documents.AI.Embeddings.QueryEmbeddingsBatchTest.Helpers;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.Embeddings.QueryEmbeddingsBatchTest;

public class QueryEmbeddingsBatchingWorkerTests : EmbeddingsGenerationTestBase
{
    private readonly TestDocumentDatabaseStub _db;
    private readonly AiConnectionStringIdentifier _connectionStringId;
    private readonly RavenLogger _logger;
    private readonly CancellationTokenSource _cts;
    
    private const string TestText = "test text";
    private const int DimensionSize = 123;

    public QueryEmbeddingsBatchingWorkerTests(ITestOutputHelper output) : base(output)
    {
        _db = new TestDocumentDatabaseStub();
        _connectionStringId = new AiConnectionStringIdentifier("test-connection");
        _logger = RavenLogManager.Instance.CreateNullLogger();
        _cts = new CancellationTokenSource();
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task EnqueueRequestAsync_ReturnsValidEmbedding()
    {
        // Arrange
        var service = TestAiHelper.CreateMockEmbeddingService(DimensionSize);

        using var worker = new QueryEmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            service,
            _connectionStringId,
            _logger,
            _cts.Token);

        worker.Start();

        // Act
        var task = worker.EnqueueRequestAsync([TestText], CancellationToken.None);
        var result = await task;

        // Assert
        for (var i = 0; i < result.Length; i++)
            Assert.True(result[i].Length == DimensionSize, $"Should be a valid embedding, but was not. Expected '{DimensionSize}' dimensions, but result #{i} has '{result[i].Length}' dimensions");
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task ProcessBatch_HandlesMultipleRequests()
    {
        // Arrange
        const int processedTextsCount = 10;
        var service = TestAiHelper.CreateMockEmbeddingService(DimensionSize);
        var mockService = service as TestEmbeddingGenerationService;

        using var worker = new QueryEmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            service,
            _connectionStringId,
            _logger,
            _cts.Token);

        worker.Start();

        // Act
        var tasks = new List<Task<ReadOnlyMemory<float>[]>>();
        for (int i = 0; i < processedTextsCount; i++)
            tasks.Add(worker.EnqueueRequestAsync([$"text {i}"], CancellationToken.None));

        await Task.WhenAll(tasks);

        // Assert
        Assert.NotNull(mockService);
        Assert.True(mockService.ProcessedTexts.Count == processedTextsCount, $"Should have processed '{processedTextsCount}' texts, but was '{mockService.ProcessedTexts.Count}'");

        // Verify all results have the expected dimension
        foreach (var task in tasks)
        {
            var result = await task;
            for (var i = 0; i < result.Length; i++)
                Assert.True(result[i].Length == DimensionSize, $"Should be a valid embedding, but was not. Expected '{DimensionSize}' dimensions, but result #{i} has '{result[i].Length}' dimensions");
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task EnqueueRequestAsync_CancellationWorks()
    {
        // Arrange
        var service = TestAiHelper.CreateMockEmbeddingService(DimensionSize);
        var mockService = service as TestEmbeddingGenerationService;
        Assert.NotNull(mockService);
        mockService.ProcessingDelayMs = (int)TimeSpan.FromSeconds(5).TotalMilliseconds; // Ensure processing takes time

        using var worker = new QueryEmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            service,
            _connectionStringId,
            _logger,
            _cts.Token);

        worker.Start();

        using var requestCts = new CancellationTokenSource();

        // Act
        var task = worker.EnqueueRequestAsync([TestText], requestCts.Token);
        await requestCts.CancelAsync();

        // Assert
        var exception = await Record.ExceptionAsync(() => task);
        Assert.NotNull(exception);
        Assert.True(exception is OperationCanceledException, $"Expected OperationCanceledException, but got {exception.GetType().Name}");
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task WorkerCancellation_CancelsAllRequests()
    {
        // Arrange
        var service = TestAiHelper.CreateMockEmbeddingService(DimensionSize);
        var mockService = service as TestEmbeddingGenerationService;
        Assert.NotNull(mockService);
        mockService.ProcessingDelayMs = 1000; // Long delay to ensure cancellation can happen

        using var workerCts = new CancellationTokenSource();

        using var worker = new QueryEmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            service,
            _connectionStringId,
            _logger,
            workerCts.Token);

        worker.Start();

        // Queue several requests
        var tasks = new List<Task<ReadOnlyMemory<float>[]>>();
        for (int i = 0; i < 5; i++)
            tasks.Add(worker.EnqueueRequestAsync([$"text {i}"], CancellationToken.None));

        // Allow some time for the requests to be enqueued
        await Task.Delay(50, workerCts.Token);

        // Act - Cancel the worker
        await workerCts.CancelAsync();

        // Assert - All tasks should be cancelled
        foreach (var task in tasks)
        {
            var exception = await Record.ExceptionAsync(() => task);
            Assert.NotNull(exception);
            Assert.True(exception is OperationCanceledException, $"Expected OperationCanceledException, but got {exception.GetType().Name}");
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task BatchingLogic_RespectsMaxBatchSize()
    {
        // Arrange
        _db.Configuration.QueryEmbeddingsMaxBatchSize = 5; // Small batch size
        _db.Configuration.QueryEmbeddingsMaxConcurrentBatches = 1;

        var service = TestAiHelper.CreateMockEmbeddingService(DimensionSize);
        var mockService = service as TestEmbeddingGenerationService;
        Assert.NotNull(mockService);
        mockService.ProcessingDelayMs = 50; // Add some delay

        using var worker = new QueryEmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            service,
            _connectionStringId,
            _logger,
            _cts.Token);

        worker.Start();

        // Act
        var tasks = new List<Task<ReadOnlyMemory<float>[]>>();
        for (int i = 0; i < 12; i++)
            tasks.Add(worker.EnqueueRequestAsync([$"text {i}"], CancellationToken.None));

        await Task.WhenAll(tasks);

        // Assert
        // Should have called at least 3 batches (12/5 ceiling: 5 + 5 + 2)
        Assert.True(mockService.BatchCallCount >= 3, $"Should have called at least 3 batches (12/5 rounded up: 5 + 5 + 2), but was '{mockService.BatchCallCount}'");

        // All results should have correct dimensions
        foreach (var task in tasks)
        {
            var result = await task;
            for (var i = 0; i < result.Length; i++)
                Assert.True(result[i].Length == DimensionSize, $"Should be a valid embedding, but was not. Expected '{DimensionSize}' dimensions, but result #{i} has '{result[i].Length}'dimensions");
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task QueryEmbeddingsBatchingWorker_RespectsMaxConcurrentBatchesConfiguration()
    {
        // Arrange
        const int expectedConcurrentWorkers = 4;

        // Configure database
        _db.Configuration.QueryEmbeddingsMaxConcurrentBatches = expectedConcurrentWorkers;
        _db.Configuration.QueryEmbeddingsMaxBatchSize = 1; // Each request is its own batch

        // Create synchronization primitives
        var concurrencyReachedEvent = new ManualResetEventSlim(false);
        var maxConcurrencyHeldEvent = new ManualResetEventSlim(false);
        var workerBlockedEvents = new ConcurrentDictionary<int, SemaphoreSlim>();

        // Track active threads and concurrency metrics
        var activeThreads = new ConcurrentDictionary<int, int>();
        var concurrencyCounter = 0;
        var maxConcurrency = 0;
        var maxConcurrencyHeldDuration = 0;

        // Create custom embedding service
        var mockService = new TestEmbeddingGenerationService();
        mockService.CustomBehavior = async (texts, token) =>
        {
            // Track thread
            int threadId = Environment.CurrentManagedThreadId;

            if (activeThreads.TryAdd(threadId, 1))
            {
                // Create semaphore for this thread
                var threadSemaphore = new SemaphoreSlim(0, 1);
                workerBlockedEvents[threadId] = threadSemaphore;

                // Increment and track concurrent threads
                var currentConcurrency = Interlocked.Increment(ref concurrencyCounter);
                Interlocked.Exchange(ref maxConcurrency, Math.Max(currentConcurrency, maxConcurrency));

                // Signal if we've reached expected concurrency
                if (currentConcurrency == expectedConcurrentWorkers && concurrencyReachedEvent.IsSet == false)
                {
                    concurrencyReachedEvent.Set();

                    // Hold at max concurrency for a moment to ensure it's stable
                    await Task.Delay(200, token);
                    maxConcurrencyHeldEvent.Set();
                    Interlocked.Increment(ref maxConcurrencyHeldDuration);
                }

                try
                {
                    // Wait to be released
                    bool released = await threadSemaphore.WaitAsync(TimeSpan.FromSeconds(30), token);
                    Assert.True(released, $"Thread blocked for too long without being released");
                }
                finally
                {
                    // Clean up
                    Interlocked.Decrement(ref concurrencyCounter);
                    threadSemaphore.Dispose();
                    workerBlockedEvents.TryRemove(threadId, out _);
                }
            }

            // Generate and return mock embeddings
            var result = new List<ReadOnlyMemory<float>>();
            foreach (var text in texts)
            {
                var embedding = new float[DimensionSize];
                for (int i = 0; i < DimensionSize; i++)
                    embedding[i] = 0.1f * i;

                result.Add(new ReadOnlyMemory<float>(embedding));
            }

            return result;
        };

        using var worker = new QueryEmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            mockService,
            _connectionStringId,
            _logger,
            _cts.Token);

        worker.Start();

        // Act - Submit requests
        const int totalRequests = expectedConcurrentWorkers * 2;
        var tasks = new List<Task<ReadOnlyMemory<float>[]>>();

        for (int i = 0; i < totalRequests; i++)
            tasks.Add(worker.EnqueueRequestAsync([$"request {i}"], CancellationToken.None));

        // Wait for expected concurrency to be reached
        bool concurrencyReached = concurrencyReachedEvent.Wait(TimeSpan.FromSeconds(30));
        bool concurrencyHeld = maxConcurrencyHeldEvent.Wait(TimeSpan.FromSeconds(5));

        // Assert
        Assert.True(concurrencyReached, $"Failed to reach expected concurrency of {expectedConcurrentWorkers} threads");
        Assert.True(concurrencyHeld, $"Failed to maintain concurrency at {expectedConcurrentWorkers} long enough to verify stability");
        Assert.True(expectedConcurrentWorkers == maxConcurrency, $"Expected max concurrency of {expectedConcurrentWorkers}, but observed {maxConcurrency}");
        Assert.True(activeThreads.Count >= expectedConcurrentWorkers, $"Expected at least {expectedConcurrentWorkers} unique threads, got {activeThreads.Count}");
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task SingleCanceledRequest_DoesNotCancelEntireBatch()
    {
        // Arrange
        var service = TestAiHelper.CreateMockEmbeddingService(DimensionSize);
        var mockService = service as TestEmbeddingGenerationService;
        Assert.NotNull(mockService);
        mockService.ProcessingDelayMs = 500; // Add delay for testing

        using var worker = new QueryEmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            service,
            _connectionStringId,
            _logger,
            _cts.Token);

        worker.Start();

        // Create cancellation token for one request
        using var individualCts = new CancellationTokenSource();

        // Act - Submit multiple requests, one with cancellation token
        var tasks = new List<Task>();

        // First 5 normal requests
        for (int i = 0; i < 5; i++)
            tasks.Add(worker.EnqueueRequestAsync([$"normal text {i}"], CancellationToken.None));

        // One cancellable request
        var cancelableTask = worker.EnqueueRequestAsync(["cancellable text"], individualCts.Token);
        tasks.Add(cancelableTask);

        // More normal requests
        for (int i = 0; i < 5; i++)
            tasks.Add(worker.EnqueueRequestAsync([$"more normal text {i}"], CancellationToken.None));

        // Cancel just the one request
        await individualCts.CancelAsync();

        // Wait for all tasks with exception handling
        var results = new List<Exception>();
        foreach (var task in tasks)
        {
            try
            {
                await task;
                results.Add(null); // No exception
            }
            catch (Exception ex)
            {
                results.Add(ex);
            }
        }

        // Assert
        // Only one task should be cancelled
        Assert.Single(results.FindAll(e => e is OperationCanceledException));

        // The specific cancellable task should be the one that was cancelled
        Assert.True(cancelableTask.IsCanceled, $"The cancellable task should be canceled");

        // All other tasks should complete successfully
        Assert.Equal(10, results.FindAll(e => e == null).Count);
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task WorkerDisposal_ClosesAllOpenTasks()
    {
        // Arrange
        var service = TestAiHelper.CreateMockEmbeddingService(DimensionSize);
        var mockService = service as TestEmbeddingGenerationService;
        Assert.NotNull(mockService);

        // Long delay to ensure worker is disposed before processing completes
        mockService.ProcessingDelayMs = 5000;

        var worker = new QueryEmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            service,
            _connectionStringId,
            _logger,
            _cts.Token);

        worker.Start();

        // Queue tasks but don't await them yet
        var tasks = new List<Task<ReadOnlyMemory<float>[]>>();
        for (int i = 0; i < 10; i++)
            tasks.Add(worker.EnqueueRequestAsync([$"disposal test {i}"], CancellationToken.None));

        // Allow some time for tasks to be enqueued
        await Task.Delay(50);

        // Act - Prepare for disposal and dispose
        worker.Dispose();

        // Assert - All tasks should either be canceled or completed
        foreach (var task in tasks)
        {
            var exception = await Record.ExceptionAsync(() => task);
            Assert.NotNull(exception);
            Assert.True(exception is OperationCanceledException,
                $"Expected OperationCanceledException after disposal, but got {exception.GetType().Name}");
        }
    }
}

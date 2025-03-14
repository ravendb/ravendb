using System;
using System.Collections.Generic;
using System.Diagnostics;
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
    private readonly SemaphoreSlim _concurrencyLimiter;
    private readonly RavenLogger _logger;
    private readonly CancellationTokenSource _cts;
    
    private const string TestText = "test text";
    private const int DimensionSize = 123;

    public QueryEmbeddingsBatchingWorkerTests(ITestOutputHelper output) : base(output)
    {
        _db = new TestDocumentDatabaseStub();
        _connectionStringId = new AiConnectionStringIdentifier("test-connection");
        _concurrencyLimiter = new SemaphoreSlim(_db.Configuration.QueryEmbeddingsMaxConcurrentBatches);
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
            _concurrencyLimiter,
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
            _concurrencyLimiter,
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
            _concurrencyLimiter,
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
            _concurrencyLimiter,
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

        var service = TestAiHelper.CreateMockEmbeddingService(DimensionSize);
        var mockService = service as TestEmbeddingGenerationService;
        Assert.NotNull(mockService);
        mockService.ProcessingDelayMs = 50; // Add some delay

        using var worker = new QueryEmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            service,
            _connectionStringId,
            _concurrencyLimiter,
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
    public async Task MultipleWorkers_ProcessConcurrently()
    {
        // Arrange - Configure multiple worker threads
        _db.Configuration.QueryEmbeddingsMaxConcurrentBatches = 4;
        _db.Configuration.QueryEmbeddingsMaxBatchSize = 5;

        var service = TestAiHelper.CreateMockEmbeddingService(DimensionSize);
        var mockService = service.Instance as TestEmbeddingGenerationService;
        Assert.NotNull(mockService);

        // Add delay so we can observe concurrency
        mockService.ProcessingDelayMs = 500;

        using var worker = new QueryEmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            service,
            _connectionStringId,
            _concurrencyLimiter,
            _logger,
            _cts.Token);

        var activeProcessors = 0;
        var maxConcurrentProcessors = 0;

        // Set up test hook to track concurrency
        worker.ForTestingPurposesOnly().AfterBatchProcessed = () => {
            var current = Interlocked.Increment(ref activeProcessors);
            Interlocked.CompareExchange(ref maxConcurrentProcessors, current, Math.Max(current, maxConcurrentProcessors));

            // Simulate processing work
            Thread.Sleep(200);

            Interlocked.Decrement(ref activeProcessors);
        };

        worker.Start();

        // Act - Submit multiple requests in quick succession
        var tasks = new List<Task<ReadOnlyMemory<float>[]>>();
        for (int i = 0; i < 20; i++)
            tasks.Add(worker.EnqueueRequestAsync([$"text {i}"], CancellationToken.None));

        // Use a stopwatch to measure parallel execution
        var sw = Stopwatch.StartNew();
        await Task.WhenAll(tasks);
        sw.Stop();

        // Assert
        Assert.True(maxConcurrentProcessors > 1, $"Should have observed multiple concurrent processors, but max was {maxConcurrentProcessors}");
        Assert.True(maxConcurrentProcessors <= _db.Configuration.QueryEmbeddingsMaxConcurrentBatches,
            $"Should not exceed {_db.Configuration.QueryEmbeddingsMaxConcurrentBatches} concurrent processors, but observed {maxConcurrentProcessors}");

        // With 20 requests in batches of 5, we expect 4 batches
        // If running serially with 500ms delay, this would take ~2000ms
        // With 4 concurrent workers, it should take ~500ms (or a bit more with overhead)
        // This verifies we're processing in parallel
        Assert.True(sw.ElapsedMilliseconds < 1500, $"Parallel processing should be faster than serial execution, took {sw.ElapsedMilliseconds}ms");
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task SingleCanceledRequest_DoesNotCancelEntireBatch()
    {
        // Arrange
        var service = TestAiHelper.CreateMockEmbeddingService(DimensionSize);
        var mockService = service.Instance as TestEmbeddingGenerationService;
        Assert.NotNull(mockService);
        mockService.ProcessingDelayMs = 500; // Add delay for testing

        using var worker = new QueryEmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            service,
            _connectionStringId,
            _concurrencyLimiter,
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
        var mockService = service.Instance as TestEmbeddingGenerationService;
        Assert.NotNull(mockService);

        // Long delay to ensure worker is disposed before processing completes
        mockService.ProcessingDelayMs = 5000;

        var worker = new QueryEmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            service,
            _connectionStringId,
            _concurrencyLimiter,
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
        await worker.PrepareForServiceDisposalAsync();
        worker.Dispose();

        // Assert - All tasks should either be canceled or completed
        foreach (var task in tasks)
        {
            var exception = await Record.ExceptionAsync(() => task);
            Assert.NotNull(exception);
            Assert.True(exception is OperationCanceledException,
                $"Expected OperationCanceledException after disposal, but got {exception.GetType().Name}");
            Assert.Contains(QueryEmbeddingsBatchingService.ShutdownMessage, exception.Message);
        }
    }
}

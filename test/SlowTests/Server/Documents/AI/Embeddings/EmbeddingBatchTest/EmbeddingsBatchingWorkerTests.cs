using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Logging;
using SlowTests.Server.Documents.AI.Embeddings.EmbeddingBatchTest.Helpers;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Xunit;

namespace SlowTests.Server.Documents.AI.Embeddings.EmbeddingBatchTest;

public class EmbeddingsBatchingWorkerTests : IDisposable
{
    private readonly TestDocumentDatabaseStub _db;
    private readonly AiConnectionStringIdentifier _connectionStringId;
    private readonly SemaphoreSlim _concurrencyLimiter;
    private readonly RavenLogger _logger;
    private readonly CancellationTokenSource _cts;

    public EmbeddingsBatchingWorkerTests()
    {
        _db = new TestDocumentDatabaseStub();
        _connectionStringId = new AiConnectionStringIdentifier("test-connection");
        _concurrencyLimiter = new SemaphoreSlim(_db.Configuration.MaxConcurrentBatches);
        _logger = RavenLogManager.Instance.CreateNullLogger();
        _cts = new CancellationTokenSource();
    }

    public void Dispose()
    {
        _concurrencyLimiter.Dispose();
        _cts.Dispose();
    }

    [Fact]
    public async Task EnqueueRequestAsync_ReturnsValidEmbedding()
    {
        // Arrange
        var service = TestAiHelper.CreateMockEmbeddingService(dimensionSize: 128);

        using var worker = new EmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            service,
            _connectionStringId,
            _concurrencyLimiter,
            _logger,
            _cts.Token);

        worker.Start();

        // Act
        var task = worker.EnqueueRequestAsync("test text", CancellationToken.None);
        var result = await task;

        // Assert
        Assert.Equal(128, result.Length);
    }

    [Fact]
    public async Task ProcessBatch_HandlesMultipleRequests()
    {
        // Arrange
        var service = TestAiHelper.CreateMockEmbeddingService(dimensionSize: 128);
        var mockService = service as TestEmbeddingGenerationService;

        using var worker = new EmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            service,
            _connectionStringId,
            _concurrencyLimiter,
            _logger,
            _cts.Token);

        worker.Start();

        // Act
        var tasks = new List<Task<ReadOnlyMemory<float>>>();
        for (int i = 0; i < 10; i++)
        {
            tasks.Add(worker.EnqueueRequestAsync($"text {i}", CancellationToken.None));
        }

        await Task.WhenAll(tasks);

        // Assert
        Assert.Equal(10, mockService.ProcessedTexts.Count);
        Assert.True(mockService.BatchCallCount <= 2); // Should batch requests

        // Verify all results have the expected dimension
        foreach (var task in tasks)
        {
            Assert.Equal(128, task.Result.Length);
        }
    }

    [Fact]
    public async Task ProcessBatch_RetriesOnFailure()
    {
        // Arrange
        var service = TestAiHelper.CreateMockEmbeddingService(dimensionSize: 128);
        var mockService = service as TestEmbeddingGenerationService;

        // Configure service to fail on first attempt only
        Assert.NotNull(mockService);
        mockService.ResetAttemptCount();
        mockService.FailForFirstNAttempts = 1;

        // Use minimal retry settings for faster test execution
        _db.Configuration.RetryDelayMs = 50;
        _db.Configuration.MaxRetries = 3;

        using var worker = new EmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            service,
            _connectionStringId,
            _concurrencyLimiter,
            _logger,
            _cts.Token);

        worker.Start();

        // Act
        var task = worker.EnqueueRequestAsync("test text", CancellationToken.None);
        var result = await task;

        // Assert
        Assert.Equal(2, mockService.AttemptCount); // Should have attempted twice
        Assert.Equal(128, result.Length); // Should eventually succeed
    }

    [Fact]
    public async Task EnqueueRequestAsync_CancellationWorks()
    {
        // Arrange
        var service = TestAiHelper.CreateMockEmbeddingService(dimensionSize: 128);
        var mockService = service as TestEmbeddingGenerationService;
        Assert.NotNull(mockService);
        mockService.ProcessingDelayMs = 500; // Ensure processing takes time

        using var worker = new EmbeddingsBatchingWorker(
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
        var task = worker.EnqueueRequestAsync("test text", requestCts.Token);
        await requestCts.CancelAsync();

        // Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
    }

    [Fact]
    public async Task WorkerCancellation_CancelsAllRequests()
    {
        // Arrange
        var service = TestAiHelper.CreateMockEmbeddingService(dimensionSize: 128);
        var mockService = service as TestEmbeddingGenerationService;
        Assert.NotNull(mockService);
        mockService.ProcessingDelayMs = 1000; // Long delay to ensure cancellation can happen

        using var workerCts = new CancellationTokenSource();

        using var worker = new EmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            service,
            _connectionStringId,
            _concurrencyLimiter,
            _logger,
            workerCts.Token);

        worker.Start();

        // Queue several requests
        var tasks = new List<Task<ReadOnlyMemory<float>>>();
        for (int i = 0; i < 5; i++)
        {
            tasks.Add(worker.EnqueueRequestAsync($"text {i}", CancellationToken.None));
        }

        // Allow some time for the requests to be enqueued
        await Task.Delay(50);

        // Act - Cancel the worker
        workerCts.Cancel();

        // Assert - All tasks should be cancelled
        foreach (var task in tasks)
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
        }
    }

    [Fact]
    public async Task BatchingLogic_RespectsMaxBatchSize()
    {
        // Arrange
        _db.Configuration.MaxBatchSize = 5; // Small batch size

        var service = TestAiHelper.CreateMockEmbeddingService(dimensionSize: 128);
        var mockService = service as TestEmbeddingGenerationService;
        Assert.NotNull(mockService);
        mockService.ProcessingDelayMs = 50; // Add some delay

        using var worker = new EmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            service,
            _connectionStringId,
            _concurrencyLimiter,
            _logger,
            _cts.Token);

        worker.Start();

        // Act
        var tasks = new List<Task<ReadOnlyMemory<float>>>();
        for (int i = 0; i < 12; i++)
            tasks.Add(worker.EnqueueRequestAsync($"text {i}", CancellationToken.None));

        await Task.WhenAll(tasks);

        // Assert
        Assert.True(mockService.BatchCallCount >= 3); // Should have called at least 3 batches (12/5 rounded up)

        // All results should have correct dimensions
        foreach (var task in tasks)
        {
            Assert.Equal(128, (await task).Length);
        }
    }

    [Fact]
    public async Task BatchingLogic_RespectsTimeout()
    {
        // Arrange
        _db.Configuration.BatchTimeoutInMs = 100;
        _db.Configuration.MaxBatchSize = 100;

        var service = TestAiHelper.CreateMockEmbeddingService(dimensionSize: 128);

        using var worker = new EmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            service,
            _connectionStringId,
            _concurrencyLimiter,
            _logger,
            _cts.Token);

        worker.Start();

        // Act - First test with single request
        var stopwatch = Stopwatch.StartNew();

        // Send one request
        var task1 = worker.EnqueueRequestAsync("text 1", CancellationToken.None);
        await task1;

        var elapsed1 = stopwatch.ElapsedMilliseconds;

        // Reset and test with multiple requests
        stopwatch.Restart();
        var task2 = worker.EnqueueRequestAsync("text 2", CancellationToken.None);

        // Wait a bit before sending more
        await Task.Delay(10);
        var task3 = worker.EnqueueRequestAsync("text 3", CancellationToken.None);

        await Task.WhenAll(task2, task3);

        var elapsed2 = stopwatch.ElapsedMilliseconds;

        // Assert
        Assert.True(elapsed1 >= 100, $"First request should wait for timeout (elapsed: {elapsed1}ms)");
        Assert.True(elapsed2 < 200, $"Second batch should process faster with multiple items (elapsed: {elapsed2}ms)");
    }

    [Fact]
    public async Task NonRetriableException_FailsImmediately()
    {
        // Arrange
        var service = TestAiHelper.CreateMockEmbeddingService(dimensionSize: 128);
        var mockService = service as TestEmbeddingGenerationService;

        // Configure service to throw a non-retriable exception
        Assert.NotNull(mockService);
        mockService.ExceptionToThrow = new ArgumentException("Invalid input");
        mockService.FailureRate = 100; // Always fail

        using var worker = new EmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            service,
            _connectionStringId,
            _concurrencyLimiter,
            _logger,
            _cts.Token);

        worker.Start();

        // Act & Assert
        var task = worker.EnqueueRequestAsync("test text", CancellationToken.None);

        // Should fail with the ArgumentException we configured
        var exception = await Assert.ThrowsAsync<ArgumentException>(() => task);
        Assert.Equal("Invalid input", exception.Message);

        // Should not have retried (BatchCallCount should be 1)
        Assert.Equal(1, mockService.BatchCallCount);
    }

    [Fact]
    public async Task RetriableException_RetriesUpToMaxRetries()
    {
        // Arrange
        _db.Configuration.MaxRetries = 2;
        _db.Configuration.RetryDelayMs = 50;

        var service = TestAiHelper.CreateMockEmbeddingService(dimensionSize: 128);
        var mockService = service as TestEmbeddingGenerationService;

        // Configure service to always throw a retriable exception
        Assert.NotNull(mockService);
        mockService.ExceptionToThrow = new InvalidOperationException("Temporary failure");
        mockService.FailureRate = 100; // Always fail

        using var worker = new EmbeddingsBatchingWorker(
            _db.Name,
            _db.Configuration,
            service,
            _connectionStringId,
            _concurrencyLimiter,
            _logger,
            _cts.Token);

        worker.Start();

        // Act
        var task = worker.EnqueueRequestAsync("test text", CancellationToken.None);

        // Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => task);
        Assert.Equal("Temporary failure", exception.Message);

        // Should have retried MaxRetries times (original + MaxRetries attempts)
        Assert.Equal(_db.Configuration.MaxRetries + 1, mockService.BatchCallCount);
    }
}

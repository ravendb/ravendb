using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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
    
    private const string TestText = "test text";
    private const int DimensionSize = 123;

    public EmbeddingsBatchingWorkerTests()
    {
        _db = new TestDocumentDatabaseStub();
        _connectionStringId = new AiConnectionStringIdentifier("test-connection");
        _concurrencyLimiter = new SemaphoreSlim(_db.Configuration.MaxConcurrentBatches);
        _logger = RavenLogManager.Instance.CreateNullLogger();
        _cts = new CancellationTokenSource();
    }

    [Fact]
    public async Task EnqueueRequestAsync_ReturnsValidEmbedding()
    {
        // Arrange
        var service = TestAiHelper.CreateMockEmbeddingService(DimensionSize);

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
        var task = worker.EnqueueRequestAsync(TestText, CancellationToken.None);
        var result = await task;

        // Assert
        Assert.True(result.Length == DimensionSize, $"Should be a valid embedding, but was not. Expected '{DimensionSize}' dimensions, actual '{result.Length}' dimensions");
    }

    [Fact]
    public async Task ProcessBatch_HandlesMultipleRequests()
    {
        // Arrange
        const int processedTextsCount = 10;
        var service = TestAiHelper.CreateMockEmbeddingService(DimensionSize);
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
        for (int i = 0; i < processedTextsCount; i++)
            tasks.Add(worker.EnqueueRequestAsync($"text {i}", CancellationToken.None));

        await Task.WhenAll(tasks);

        // Assert
        Assert.NotNull(mockService);
        Assert.True(mockService.ProcessedTexts.Count == processedTextsCount, $"Should have processed '{processedTextsCount}' texts, but was '{mockService.ProcessedTexts.Count}'");
        Assert.True(mockService.BatchCallCount <= 2); // Should batch requests

        // Verify all results have the expected dimension
        foreach (var task in tasks)
            Assert.True((await task).Length == DimensionSize, $"Should be a valid embedding, but was not. Expected '{DimensionSize}' dimensions, actual '{(await task).Length}' dimensions");
    }

    [Fact]
    public async Task ProcessBatch_RetriesOnFailure()
    {
        // Arrange
        _db.Configuration.MaxRetries = 2;
        _db.Configuration.RetryDelayMs = 50;

        var service = TestAiHelper.CreateMockEmbeddingService(DimensionSize);
        var mockService = service as TestEmbeddingGenerationService;

        // Reset attempt counter
        Assert.NotNull(mockService);
        mockService.ResetAttemptCount();

        // Setup custom behavior that will fail on first attempt with a retriable exception
        mockService.CustomBehavior = async (texts, cancellationToken) =>
        {
            var currentAttempt = mockService.AttemptCount;

            // Use IOException which should be retriable on the first attempt
            if (currentAttempt == 1)
                throw new IOException($"Temporary network error on attempt {currentAttempt}");

            // On second attempt, succeed
            await Task.Delay(10, cancellationToken); // Small delay for realism

            var result = new List<ReadOnlyMemory<float>>();
            foreach (var unused in texts)
            {
                var embedding = new float[mockService.DimensionSize];
                for (int i = 0; i < mockService.DimensionSize; i++)
                    embedding[i] = 0.1f * i;

                result.Add(new ReadOnlyMemory<float>(embedding));
            }

            return result;
        };

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
        var task = worker.EnqueueRequestAsync(TestText, CancellationToken.None);
        var result = await task;

        // Assert
        Assert.True(mockService.AttemptCount == 2, $"Should have attempted twice, but was '{mockService.AttemptCount}'");
        Assert.True(result.Length == DimensionSize, $"Should be a valid embedding, but was not. Expected length: '{DimensionSize}', actual length: '{result.Length}'");
    }

    [Fact]
    public async Task EnqueueRequestAsync_CancellationWorks()
    {
        // Arrange
        var service = TestAiHelper.CreateMockEmbeddingService(DimensionSize);
        var mockService = service as TestEmbeddingGenerationService;
        Assert.NotNull(mockService);
        mockService.ProcessingDelayMs = (int)TimeSpan.FromSeconds(5).TotalMilliseconds; // Ensure processing takes time

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
        var task = worker.EnqueueRequestAsync(TestText, requestCts.Token);
        await requestCts.CancelAsync();

        // Assert
        var exception = await Record.ExceptionAsync(() => task);
        Assert.NotNull(exception);
        Assert.True(exception is OperationCanceledException, $"Expected OperationCanceledException, but got {exception.GetType().Name}");
    }

    [Fact]
    public async Task WorkerCancellation_CancelsAllRequests()
    {
        // Arrange
        var service = TestAiHelper.CreateMockEmbeddingService(DimensionSize);
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
            tasks.Add(worker.EnqueueRequestAsync($"text {i}", CancellationToken.None));

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

    [Fact]
    public async Task BatchingLogic_RespectsMaxBatchSize()
    {
        // Arrange
        _db.Configuration.MaxBatchSize = 5; // Small batch size
        _db.Configuration.BatchTimeoutInMs = (int)TimeSpan.FromSeconds(10).TotalMilliseconds; // We don't want timeout to interfere

        var service = TestAiHelper.CreateMockEmbeddingService(DimensionSize);
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
        // Should have called at least 3 batches ()
        Assert.True(mockService.BatchCallCount >= 3, $"Should have called at least 3 batches (12/5 rounded up: 5 + 5 + 2), but was '{mockService.BatchCallCount}'");

        // All results should have correct dimensions
        foreach (var task in tasks)
            Assert.True((await task).Length == DimensionSize, $"Should be a valid embedding, but was not. Expected '{DimensionSize}' dimensions, actual '{(await task).Length}' dimensions");
    }

    [Fact]
    public async Task BatchingLogic_RespectsTimeout()
    {
        // Arrange
        _db.Configuration.BatchTimeoutInMs = 100;
        _db.Configuration.MaxBatchSize = 100;

        var service = TestAiHelper.CreateMockEmbeddingService(DimensionSize);

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
        const string exceptionMessage = "Some understandable exception message";
        var service = TestAiHelper.CreateMockEmbeddingService(DimensionSize);
        var mockService = service as TestEmbeddingGenerationService;

        // Configure service to throw a non-retriable exception
        Assert.NotNull(mockService);
        mockService.ExceptionToThrow = new ArgumentException(exceptionMessage);
        mockService.FailureRateInPercentage = 100; // Always fail

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
        var task = worker.EnqueueRequestAsync(TestText, CancellationToken.None);

        // Should fail with the ArgumentException we configured
        var exception = await Assert.ThrowsAsync<ArgumentException>(() => task);
        Assert.True(exception.Message == exceptionMessage, $"Expected exception message '{exceptionMessage}', but got '{exception.Message}'");
        Assert.True(mockService.BatchCallCount == 1, $"Should have called the service once, but was '{mockService.BatchCallCount}'");
    }

    [Fact]
    public async Task RetriableException_RetriesUpToMaxRetries()
    {
        // Arrange
        const string exceptionMessagePrefix = "Temporary network error on attempt";
        _db.Configuration.MaxRetries = 2;
        _db.Configuration.RetryDelayMs = 50;

        var service = TestAiHelper.CreateMockEmbeddingService(DimensionSize);
        var mockService = service as TestEmbeddingGenerationService;

        // Reset attempt counter
        Assert.NotNull(mockService);
        mockService.ResetAttemptCount();

        // Setup custom behavior that will always fail with a retriable exception
        mockService.CustomBehavior = (_, _) => throw new IOException($"{exceptionMessagePrefix} '{mockService.AttemptCount}'");

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
        var task = worker.EnqueueRequestAsync(TestText, CancellationToken.None);

        // Should fail after maxRetries + 1 attempts
        var exception = await Assert.ThrowsAsync<IOException>(() => task);
        Assert.True(exception.Message.Contains(exceptionMessagePrefix), $"Expected exception message to contain '{exceptionMessagePrefix}', but got '{exception.Message}'");
        Assert.True(mockService.AttemptCount == _db.Configuration.MaxRetries + 1, $"Should have attempted {_db.Configuration.MaxRetries + 1} times, but was '{mockService.AttemptCount}'");
    }

    void IDisposable.Dispose()
    {
        _concurrencyLimiter.Dispose();
        _cts.Dispose();
    }
}

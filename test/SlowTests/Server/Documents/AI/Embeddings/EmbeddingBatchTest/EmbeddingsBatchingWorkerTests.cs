using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Logging;
using SlowTests.Server.Documents.AI.Embeddings.EmbeddingBatchTest.Helpers;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.AI.Embeddings.EmbeddingBatchTest;

public class EmbeddingsBatchingWorkerTests : EmbeddingsGenerationTestBase
{
    private readonly TestDocumentDatabaseStub _db;
    private readonly AiConnectionStringIdentifier _connectionStringId;
    private readonly SemaphoreSlim _concurrencyLimiter;
    private readonly RavenLogger _logger;
    private readonly CancellationTokenSource _cts;
    
    private const string TestText = "test text";
    private const int DimensionSize = 123;

    public EmbeddingsBatchingWorkerTests(ITestOutputHelper output) : base(output)
    {
        _db = new TestDocumentDatabaseStub();
        _connectionStringId = new AiConnectionStringIdentifier("test-connection");
        _concurrencyLimiter = new SemaphoreSlim(_db.Configuration.MaxConcurrentBatches);
        _logger = RavenLogManager.Instance.CreateNullLogger();
        _cts = new CancellationTokenSource();
    }

    [RavenFact(RavenTestCategory.Ai)]
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
        var task = worker.EnqueueRequestAsync([TestText], CancellationToken.None);
        var result = await task;

        // Assert
        for (var i = 0; i < result.Count; i++)
            Assert.True(result[i].Length == DimensionSize, $"Should be a valid embedding, but was not. Expected '{DimensionSize}' dimensions, but result #{i} has '{result[i].Length}' dimensions");

    }

    [RavenFact(RavenTestCategory.Ai)]
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
        var tasks = new List<Task<IList<ReadOnlyMemory<float>>>>();
        for (int i = 0; i < processedTextsCount; i++)
            tasks.Add(worker.EnqueueRequestAsync([$"text {i}"], CancellationToken.None));

        await Task.WhenAll(tasks);

        // Assert
        Assert.NotNull(mockService);
        Assert.True(mockService.ProcessedTexts.Count == processedTextsCount, $"Should have processed '{processedTextsCount}' texts, but was '{mockService.ProcessedTexts.Count}'");
        Assert.True(mockService.BatchCallCount <= 2); // Should batch requests

        // Verify all results have the expected dimension
        foreach (var task in tasks)
        {
            var result = await task;
            for (var i = 0; i < result.Count; i++)
                Assert.True(result[i].Length == DimensionSize, $"Should be a valid embedding, but was not. Expected '{DimensionSize}' dimensions, but result #{i} has '{result[i].Length}' dimensions");
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
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
        var task = worker.EnqueueRequestAsync([TestText], CancellationToken.None);
        var result = await task;

        // Assert
        Assert.True(mockService.AttemptCount == 2, $"Should have attempted twice, but was '{mockService.AttemptCount}'");
        for (var i = 0; i < result.Count; i++)
            Assert.True(result[i].Length == DimensionSize, $"Should be a valid embedding, but was not. Expected length: '{DimensionSize}', but result #{i} has '{result[i].Length}' dimensions");
    }

    [RavenFact(RavenTestCategory.Ai)]
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
        var tasks = new List<Task<IList<ReadOnlyMemory<float>>>>();
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
        var tasks = new List<Task<IList<ReadOnlyMemory<float>>>>();
        for (int i = 0; i < 12; i++)
            tasks.Add(worker.EnqueueRequestAsync([$"text {i}"], CancellationToken.None));

        await Task.WhenAll(tasks);

        // Assert
        // Should have called at least 3 batches ()
        Assert.True(mockService.BatchCallCount >= 3, $"Should have called at least 3 batches (12/5 rounded up: 5 + 5 + 2), but was '{mockService.BatchCallCount}'");

        // All results should have correct dimensions
        foreach (var task in tasks)
        {
            var result = await task;
            for (var i = 0; i < result.Count; i++)
                Assert.True(result[i].Length == DimensionSize, $"Should be a valid embedding, but was not. Expected '{DimensionSize}' dimensions, but result #{i} has '{result[i].Length}'dimensions");
        }
    }

    [RavenFact(RavenTestCategory.Ai)]
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
        var task1 = worker.EnqueueRequestAsync(["text 1"], CancellationToken.None);
        await task1;

        var elapsed1 = stopwatch.ElapsedMilliseconds;

        // Reset and test with multiple requests
        stopwatch.Restart();
        var task2 = worker.EnqueueRequestAsync(["text 2"], CancellationToken.None);

        // Wait a bit before sending more
        await Task.Delay(10);
        var task3 = worker.EnqueueRequestAsync(["text 3"], CancellationToken.None);

        await Task.WhenAll(task2, task3);

        var elapsed2 = stopwatch.ElapsedMilliseconds;

        // Assert
        Assert.True(elapsed1 >= 100, $"First request should wait for timeout (elapsed: {elapsed1}ms)");
        Assert.True(elapsed2 < 200, $"Second batch should process faster with multiple items (elapsed: {elapsed2}ms)");
    }

    [RavenFact(RavenTestCategory.Ai)]
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
        var task = worker.EnqueueRequestAsync([TestText], CancellationToken.None);

        // Should fail with the ArgumentException we configured
        var exception = await Assert.ThrowsAsync<ArgumentException>(() => task);
        Assert.True(exception.Message == exceptionMessage, $"Expected exception message '{exceptionMessage}', but got '{exception.Message}'");
        Assert.True(mockService.BatchCallCount == 1, $"Should have called the service once, but was '{mockService.BatchCallCount}'");
    }

    [RavenFact(RavenTestCategory.Ai)]
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
        var task = worker.EnqueueRequestAsync([TestText], CancellationToken.None);

        // Should fail after maxRetries + 1 attempts
        var exception = await Assert.ThrowsAsync<IOException>(() => task);
        Assert.True(exception.Message.Contains(exceptionMessagePrefix), $"Expected exception message to contain '{exceptionMessagePrefix}', but got '{exception.Message}'");
        Assert.True(mockService.AttemptCount == _db.Configuration.MaxRetries + 1, $"Should have attempted {_db.Configuration.MaxRetries + 1} times, but was '{mockService.AttemptCount}'");
    }

    [RavenFact(RavenTestCategory.Ai)]
    public async Task PartialSuccessInBatch_ShouldHandleMixedResults()
    {
        const string successPrefix = "success";
        const int dimensionSize = 384;
        // Create a document store and register AI integration
        using var store = GetDocumentStore();
        (_, AiConnectionString connection) = AddEmbeddingsGenerationTask(store);

        // Get the database instance
        var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

        // Configure for test
        database.Configuration.Ai.MaxRetries = 0; // Disable retries for this test
        database.Configuration.Ai.BatchTimeoutInMs = 50;
        database.Configuration.Ai.MaxBatchSize = 10; // Allow all requests in one batch

        // Create the batching service
        var batchService = new EmbeddingsBatchingService(database.AiIntegrations);
        var aiConnectionStringIdentifier = new AiConnectionStringIdentifier(connection.Identifier);

        // Submit a first request to ensure worker is created
        await batchService.GetEmbeddingAsync(aiConnectionStringIdentifier, ["Initial request"], CancellationToken.None);

        // Verify we can get the worker
        var worker = batchService.ForTestingPurposesOnly().GetBatchWorker(aiConnectionStringIdentifier);
        Assert.True(worker != null, "Worker wasn't created properly");

        // Create a service that selectively succeeds/fails based on text content
        var selectiveService = new TestEmbeddingGenerationService
        {
            DimensionSize = dimensionSize,
            CustomBehavior = (texts, _) =>
            {
                // Process texts individually
                var results = new List<ReadOnlyMemory<float>>();

                foreach (var text in texts)
                {
                    // Succeed for texts containing 'successPrefix' fail for others
                    if (text.Contains(successPrefix))
                    {
                        var embedding = new float[dimensionSize];
                        for (int i = 0; i < dimensionSize; i++)
                            embedding[i] = 0.1f * i;

                        results.Add(embedding);
                    }
                    else
                    {
                        // Failure case - add null to create "holes" in the results
                        results.Add(null); // Empty embedding
                    }
                }

                // Emulate the expected behavior - return list with same count as input
                return Task.FromResult<IList<ReadOnlyMemory<float>>>(results);
            }
        };

        // Replace the service field directly
        var serviceField = typeof(EmbeddingsBatchingWorker).GetField("<service>P", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        Assert.True(serviceField != null, "We want to replace the service field, but it wasn't found");
        serviceField.SetValue(worker, selectiveService);

        // Submit a mix of requests that should succeed or fail
        var successTasks = new List<Task<IList<ReadOnlyMemory<float>>>>();
        var failureTasks = new List<Task<IList<ReadOnlyMemory<float>>>>();
        var allTasks = new List<Task<IList<ReadOnlyMemory<float>>>>();

        // Randomize the order of requests controlling that we have at least one success and one failure
        var random = new Random();
        while (successTasks.Count < 1 || failureTasks.Count < 1 || allTasks.Count < 10)
        {
            var text = random.Next(0, 2) == 0 ? "success" : "failure";
            var task = batchService.GetEmbeddingAsync(aiConnectionStringIdentifier, [text], CancellationToken.None).AsTask();

            if (text == successPrefix)
                successTasks.Add(task);
            else
                failureTasks.Add(task);

            allTasks.Add(task);
        }

        // Wait for successful tasks
        await Task.WhenAll(allTasks);

        // Verify successful tasks
        foreach (var task in successTasks)
        {
            Assert.True(task.IsCompletedSuccessfully, $"Tasks containing '{successPrefix}' should complete successfully");
            for (var i = 0; i < task.Result.Count; i++)
                Assert.True(task.Result[i].Length == dimensionSize, $"Expected embedding of length '{dimensionSize}', but result #{i} got '{task.Result[i].Length}' dimensions");
        }

        // Verify failure tasks - they should either fail or return empty embeddings
        foreach (var task in failureTasks)
            for (var i = 0; i < task.Result.Count; i++)
                Assert.True(task.Result[i].Length == 0, $"Expected empty embedding for failure case, but result #{i} got '{task.Result[i].Length}' dimensions");
    }
}

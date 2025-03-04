using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI;
#pragma warning disable SKEXP0001

namespace SlowTests.Server.Documents.AI.Embeddings.EmbeddingBatchTest.Helpers;

public static class TestAiHelper
{
    public static ITextEmbeddingGenerationService CreateMockEmbeddingService(
        int dimensionSize = 128,
        int failureRate = 0,
        Exception exceptionToThrow = null)
    {
        return new TestEmbeddingGenerationService
        {
            DimensionSize = dimensionSize,
            FailureRateInPercentage = failureRate,
            ExceptionToThrow = exceptionToThrow
        };
    }

    public static TestAiIntegrationsController CreateAiIntegrationsController(
        TestDocumentDatabaseStub db,
        params (string connectionStringId, ITextEmbeddingGenerationService service)[] services)
    {
        var controller = new TestAiIntegrationsController(db);

        foreach ((string id, ITextEmbeddingGenerationService service) in services)
            controller.RegisterService(new AiConnectionStringIdentifier(id), service);

        return controller;
    }
}

public class TestEmbeddingServiceWrapper : ITextEmbeddingGenerationService
{
    private readonly ITextEmbeddingGenerationService _innerService;
    public Exception ExceptionToThrow { get; set; }
    public int CallCount { get; private set; } = 0;

    public IReadOnlyDictionary<string, object> Attributes => _innerService.Attributes;

    public TestEmbeddingServiceWrapper(ITextEmbeddingGenerationService innerService, Exception exceptionToThrow = null)
    {
        _innerService = innerService;
        ExceptionToThrow = exceptionToThrow;
    }

    public async Task<IList<ReadOnlyMemory<float>>> GenerateEmbeddingsAsync(IList<string> texts, CancellationToken cancellationToken = default)
    {
        CallCount++;

        if (ExceptionToThrow != null)
            throw ExceptionToThrow;

        return await _innerService.GenerateEmbeddingsAsync(texts, cancellationToken: cancellationToken);
    }

    public Task<IList<ReadOnlyMemory<float>>> GenerateEmbeddingsAsync(IList<string> data, Kernel kernel = null, CancellationToken cancellationToken = new())
    {
        CallCount++;

        if (ExceptionToThrow != null)
            throw ExceptionToThrow;

        return _innerService.GenerateEmbeddingsAsync(data, kernel, cancellationToken);
    }
}

public class TestEmbeddingGenerationService : ITextEmbeddingGenerationService
{
    public int DimensionSize { get; set; } = 128;
    public int ProcessingDelayMs { get; set; } = 10;
    public int FailureRateInPercentage { get; set; } = 0; // 0-100, percentage of requests that should fail
    public int BatchCallCount { get; private set; } = 0;
    public List<string> ProcessedTexts { get; } = [];
    public Exception ExceptionToThrow { get; set; } = null;

    // Add callback for custom behavior
    public Func<IList<string>, CancellationToken, Task<IList<ReadOnlyMemory<float>>>> CustomBehavior { get; set; }

    // Track call attempts
    public int AttemptCount { get; private set; } = 0;

    private readonly Dictionary<string, object> _attributes = new()
    {
        ["ModelId"] = "test-model",
        ["MaxInputLength"] = 512,
        ["Dimensions"] = 128
    };

    public IReadOnlyDictionary<string, object> Attributes => _attributes;

    public async Task<IList<ReadOnlyMemory<float>>> GenerateEmbeddingsAsync(IList<string> texts, CancellationToken cancellationToken = default)
    {
        BatchCallCount++;
        AttemptCount++;

        // If we have custom behavior defined, use it
        if (CustomBehavior != null)
            return await CustomBehavior(texts, cancellationToken);

        // Save processed texts
        lock (ProcessedTexts)
        {
            ProcessedTexts.AddRange(texts);
        }

        // Simulate processing delay
        if (ProcessingDelayMs > 0)
            await Task.Delay(ProcessingDelayMs, cancellationToken);

        // Simulate errors if configured
        if (FailureRateInPercentage > 0)
        {
            var rand = new Random();
            if (rand.Next(100) < FailureRateInPercentage)
            {
                if (ExceptionToThrow != null)
                    throw ExceptionToThrow;

                throw new InvalidOperationException("Simulated error in embedding generation");
            }
        }

        // Generate random embeddings for testing
        var result = new List<ReadOnlyMemory<float>>();
        foreach (var text in texts)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var hash = text.GetHashCode();
            var rand = new Random(hash);
            var embedding = new float[DimensionSize];

            for (int i = 0; i < DimensionSize; i++)
                embedding[i] = (float)rand.NextDouble();

            result.Add(new ReadOnlyMemory<float>(embedding));
        }

        return result;
    }

    public Task<IList<ReadOnlyMemory<float>>> GenerateEmbeddingsAsync(IList<string> data, Kernel kernel = null, CancellationToken cancellationToken = new()) =>
        GenerateEmbeddingsAsync(data, cancellationToken);

    public void ResetAttemptCount()
    {
        AttemptCount = 0;
    }
}

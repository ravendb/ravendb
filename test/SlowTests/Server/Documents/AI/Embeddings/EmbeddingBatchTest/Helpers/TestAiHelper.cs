using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.ETL.Providers.AI;
#pragma warning disable SKEXP0001

namespace SlowTests.Server.Documents.AI.Embeddings.EmbeddingBatchTest.Helpers;

public static class TestAiHelper
{
    public static ITextEmbeddingGenerationService CreateEmbeddingService(
        Exception exceptionToThrow = null)
    {
        var services = AiHelper.CreateServicesForTest(
            new EmbeddingsGenerationConfiguration {
                Connection = new AiConnectionString { OnnxSettings = new OnnxSettings()}
            }, out string serviceId);

        var realService = services.GetRequiredKeyedService<ITextEmbeddingGenerationService>(serviceId);

        return exceptionToThrow != null
            ? new TestEmbeddingServiceWrapper(realService, exceptionToThrow)
            : realService;
    }

    public static ITextEmbeddingGenerationService CreateMockEmbeddingService(
        int dimensionSize = 128,
        int failureRate = 0,
        Exception exceptionToThrow = null)
    {
        return new TestEmbeddingGenerationService
        {
            DimensionSize = dimensionSize,
            FailureRate = failureRate,
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
    public int FailureRate { get; set; } = 0; // 0-100, percentage of requests that should fail
    public int BatchCallCount { get; private set; } = 0;
    public List<string> ProcessedTexts { get; } = [];
    public Exception ExceptionToThrow { get; set; } = null;

    // Added helper variables for retry testing
    public int AttemptCount { get; private set; } = 0;
    public int FailForFirstNAttempts { get; set; } = 0;

    private readonly Dictionary<string, object> _attributes = new()
    {
        ["ModelId"] = "test-model",
        ["MaxInputLength"] = 512,
        ["Dimensions"] = 128
    };

    public IReadOnlyDictionary<string, object> Attributes => _attributes;

    public async Task<IList<ReadOnlyMemory<float>>> GenerateEmbeddingsAsync(
        IList<string> texts, CancellationToken cancellationToken = default)
    {
        BatchCallCount++;
        AttemptCount++; // Count this attempt

        // Save processed texts
        lock (ProcessedTexts)
        {
            ProcessedTexts.AddRange(texts);
        }

        // Simulate processing delay
        if (ProcessingDelayMs > 0)
            await Task.Delay(ProcessingDelayMs, cancellationToken);

        // Fail for specific number of attempts if configured
        if (AttemptCount <= FailForFirstNAttempts)
        {
            throw new InvalidOperationException($"Simulated failure on attempt {AttemptCount}");
        }

        // Simulate errors based on failure rate
        if (FailureRate > 0)
        {
            var rand = new Random();
            if (rand.Next(100) < FailureRate)
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

    public Task<IList<ReadOnlyMemory<float>>> GenerateEmbeddingsAsync(
        IList<string> data,
        Kernel kernel = null,
        CancellationToken cancellationToken = new())
    => GenerateEmbeddingsAsync(data, cancellationToken);

    // Reset attempt counter for new tests
    public void ResetAttemptCount()
    {
        AttemptCount = 0;
    }
}

using System;
using System.Collections.Generic;
using System.Data.HashFunction;
using System.Data.HashFunction.Blake2;
using System.Diagnostics.CodeAnalysis;
using Microsoft.SemanticKernel.Embeddings;
using Microsoft.SemanticKernel;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Server.Documents.ETL.Providers.AI.Extensions;

namespace Raven.Server.Documents.ETL.Providers.AI;

public static class AiHelper
{
    internal static readonly List<string> TestValuesList = ["TestValue"];
    private static readonly IBlake2B Hash;

    static AiHelper()
    {
        Hash = Blake2BFactory.Instance.Create(new Blake2BConfig
        {
            HashSizeInBits = 256
        });
    }

    public static string CalculateValueHash(string value)
    {
        return Hash.ComputeHash(value).AsHexString(uppercase: true);
    }

    public static string GetDocumentEmbeddingsId(string documentId)
    {
        return $"{documentId}/embeddings";
    }

    public static string GetDocumentEmbeddingsCollectionName(string sourceCollectionName)
    {
        return $"{sourceCollectionName}/embeddings";
    }

    public static string GetValueEmbeddingsDocumentId(string configurationName, string hash)
    {
        return $"embeddings/{configurationName}/{hash}";
    }

    [Experimental("SKEXP0001")]
    public static ITextEmbeddingGenerationService CreateService(AiEtlConfiguration configuration)
    {
        var kernelBuilder = Kernel.CreateBuilder();
        kernelBuilder.Configure(configuration, isConnectionTest: false, out _);
        var kernel = kernelBuilder.Build();
        return kernel.GetRequiredService<ITextEmbeddingGenerationService>();
    }

    [Experimental("SKEXP0001")]
    public static IServiceProvider CreateServicesForTest(AiEtlConfiguration configuration, out string serviceId)
    {
        var kernelBuilder = Kernel.CreateBuilder();
        kernelBuilder.Configure(configuration, isConnectionTest: true, out serviceId);
        var kernel = kernelBuilder.Build();
        return kernel.Services;
    }

    public static class ServiceIdentifiers
    {
        private const string ProductionPrefix = "ProductionEmbeddingService";
        private const string TestPrefix = "ConnectionTestEmbeddingService";

        public static string Production => ProductionPrefix;

        public static string GenerateTestId() => $"{TestPrefix}_{Guid.NewGuid():N}";
    }
}

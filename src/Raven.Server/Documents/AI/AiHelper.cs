using Microsoft.SemanticKernel.Embeddings;
using Microsoft.SemanticKernel;
using System.Diagnostics.CodeAnalysis;
using System;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.ETL.Providers.AI.Extensions;

namespace Raven.Server.Documents.AI;

public static class AiHelper
{
    [Experimental("SKEXP0001")]
    public static ITextEmbeddingGenerationService CreateService(AiIntegrationConfiguration configuration)
    {
        var kernelBuilder = Kernel.CreateBuilder();
        kernelBuilder.Configure(configuration, isConnectionTest: false, out _);
        var kernel = kernelBuilder.Build();
        return kernel.GetRequiredService<ITextEmbeddingGenerationService>();
    }

    [Experimental("SKEXP0001")]
    public static IServiceProvider CreateServicesForTest(AiIntegrationConfiguration configuration, out string serviceId)
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

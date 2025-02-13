using System;
using System.Collections.Generic;
using System.Data.HashFunction;
using System.Data.HashFunction.Blake2;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using Corax.Utils;
using Microsoft.SemanticKernel.Embeddings;
using Microsoft.SemanticKernel;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.ETL.Providers.AI.Extensions;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.Documents.OngoingTasks;
using Sparrow.Server;

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

    public static string GetPrefixForAttachmentInEmbeddingsDocument(string etlTaskName, string path)
    {
        return $"{etlTaskName}_{path}_";
    }
    
    public static string GetValueEmbeddingsDocumentId(string connectionString, string hash)
    {
        return $"embeddings/{connectionString}/{hash}";
    }
    
#pragma warning disable SKEXP0001
    public static VectorValue GenerateAndEnqueueSingleEmbedding(ITextEmbeddingGenerationService service, ByteStringContext allocator, string textValue, int dimensions)
#pragma warning restore SKEXP0001
    {
        var embedding = service.GenerateEmbeddingAsync(textValue).GetAwaiter().GetResult();

        // todo enqueue embedding to be cached in background
        
        

        // return embedding
        
        var memoryScope = allocator.Allocate(dimensions, out Memory<byte> memory);
            
        MemoryMarshal.AsBytes(embedding.Span).CopyTo(memory.Span);

        return new VectorValue(memoryScope, memory, dimensions);
    }

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

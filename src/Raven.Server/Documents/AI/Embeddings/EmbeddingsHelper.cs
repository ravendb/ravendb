using System;
using System.Collections.Generic;
using System.Data.HashFunction;
using System.Data.HashFunction.Blake2;
using System.Runtime.InteropServices;
using Corax.Utils;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Sparrow.Server;
using Voron.Data.Graphs;

namespace Raven.Server.Documents.AI.Embeddings;

public static class EmbeddingsHelper
{
    internal static readonly List<string> TestValuesList = ["TestValue"];
    private static readonly IBlake2B Hash;

    static EmbeddingsHelper()
    {
        Hash = Blake2BFactory.Instance.Create(new Blake2BConfig
        {
            HashSizeInBits = 256
        });
    }

    public static string CalculateInputValueHash(string value)
    {
        return Hash.ComputeHash(value).AsHexString(uppercase: true);
    }

    public static string GetEmbeddingDocumentId(string documentId)
    {
        return $"{documentId}/embeddings";
    }

    public static string GetEmbeddingDocumentCollectionName(string sourceCollectionName)
    {
        return $"{sourceCollectionName}/embeddings";
    }

    public static string GetPrefixForAttachmentInEmbeddingsDocument(EmbeddingsGenerationTaskIdentifier embeddingsGenerationTaskIdentifier, string path)
    {
        return $"{embeddingsGenerationTaskIdentifier.Value}_{path}_";
    }

    public static string GetEmbeddingCacheDocumentId(AiConnectionStringIdentifier aiConnectionStringIdentifier, string hash)
    {
        return $"embeddings-cache/{aiConnectionStringIdentifier.Value}/{hash}";
    }

#pragma warning disable SKEXP0001
    public static VectorValue GenerateAndEnqueueSingleEmbedding(ITextEmbeddingGenerationService service, ByteStringContext allocator, EmbeddingsStorage embeddingsStorage, string textValue, int dimensions, AiConnectionStringIdentifier connectionStringIdentifier)
#pragma warning restore SKEXP0001
    {
        var embedding = service.GenerateEmbeddingAsync(textValue).GetAwaiter().GetResult();

        //aiStorage.EnqueueEmbeddingToCache(connectionStringIdentifier, textValue, embedding);

        var memoryScope = allocator.Allocate(dimensions, out Memory<byte> memory);

        MemoryMarshal.AsBytes(embedding.Span).CopyTo(memory.Span);

        return new VectorValue(memoryScope, memory, VectorEmbeddingType.Single, dimensions);
    }
}

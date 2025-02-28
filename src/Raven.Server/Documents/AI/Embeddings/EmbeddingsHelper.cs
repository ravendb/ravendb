using System;
using System.Collections.Generic;
using System.Data.HashFunction;
using System.Data.HashFunction.Blake2;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;

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

    public static string GetEmbeddingCacheDocumentId(AiConnectionStringIdentifier aiConnectionStringIdentifier, string hash, in Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType targetQuantization)
    {
        var suffix = targetQuantization switch
        {
            Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType.Single => string.Empty,
            Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType.Int8 => "/int8",
            Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType.Binary => "/binary",
            _ => throw new ArgumentException($"Unknown quantization type '{targetQuantization}'")
        };
        
        return $"embeddings-cache/{aiConnectionStringIdentifier.Value}/{hash}{suffix}";
    }
}

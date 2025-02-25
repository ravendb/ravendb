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
}

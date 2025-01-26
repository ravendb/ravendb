using System;
using System.Data.HashFunction;
using System.Data.HashFunction.Blake2;

namespace Raven.Server.Documents.ETL.Providers.AI;

public static class AiHelper
{
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

    public static string GetCacheDocumentId(string configurationKey, string hash)
    {
        return $"embeddings/{configurationKey}/{hash}";
    }
}

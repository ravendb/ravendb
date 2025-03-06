using System;
using System.Collections.Generic;
using System.Data.HashFunction;
using System.Data.HashFunction.Blake2;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Queries.Vector;
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string GenerateDestinationAttachmentName(in string prefix, in string originalAttachmentName, in VectorEmbeddingType quantization)
    {
        var suffix = quantization switch
        {
            VectorEmbeddingType.Int8 => "_int8",
            VectorEmbeddingType.Binary => "_binary",
            _ => string.Empty
        };
        
        return $"{prefix}{originalAttachmentName}{suffix}";
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

    public static string GetEmbeddingCacheDocumentId(AiConnectionStringIdentifier aiConnectionStringIdentifier, string valueHash, in VectorEmbeddingType targetQuantization)
    {
        var suffix = targetQuantization switch
        {
            Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType.Single => string.Empty,
            Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType.Int8 => "/int8",
            Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType.Binary => "/binary",
            _ => throw new ArgumentException($"Unknown quantization type '{targetQuantization}'")
        };
        
        return $"embeddings-cache/{aiConnectionStringIdentifier.Value}/{valueHash}{suffix}";
    }

    public static EmbeddingValue CreateEmbeddingValue(ReadOnlyMemory<float> embedding, VectorEmbeddingType quantization)
    {
        switch (quantization)
        {
            case VectorEmbeddingType.Single:
                return new EmbeddingValue(embedding, embedding.Length * sizeof(float));
            case VectorEmbeddingType.Int8:
            {   var dest = MemoryMarshal.Cast<float, sbyte>(embedding.Span);
                if (VectorQuantizer.TryToInt8(embedding.Span, dest, out int usedBytes) == false)
                {
                    var newMemory = new ReadOnlyMemory<float>(new float[embedding.Length + 1]);
                    var span = MemoryMarshal.Cast<float, sbyte>(newMemory.Span);
                    var result = VectorQuantizer.TryToInt8(embedding.Span, span, out usedBytes);
                    Debug.Assert(result, "TryToInt8 should always return true");

                    return new EmbeddingValue(newMemory, usedBytes);
                }

                return new EmbeddingValue(embedding, usedBytes);
            }
            case VectorEmbeddingType.Binary:
            {
                var dest = MemoryMarshal.Cast<float, byte>(embedding.Span);
                VectorQuantizer.TryToInt1(embedding.Span, dest, out int usedBytes);

                return new EmbeddingValue(embedding, usedBytes);
            }
            default:
                throw new ArgumentOutOfRangeException($"Quantization type {quantization} is not supported");
        }
    }
}

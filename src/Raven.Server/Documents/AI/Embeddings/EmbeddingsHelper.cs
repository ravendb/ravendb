using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Queries.Vector;
using Raven.Server.Documents.ETL.Providers.AI;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Sparrow.Platform;
using Sparrow.Server.Utils;

namespace Raven.Server.Documents.AI.Embeddings;

public static class EmbeddingsHelper
{
    /// <summary>
    /// Contains predefined test values used to verify connection with Language Models.
    /// </summary>
    /// <remarks>
    /// These values are sent to the language model when testing the connection
    /// through either the "Test Connection" button in the UI or the corresponding API endpoint.
    /// The system uses these values to generate test embeddings and validate that the
    /// language model properly responds to embedding generation requests.
    /// </remarks>
    internal static readonly List<string> ValuesListToVerifyConnection = ["TestValue", "TestValue2"];

    public static string CalculateInputValueHash(string value)
    {
        Span<byte> hashBuffer = stackalloc byte[Sodium.GenericHashSize];
        var valueSpan = MemoryMarshal.Cast<char, byte>(value.AsSpan());

        Sodium.GenericHash(valueSpan, hashBuffer);

        return Convert.ToHexString(hashBuffer);
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

    public static ReadOnlyMemory<byte> CreateEmbeddingValue(ReadOnlyMemory<float> embedding, VectorEmbeddingType quantization)
    {
        switch (quantization)
        {
            case VectorEmbeddingType.Single:
                return MemoryMarshalEx.Cast<float, byte>(embedding);
            case VectorEmbeddingType.Int8:
            {   var dest = MemoryMarshal.Cast<float, sbyte>(embedding.Span);
                if (VectorQuantizer.TryToInt8(embedding.Span, dest, out int usedBytes) == false)
                {
                    var newMemory = new ReadOnlyMemory<float>(new float[embedding.Length + 1]);
                    var span = MemoryMarshal.Cast<float, sbyte>(newMemory.Span);
                    var result = VectorQuantizer.TryToInt8(embedding.Span, span, out usedBytes);
                    Debug.Assert(result, "TryToInt8 should always return true");
        
                    return MemoryMarshalEx.Cast<float, byte>(embedding)[..usedBytes];
                }
                
                return MemoryMarshalEx.Cast<float, byte>(embedding)[..usedBytes];
            }
            case VectorEmbeddingType.Binary:
            {
                var dest = MemoryMarshal.Cast<float, byte>(embedding.Span);
                VectorQuantizer.TryToInt1(embedding.Span, dest, out int usedBytes);
        
                return MemoryMarshalEx.Cast<float, byte>(embedding)[..usedBytes];
            }
            default:
                throw new ArgumentOutOfRangeException($"Quantization type {quantization} is not supported");
        }
    }
}

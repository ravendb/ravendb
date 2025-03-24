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

    [SkipLocalsInit]
    public static string CalculateInputValueHash(string value)
    {
        // Here we take a string, and we:
        // * convert to upper case
        // * trim all white space
        // * generate a hash
        //
        // The idea is that we want to try (as much as possible) to be able
        // to get better hash values, so "keyboard" and "Keyboard " would end up
        // being the same. Note that we are being pretty simplistic here on purpose,
        // to avoid having a complex rule set for "tokenization" these, in addition to
        // the actual tokens
        Span<char> buffer = stackalloc char[256];
        if(value.Length > buffer.Length)
            buffer = new char[value.Length];
        var len = value.AsSpan().Trim().ToUpperInvariant(buffer);
        buffer = buffer[..len];
        Span<byte> hashBuffer = stackalloc byte[Sodium.GenericHashSize];
        var valueSpan = MemoryMarshal.Cast<char, byte>(buffer);

        Sodium.GenericHash(valueSpan, hashBuffer);

        return Convert.ToHexString(hashBuffer);
    }
    
    public static string GetEmbeddingDocumentId(string documentId)
    {
        return $"{documentId}/embeddings";
    }

    public static string GetEmbeddingDocumentCollectionName(string sourceCollectionName)
    {
        return $"{sourceCollectionName}/embeddings";
    }

    public static string GetEmbeddingCacheDocumentId(AiConnectionStringIdentifier id, string valueHash, VectorEmbeddingType targetQuantization)
    {
        var suffix = targetQuantization switch
        {
            VectorEmbeddingType.Single => string.Empty,
            VectorEmbeddingType.Int8 => "/int8",
            VectorEmbeddingType.Binary => "/binary",
            _ => throw new ArgumentException($"Unknown quantization type '{targetQuantization}'")
        };
        
        return $"embeddings-cache/{id.Value}/{valueHash}{suffix}";
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
        
                    return MemoryMarshalEx.Cast<float, byte>(newMemory)[..usedBytes];
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

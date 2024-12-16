using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Queries.Vector;
using SmartComponents.LocalEmbeddings;
using Sparrow;
using Sparrow.Server;
using VectorValue = Corax.Utils.VectorValue;

namespace Raven.Server.Documents.Indexes.VectorSearch;

public static class GenerateEmbeddings
{
    // Dimensions (buffer size) from internals of SmartComponents.
    private const int F32Size = 1536;
    private static readonly LocalEmbedder Embedder = new();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static VectorValue FromText(ByteStringContext allocator, in VectorOptions options, in string text)
    {
        var embedding = CreateEmbeddingViaSmartComponentsLocalEmbedding<EmbeddingF32>(allocator, text, F32Size);
        return options.DestinationEmbeddingType is VectorEmbeddingType.Single 
            ? new VectorValue(embedding.MemoryScope, embedding.Memory, embedding.UsedBytes) : 
            Quantize(allocator, options.DestinationEmbeddingType, embedding.MemoryScope, embedding.Memory, embedding.UsedBytes);
    }

    public static VectorValue FromArray(ByteStringContext allocator, IDisposable memoryScope, Memory<byte> memory, in VectorOptions options, int usedBytes)
    {
        var embeddingSourceType = options.SourceEmbeddingType;
        var embeddingDestinationType = options.DestinationEmbeddingType;
        switch (embeddingSourceType)
        {
            case VectorEmbeddingType.Binary or VectorEmbeddingType.Int8:
                PortableExceptions.ThrowIf<InvalidDataException>(embeddingDestinationType != embeddingSourceType);
                return new VectorValue(memoryScope, memory, usedBytes);
            case VectorEmbeddingType.Single when embeddingDestinationType is VectorEmbeddingType.Single:
                return new (memoryScope, memory, usedBytes);
            default:
                return Quantize(allocator, options.DestinationEmbeddingType, memoryScope, memory, usedBytes);
        }
    }

    public static VectorValue FromBase64Array(in VectorOptions options, ByteStringContext allocator, string base64)
    {
        var bytesRequired = (int)Math.Ceiling((base64.Length * 3) / 4.0); //this is approximation
        var memScope = allocator.Allocate(bytesRequired, out Memory<byte> mem);
        var result = Convert.TryFromBase64String(base64, mem.Span, out var bytesWritten);
        PortableExceptions.ThrowIf<InvalidDataException>(result == false, $"Excepted array encoded with base64, however got: '{base64}'");
        return FromArray(allocator, memScope, mem, options, bytesWritten);
    }

    private static VectorValue Quantize(ByteStringContext allocator, in VectorEmbeddingType destinationFormat,
        IDisposable memoryScope,
        Memory<byte> memory, int usedBytes)
    {
        if (destinationFormat is VectorEmbeddingType.Single)
            return new VectorValue(memoryScope, memory, usedBytes);

        VectorValue embeddings;
        var source = MemoryMarshal.Cast<byte, float>(memory.Span.Slice(0, usedBytes));
        
        switch (destinationFormat)
        {
            case VectorEmbeddingType.Int8:
            {
                var dest = MemoryMarshal.Cast<byte, sbyte>(memory.Span);
                if (dest.Length < source.Length + sizeof(float))
                {
                    var requestedSize = dest.Length + sizeof(float);
                    var mem = allocator.Allocate(requestedSize, out System.Memory<byte> buffer);
                    VectorQuantizer.TryToInt8(source, MemoryMarshal.Cast<byte, sbyte>(buffer.Span), out usedBytes);

                    embeddings = new VectorValue(mem, buffer);
                    memoryScope.Dispose();
                }
                else
                {
                    VectorQuantizer.TryToInt8(source, dest, out usedBytes);
                    embeddings = new VectorValue(memoryScope, memory, usedBytes);
                }

                embeddings.OverrideLength(usedBytes);
                break;
            }
            case VectorEmbeddingType.Binary:
            {
                var dest = MemoryMarshal.Cast<byte, byte>(memory.Span);
                VectorQuantizer.TryToInt1(source, dest, out usedBytes);
                embeddings = new VectorValue(memoryScope, memory, usedBytes);
                break;
            }
            case VectorEmbeddingType.Single:
            case VectorEmbeddingType.Text:
            default:
                throw new ArgumentOutOfRangeException(nameof(destinationFormat), destinationFormat, null);
        }

        return embeddings;
    }

    private static (IDisposable MemoryScope, Memory<byte> Memory, int UsedBytes) CreateEmbeddingViaSmartComponentsLocalEmbedding<TEmbedding>(ByteStringContext allocator, in string text, in int dimensions)
        where TEmbedding : IEmbedding<TEmbedding>
    {
        var memoryScope = allocator.Allocate(dimensions, out System.Memory<byte> memory);
        Embedder.Embed<TEmbedding>(text, memory);
        return (memoryScope, memory, dimensions);
    }
}

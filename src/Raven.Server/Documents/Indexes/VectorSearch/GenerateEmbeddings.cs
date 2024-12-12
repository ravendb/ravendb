using System;
using System.Buffers;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Utils;
using Jint;
using Jint.Native;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Vector;
using SmartComponents.LocalEmbeddings;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;

namespace Raven.Server.Documents.Indexes.VectorSearch;

public static class GenerateEmbeddings
{
    // Dimensions (buffer size) from internals of SmartComponents.
    private const int I1Size = 48;
    private const int I8Size = 388;
    private const int F32Size = 1536;

    [ThreadStatic] // avoid convoys in querying
    private static ArrayPool<byte> Allocator;

    private static readonly LocalEmbedder Embedder = new();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static VectorValue FromText(in VectorOptions options, in string text)
    {
        Allocator ??= ArrayPool<byte>.Create();
        var embeddings = CreateEmbeddingViaSmartComponentsLocalEmbedding<EmbeddingF32>(text, F32Size);

        switch (options.DestinationEmbeddingType)
        {
            case VectorEmbeddingType.Int8:
            {
                var source = MemoryMarshal.Cast<byte, float>(embeddings.GetEmbedding());
                var dest = MemoryMarshal.Cast<byte, sbyte>(embeddings.GetEmbedding());

                int usedBytes;
                if (dest.Length < source.Length + sizeof(float))
                {
                    var mem = Allocator.Rent(dest.Length + sizeof(float));
                    VectorQuantizer.TryToInt8(source, MemoryMarshal.Cast<byte, sbyte>(mem), out usedBytes);

                    var newDest = new VectorValue(Allocator, mem, mem);
                    embeddings.Dispose();
                    embeddings = newDest;
                }
                else
                {
                    VectorQuantizer.TryToInt8(source, dest, out usedBytes);
                }

                embeddings.OverrideLength(usedBytes);
                break;
            }
            case VectorEmbeddingType.Binary:
            {
                var source = MemoryMarshal.Cast<byte, float>(embeddings.GetEmbedding());
                var dest = MemoryMarshal.Cast<byte, byte>(embeddings.GetEmbedding());
                VectorQuantizer.TryToInt1(source, dest, out int usedBytes);
                embeddings.OverrideLength(usedBytes);
                break;
            }
        }

        return embeddings;
    }

    public static VectorValue FromArray(in VectorOptions options, ByteStringContext allocator, string base64)
    {
        var bytesRequired = (int)Math.Ceiling((base64.Length * 3) / 4.0); //this is approximation
        var memScope = allocator.Allocate(bytesRequired, out ByteString mem);
        var result = Convert.TryFromBase64String(base64, mem.ToSpan(), out var bytesWritten);
        PortableExceptions.ThrowIf<InvalidDataException>(result == false, $"Excepted array encoded with base64, however got: '{base64}'");
        return FromArray(options, memScope, mem, bytesWritten);
    }

    public static VectorValue FromArray(in VectorOptions options, ByteStringContext<ByteStringMemoryCache>.InternalScope disposable, ByteString mem, int usedBytes)
    {
        var embeddingSourceType = options.SourceEmbeddingType;
        var embeddingDestinationType = options.DestinationEmbeddingType;
        if (embeddingSourceType is VectorEmbeddingType.Binary or VectorEmbeddingType.Int8)
        {
            PortableExceptions.ThrowIf<InvalidDataException>(embeddingDestinationType != embeddingSourceType);
            return new VectorValue(disposable, mem, usedBytes);
        }

        Allocator ??= ArrayPool<byte>.Create();

        switch (embeddingDestinationType)
        {
            case VectorEmbeddingType.Single:
            {
                return new VectorValue(disposable, mem, usedBytes);
            }
            case VectorEmbeddingType.Int8:
            {
                var destination = mem.ToSpan<sbyte>();

                if (mem.Length < usedBytes + sizeof(float))
                {
                    var rentedMem = Allocator.Rent(mem.Length + sizeof(float));

                    destination = MemoryMarshal.Cast<byte, sbyte>(rentedMem);
                }

                bool result = VectorQuantizer.TryToInt8(mem.ToSpan<float>().Slice(0, usedBytes / sizeof(float)), destination, out usedBytes);
                PortableExceptions.ThrowIfNot<InvalidDataException>(result, "Error during quantization of the array.");
                return new VectorValue(disposable, mem, usedBytes);
            }
            case VectorEmbeddingType.Binary:
            {
                bool result = VectorQuantizer.TryToInt1(mem.ToSpan<float>().Slice(0, usedBytes / sizeof(float)), mem.ToSpan<byte>(), out usedBytes);
                PortableExceptions.ThrowIfNot<InvalidDataException>(result, "Error during quantization of the array.");
                return new VectorValue(disposable, mem, usedBytes);
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(embeddingDestinationType), embeddingDestinationType, null);
        }
    }

    private static VectorValue CreateEmbeddingViaSmartComponentsLocalEmbedding<TEmbedding>(in string text, in int dimensions)
        where TEmbedding : IEmbedding<TEmbedding>
    {
        var currentAllocator = Allocator ??= ArrayPool<byte>.Create();
        var buffer = currentAllocator.Rent(dimensions);
        var embedding = new Memory<byte>(buffer, 0, dimensions);
        Embedder.Embed<TEmbedding>(text, embedding);
        return new VectorValue(currentAllocator, buffer, embedding);
    }
}

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
        var embeddings = CreateSmartComponentsLocalEmbedding<EmbeddingF32>(text, F32Size);

        switch (options.DestinationEmbeddingType)
        {
            case VectorEmbeddingType.Int8:
            {
                var source = MemoryMarshal.Cast<byte, float>(embeddings.GetEmbedding());
                var dest = MemoryMarshal.Cast<byte, sbyte>(embeddings.GetEmbedding());
                VectorQuantizer.TryToInt8(source, dest, out int usedBytes);
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
        var bytesRequired = (base64.Length * 3) / 4; //this is approximation
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
        
        switch (embeddingDestinationType)
        {
            case VectorEmbeddingType.Single:
            {
                return new VectorValue(disposable, mem, usedBytes);
            }
            case VectorEmbeddingType.Int8:
            {
                bool result = VectorQuantizer.TryToInt8(mem.ToSpan<float>().Slice(0, usedBytes), mem.ToSpan<sbyte>(), out usedBytes);
                PortableExceptions.ThrowIf<InvalidDataException>(result, $"Error during quantization of the array.");
                return new VectorValue(disposable, mem, usedBytes);
            }
            case VectorEmbeddingType.Binary:
            {
                bool result = VectorQuantizer.TryToInt1(mem.ToSpan<float>().Slice(0, usedBytes), mem.ToSpan<byte>(), out usedBytes);
                PortableExceptions.ThrowIf<InvalidDataException>(result, $"Error during quantization of the array.");
                return new VectorValue(disposable, mem, usedBytes);
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(embeddingDestinationType), embeddingDestinationType, null);
        }
    }

    private static void PerformQuantization(in VectorOptions options, ReadOnlySpan<byte> source)
    {
        
    }
    
    private static VectorValue CreateSmartComponentsLocalEmbedding<TEmbedding>(in string text, in int dimensions)
        where TEmbedding : IEmbedding<TEmbedding>
    {
        var currentAllocator = Allocator ??= ArrayPool<byte>.Create();
        var buffer = currentAllocator.Rent(dimensions);
        var embedding = new Memory<byte>(buffer, 0, dimensions);
        Embedder.Embed<TEmbedding>(text, embedding);
        return new VectorValue(currentAllocator, buffer, embedding);
    }


#pragma warning disable SKEXP0070 // ignore experimental warning 
        // private static readonly Lazy<BertOnnxTextEmbeddingGenerationService> EmbeddingGenerator = new(LoadOnnxModel);
        // private static BertOnnxTextEmbeddingGenerationService LoadOnnxModel()
        // {
        //     // TODO: Figure out distribution model
        //     // https://huggingface.co/SmartComponents/bge-micro-v2/resolve/72908b7/onnx/model_quantized.onnx
        //     // https://huggingface.co/SmartComponents/bge-micro-v2/resolve/72908b7/vocab.txt
        //     
        //     return BertOnnxTextEmbeddingGenerationService.Create(
        //         "C:\\Users\\ayende\\Downloads\\model_quantized.onnx",
        //         vocabPath: "C:\\Users\\ayende\\Downloads\\vocab.txt",
        //         new BertOnnxOptions { CaseSensitive = false, MaximumTokens = 512 });
        // }
        //
        // public static byte[] UsingI8(string str)
        // {
        //     var service = EmbeddingGenerator.Value;
        //     Task<IList<ReadOnlyMemory<float>>> generateEmbeddingsAsync = service.GenerateEmbeddingsAsync([str]);
        //     ReadOnlyMemory<float> readOnlyMemory = generateEmbeddingsAsync.Result.Single();
        //     var buffer = new byte[readOnlyMemory.Length / 8];
        //     QuantizeI8(readOnlyMemory.Span, buffer);
        //     return buffer;
        // }
        //
        //
        // private static void QuantizeI8(ReadOnlySpan<float> input, Span<byte> result)
        // {
        //     // https://github.com/dotnet/smartcomponents/blob/4dbb671443c84407b598a0104441afd1186d9a3a/src/SmartComponents.LocalEmbeddings/EmbeddingI1.cs
        //     var inputLength = input.Length;
        //     for (var j = 0; j < inputLength; j += 8)
        //     {
        //         // Vectorized approaches don't seem to get even close to the
        //         // speed of doing it in this naive way
        //         var sources = input.Slice(j, 8);
        //         var sum = (byte)0;
        //
        //         if (float.IsPositive(sources[0])) { sum |= 128; }
        //
        //         if (float.IsPositive(sources[1])) { sum |= 64; }
        //
        //         if (float.IsPositive(sources[2])) { sum |= 32; }
        //
        //         if (float.IsPositive(sources[3])) { sum |= 16; }
        //
        //         if (float.IsPositive(sources[4])) { sum |= 8; }
        //
        //         if (float.IsPositive(sources[5])) { sum |= 4; }
        //
        //         if (float.IsPositive(sources[6])) { sum |= 2; }
        //
        //         if (float.IsPositive(sources[7])) { sum |= 1; }
        //
        //         result[j / 8] = sum;
        //     }
        // }

#pragma warning restore SKEXP0070
    }

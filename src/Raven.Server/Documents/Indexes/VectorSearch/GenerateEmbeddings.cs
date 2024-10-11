using System;
using System.Buffers;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Raven.Client.Documents.Indexes.Vector;
using SmartComponents.LocalEmbeddings;
using Sparrow;

namespace Raven.Server.Documents.Indexes.VectorSearch;

public static class GenerateEmbeddings
{
    //Dimensions (buffer size) from internals of SmartComponents.
    private const int I1Size = 48;
    private const int I8Size = 388;
    private const int F32Size = 1536;


    [ThreadStatic] //avoid convoys in querying
    private static ArrayPool<byte> Allocator;

    private static readonly LocalEmbedder Embedder = new();


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static VectorValue FromText(in EmbeddingType embeddingType, in string text)
    {
        return embeddingType switch
        {
            EmbeddingType.Float32 => CreateSmartComponentsLocalEmbedding<EmbeddingF32>(text, F32Size),
            EmbeddingType.Int8 => CreateSmartComponentsLocalEmbedding<EmbeddingI8>(text, I8Size),
            EmbeddingType.Binary => CreateSmartComponentsLocalEmbedding<EmbeddingI1>(text, I1Size),
            _ => throw new ArgumentOutOfRangeException(nameof(embeddingType), embeddingType, null)
        };
    }

    public static unsafe VectorValue FromArray<T>(in EmbeddingType embeddingSourceType, in EmbeddingType embeddingDestinationType, in T[] array)
        where T : unmanaged
    {
        if (embeddingSourceType != EmbeddingType.Float32)
        {
            if (embeddingSourceType == EmbeddingType.Binary)
            {
                if (typeof(T) == typeof(byte))
                    return new VectorValue(arrayPool: null, (byte[])(object)array, (byte[])(object)array);


                var bytes = MemoryMarshal.Cast<T, byte>(array);
                var allocator = Allocator ??= ArrayPool<byte>.Create();
                var buffer = allocator.Rent(bytes.Length);
                bytes.CopyTo(buffer.AsSpan());
            }
        }

        PortableExceptions.ThrowIf<InvalidDataException>(typeof(T) != typeof(float),
            $"Quantization {EmbeddingType.Binary} is expecting a float array but got '{array.GetType().FullName}'.");

        switch (embeddingDestinationType)
        {
            case EmbeddingType.Binary:
            {
                PortableExceptions.Throw<NotSupportedException>("Not supported embedding destination type");
                return default;
            }
            case EmbeddingType.Int8:
            {
                PortableExceptions.Throw<NotSupportedException>("Not supported embedding destination type");
                return default;
            }
            default:
            {
                var embeddings = (float[])(object)array;
                var currentAllocator = Allocator ??= ArrayPool<byte>.Create();
                int bytesRequires = embeddings.Length; //todo store norm
                byte[] buffer = currentAllocator.Rent(bytesRequires);
                MemoryMarshal.Cast<float, byte>(embeddings).CopyTo(buffer);
                return new VectorValue(currentAllocator, buffer, new Memory<byte>(buffer, 0, bytesRequires));
            }
        }
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

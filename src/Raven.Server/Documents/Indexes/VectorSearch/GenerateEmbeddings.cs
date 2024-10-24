using System;
using System.Buffers;
using System.IO;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Jint;
using Jint.Native;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Queries;
using SmartComponents.LocalEmbeddings;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.VectorSearch;

public static class VectorUtils
{
    public static T GetNumerical<T>(object value)
    {
        switch (value)
        {
            case LazyStringValue lsv when typeof(T) == typeof(float):
                return (T)(object)(float)lsv;
            case LazyStringValue lsv when typeof(sbyte) == typeof(T):
                return (T)(object)Convert.ToSByte((int)lsv);
            case LazyStringValue lsv when typeof(byte) == typeof(T):
                return (T)(object)(byte)Convert.ToByte((int)lsv);
            case long l when typeof(T) == typeof(float):
                return (T)(object)Convert.ToSingle(l);
            case long l when typeof(sbyte) == typeof(T):
                return (T)(object)Convert.ToSByte(l);
            case long l when typeof(byte) == typeof(T):
                return (T)(object)Convert.ToByte(l);
            case float f when typeof(T) == typeof(float):
                return (T)(object)f;
            case float f when typeof(sbyte) == typeof(T):
                return (T)(object)Convert.ToSByte(f);
            case float f when typeof(byte) == typeof(T):
                return (T)(object)Convert.ToByte(f);
            case byte b when typeof(T) == typeof(float):
                return (T)(object)Convert.ToSingle(b);
            case byte b when typeof(sbyte) == typeof(T):
                return (T)(object)Convert.ToSByte(b);
            case byte b when typeof(byte) == typeof(T):
                return (T)(object)Convert.ToByte(b);
            case sbyte sb when typeof(T) == typeof(float):
                return (T)(object)Convert.ToSingle(sb);
            case sbyte sb when typeof(sbyte) == typeof(T):
                return (T)(object)Convert.ToSByte(sb);
            case sbyte sb when typeof(byte) == typeof(T):
                return (T)(object)Convert.ToByte(sb);
            case LazyNumberValue lnv when typeof(T) == typeof(float):
                return (T)(object)Convert.ToSingle((float)lnv);
            case LazyNumberValue lnv when typeof(sbyte) == typeof(T):
                return (T)(object)Convert.ToSByte((sbyte)lnv);
            case LazyNumberValue lnv when typeof(byte) == typeof(T):
                return (T)(object)Convert.ToByte((byte)(sbyte)lnv);
            case JsValue jsn:
                value = jsn.AsNumber();
                break;
        }

        if (value is double d)
        {
            if (typeof(T) == typeof(float))
                return (T)(object)Convert.ToSingle(d);

            if (typeof(sbyte) == typeof(T))
                return (T)(object)Convert.ToSByte(d);

            if (typeof(byte) == typeof(T))
                return (T)(object)Convert.ToByte(d);
        }

        PortableExceptions.Throw<InvalidDataException>($"Type of T is expected to be either {typeof(float)}, {typeof(sbyte)} or {typeof(byte)}. Got {value.GetType().FullName} instead.");
        return default;
    }
}

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
        return options.DestinationEmbeddingType switch
        {
            EmbeddingType.Single => CreateSmartComponentsLocalEmbedding<EmbeddingF32>(text, F32Size),
            EmbeddingType.Int8 => CreateSmartComponentsLocalEmbedding<EmbeddingI8>(text, I8Size),
            EmbeddingType.Binary => CreateSmartComponentsLocalEmbedding<EmbeddingI1>(text, I1Size),
            _ => throw new NotSupportedException($"Unsupported {nameof(options.DestinationEmbeddingType)}: {options.DestinationEmbeddingType}")
        };
    }

    public static unsafe VectorValue FromArray<T>(in VectorOptions options, in T[] array)
        where T : unmanaged
    {
        var embeddingSourceType = options.SourceEmbeddingType;
        var embeddingDestinationType = options.DestinationEmbeddingType;
        
        if (embeddingSourceType is EmbeddingType.Binary)
        {
            PortableExceptions.ThrowIf<InvalidDataException>(typeof(T) != typeof(byte), $"Data already quantized in '{EmbeddingType.Binary}' form should be of type '{typeof(byte)}'.");
            return new VectorValue(arrayPool: null, (byte[])(object)array, (byte[])(object)array);
        }
        
        if (embeddingSourceType is EmbeddingType.Int8)
        {
            if (typeof(T) == typeof(byte))
                return new VectorValue(arrayPool: null, (byte[])(object)array, new Memory<byte>((byte[])(object)array, 0, array.Length));
            
            PortableExceptions.ThrowIf<InvalidDataException>(typeof(T) != typeof(sbyte), $"Data already quantized in '{EmbeddingType.Int8}' form should be of type '{typeof(sbyte)}'.");
            var bytes = MemoryMarshal.Cast<T, byte>(array);
            var allocator = Allocator ??= ArrayPool<byte>.Create();
            var buffer = allocator.Rent(bytes.Length);
            bytes.CopyTo(buffer.AsSpan());
            
            return new VectorValue(arrayPool: allocator, buffer, new Memory<byte>(buffer, 0, array.Length));
        }
        
        switch (embeddingDestinationType)
        {
            case EmbeddingType.Binary:
            {
                PortableExceptions.Throw<NotSupportedException>("TODO");
                return default;
            }
            case EmbeddingType.Int8:
            {
                PortableExceptions.Throw<NotSupportedException>("TODO");
                return default;
            }
            default:
            {
                if (typeof(T) == typeof(byte))
                {
                    return new VectorValue(arrayPool:null, (byte[])(object)array, (byte[])(object)array);
                }
                
                var embeddings = (float[])(object)array;
                var currentAllocator = Allocator ??= ArrayPool<byte>.Create();
                int bytesRequires = embeddings.Length * sizeof(float); //todo store norm
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

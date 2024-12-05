using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;

namespace Raven.Client.Documents.Queries.Vector;

public class VectorQuantizer
{
    private static ReadOnlySpan<byte> BinaryQuantizerLookup => [0b_1000_0000, 0b_0100_0000, 0b_0010_0000, 0b_0001_0000, 0b_0000_1000, 0b_0000_0100, 0b_0000_0010, 0b_0000_0001];

    public static unsafe bool TryToInt8(ReadOnlySpan<float> rawEmbedding, ReadOnlySpan<sbyte> destination, out int usedBytes)
    {
        usedBytes = 0;
        if (destination.Length < rawEmbedding.Length)
            return false;
        ref var sourceRef = ref MemoryMarshal.GetReference(rawEmbedding);
        var maxComponent = float.MinValue;
        for (int i = 0; i < rawEmbedding.Length; i++)
            maxComponent = Math.Max(maxComponent, Math.Abs(Unsafe.Add(ref sourceRef, i)));
        
        var scaleFactor = 127f / maxComponent;
        ref var destinationRef = ref MemoryMarshal.GetReference(destination);
        
        for (var i = 0; i < rawEmbedding.Length; i++)
        {
            var scaledValue = Unsafe.Add(ref sourceRef, i) * scaleFactor;
            Unsafe.Add(ref destinationRef, i) = Convert.ToSByte(scaledValue);
        }
        
        // Store magnitude as four last sbytes
        Unsafe.As<sbyte, float>(ref Unsafe.Add(ref destinationRef, rawEmbedding.Length)) = maxComponent;
        
        usedBytes = rawEmbedding.Length + sizeof(float);
        return true;
    }

    public static sbyte[] ToInt8(float[] rawEmbedding)
    {
        var mem = new sbyte[rawEmbedding.Length + sizeof(float)];
        TryToInt8(rawEmbedding, mem, out _);
        return mem;
    }
    
    public static byte[] ToInt1(ReadOnlySpan<float> rawEmbedding)
    {
        const byte dimensionsInOneByte = 8;
        var outputLength = ((int)rawEmbedding.Length / dimensionsInOneByte + (rawEmbedding.Length % dimensionsInOneByte != 0).ToInt32());
        var bytes = new byte[outputLength];
        TryToInt1(rawEmbedding, bytes, out _);
        return bytes;
    }
    
    internal static bool TryToInt1(ReadOnlySpan<float> source, ReadOnlySpan<byte> destination, out int usedBytes)
    {
        const byte dimensionsInOneByte = 8;
        var inputLength = (nuint)source.Length;
        var outputLength = (nuint)((int)inputLength / dimensionsInOneByte + ((int)inputLength % dimensionsInOneByte != 0).ToInt32());

        if ((int)outputLength > destination.Length)
        {
            usedBytes = 0;
            return false;
        }
        
        ref var resultRef = ref MemoryMarshal.GetReference(destination);
        ref var embeddingRef = ref MemoryMarshal.GetReference(source);
        ref var lookupTableRef = ref MemoryMarshal.GetReference(BinaryQuantizerLookup);
        for (nuint j = 0; j < inputLength; j++)
        {
            var result = Unsafe.Add(ref embeddingRef, j) >= 0 
                ? Unsafe.AddByteOffset(ref lookupTableRef, j % dimensionsInOneByte) 
                : byte.MinValue;

            // In case when source and destination is the same memory we've to clean it first
            if (j % dimensionsInOneByte == 0)
                Unsafe.AddByteOffset(ref resultRef, j / dimensionsInOneByte) = 0;

            Unsafe.AddByteOffset(ref resultRef, j / dimensionsInOneByte) |= result;
        }

        usedBytes = (int)outputLength;
        return true;
    }
}

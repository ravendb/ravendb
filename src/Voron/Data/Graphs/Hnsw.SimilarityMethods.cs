using System;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using NotImplementedException = System.NotImplementedException;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    public enum SimilarityMethod : byte
    {
        CosineSimilaritySingles = 0,
        CosineSimilarityI8 = 1,
        HammingDistance = 2,
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static float CosineSimilaritySingles(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        var aSingles = MemoryMarshal.Cast<byte, float>(a);
        var bSingles = MemoryMarshal.Cast<byte, float>(b);
        return 1f - TensorPrimitives.CosineSimilarity(aSingles, bSingles);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static float CosineSimilarityI8(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        // assert |a| == |b|
        
        var vectorLength = a.Length - sizeof(float);
        
        ref var aRef = ref MemoryMarshal.GetReference(a);
        ref var bRef = ref MemoryMarshal.GetReference(b);
        
        var magA = Unsafe.ReadUnaligned<float>(ref Unsafe.AddByteOffset(ref aRef, a.Length - sizeof(float)));
        var magB = Unsafe.ReadUnaligned<float>(ref Unsafe.AddByteOffset(ref bRef, b.Length - sizeof(float)));
        
        var alpha1 = magA / 127f;
        var alpha2 = magB / 127f;
        
        float dotProduct = alpha1 * alpha2 * vectorLength;

        for (int i = 0; i < vectorLength; i++)
        {
            dotProduct += alpha1 * a[i];
            dotProduct += alpha2 * b[i];
            dotProduct += a[i] * b[i];
        }

        float sq1 = 0;
        float sq2 = 0;
        
        for (int i = 0; i < vectorLength; i++)
        {
            sq1 += a[i] * a[i];
            sq2 += b[i] * b[i];
        }
        
        sq1 = MathF.Sqrt(sq1);
        sq2 = MathF.Sqrt(sq2);
        
        return 1f - (dotProduct / (sq1 * sq2));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static float HammingDistance(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        return TensorPrimitives.HammingBitDistance<byte>(a, b);
    }
}

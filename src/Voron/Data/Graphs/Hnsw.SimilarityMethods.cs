using System;
using System.Diagnostics;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

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
        Debug.Assert(a.Length == b.Length, "a.Length == b.Length");
        var vectorLength = a.Length - sizeof(float);
        
        ref var aRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte,sbyte>(a[..vectorLength]));
        ref var bRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, sbyte>(b[..vectorLength]));
        
        var magA = Unsafe.ReadUnaligned<float>(ref MemoryMarshal.GetReference(a[vectorLength..]));
        var magB = Unsafe.ReadUnaligned<float>(ref MemoryMarshal.GetReference(b[vectorLength..]));
        
        var alpha1 = magA / 127f;
        var alpha2 = magB / 127f;
        
        float dotProduct = alpha1 * alpha2 * vectorLength;

        float sq1 = 0;
        float sq2 = 0;
        
        for (int i = 0; i < vectorLength; i++)
        {
            var aValue = Unsafe.Add(ref aRef, i);
            var bValue = Unsafe.Add(ref bRef, i);
            dotProduct += alpha1 * aValue;
            dotProduct += alpha2 * bValue;
            dotProduct += aValue * bValue;
            
            sq1 += aValue * aValue;
            sq2 += bValue * bValue;
        }
        
        sq1 = MathF.Sqrt(sq1);
        sq2 = MathF.Sqrt(sq2);

        return 1f - Math.Clamp(dotProduct / (sq1 * sq2), -1f, 1f);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static float HammingDistance(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        return TensorPrimitives.HammingBitDistance<byte>(a, b);
    }
}

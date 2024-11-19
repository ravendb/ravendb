using System;
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
    private static float CosineSimilaritySingles(Span<byte> a, Span<byte> b) => 1 - TensorPrimitives.CosineSimilarity(MemoryMarshal.Cast<byte, float>(a), MemoryMarshal.Cast<byte, float>(b));
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static float CosineSimilarityI8(Span<byte> a, Span<byte> b)
    {
        throw new NotImplementedException("Not implemented yet");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static float HammingDistance(Span<byte> a, Span<byte> b)
    {
        return TensorPrimitives.HammingBitDistance<byte>(a, b);
    }
}

using System;
using System.Diagnostics;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Sparrow;

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

        ref var aRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, sbyte>(a[..vectorLength]));
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
    
    internal static void DistanceToScoreHammingSimilarity(Span<float> scores, int vectorSizeInBytes)
    {
        var pos = 0;
        ref float bufferRef = ref MemoryMarshal.GetReference(scores);
        int N = 0;

        if (AdvInstructionSet.IsAcceleratedVector512)
        {
            var divisor = Vector512.Create(8f * vectorSizeInBytes);
            N = Vector512<float>.Count;
            for (; pos + N < scores.Length; pos += N)
            {
                ref var currentPos = ref Unsafe.Add(ref bufferRef, pos);
                var currentScores = Vector512.LoadUnsafe(ref currentPos);
                var divide = Vector512.Divide(currentScores, divisor);
                var result = Vector512.Subtract(Vector512.Create(1F), divide);
                result.StoreUnsafe(ref currentPos);
            }
        }

        if (AdvInstructionSet.IsAcceleratedVector256)
        {
            var divisor = Vector256.Create(8f * vectorSizeInBytes);
            N = Vector256<float>.Count;
            for (; pos + N < scores.Length; pos += N)
            {
                ref var currentPos = ref Unsafe.Add(ref bufferRef, pos);
                var currentScores = Vector256.LoadUnsafe(ref currentPos);
                var divide = Vector256.Divide(currentScores, divisor);
                var result = Vector256.Subtract(Vector256.Create(1F), divide);
                result.StoreUnsafe(ref currentPos);
            }
        }

        if (AdvInstructionSet.IsAcceleratedVector128)
        {
            var divisor = Vector128.Create(8f * vectorSizeInBytes);
            N = Vector128<float>.Count;
            for (; pos + N < scores.Length; pos += N)
            {
                ref var currentPos = ref Unsafe.Add(ref bufferRef, pos);
                var currentScores = Vector128.LoadUnsafe(ref currentPos);
                var divide = Vector128.Divide(currentScores, divisor);
                var result = Vector128.Subtract(Vector128.Create(1F), divide);
                result.StoreUnsafe(ref currentPos);
            }
        }

        for (; pos < scores.Length; pos++)
            Unsafe.Add(ref bufferRef, pos) = 1f - (Unsafe.Add(ref bufferRef, pos) / (8f * vectorSizeInBytes));
    }

    internal static void DistanceToScoreCosineSimilarity(Span<float> scores)
    {
        var pos = 0;
        ref float bufferRef = ref MemoryMarshal.GetReference(scores);
        int N = 0;

        if (AdvInstructionSet.IsAcceleratedVector512)
        {
            N = Vector512<float>.Count;
            for (; pos + N < scores.Length; pos += N)
            {
                ref var currentPos = ref Unsafe.Add(ref bufferRef, pos);
                var currentScores = Vector512.LoadUnsafe(ref currentPos);
                var result = Vector512.Subtract(Vector512.Create(1F), currentScores);
                result.StoreUnsafe(ref currentPos);
            }
        }

        if (AdvInstructionSet.IsAcceleratedVector256)
        {
            N = Vector256<float>.Count;
            for (; pos + N < scores.Length; pos += N)
            {
                ref var currentPos = ref Unsafe.Add(ref bufferRef, pos);
                var currentScores = Vector256.LoadUnsafe(ref currentPos);
                var result = Vector256.Subtract(Vector256.Create(1F), currentScores);
                result.StoreUnsafe(ref currentPos);
            }
        }

        if (AdvInstructionSet.IsAcceleratedVector128)
        {
            N = Vector128<float>.Count;
            for (; pos + Vector128<float>.Count < scores.Length; pos += N)
            {
                ref var currentPos = ref Unsafe.Add(ref bufferRef, pos);
                var currentScores = Vector128.LoadUnsafe(ref currentPos);
                var result = Vector128.Subtract(Vector128.Create(1F), currentScores);
                result.StoreUnsafe(ref currentPos);
            }
        }

        for (; pos < scores.Length; pos++)
            Unsafe.Add(ref bufferRef, pos) = 1 - Unsafe.Add(ref bufferRef, pos);
    }
}

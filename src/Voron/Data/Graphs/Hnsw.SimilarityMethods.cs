using System;
using System.Diagnostics;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Sparrow;
using Sparrow.Server.Tensors;

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
    internal static float CosineDistanceSingles(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        var aSingles = MemoryMarshal.Cast<byte, float>(a);
        var bSingles = MemoryMarshal.Cast<byte, float>(b);
        return Functions.CosineDistance(aSingles, bSingles);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static float CosineDistanceI8(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        Debug.Assert(a.Length == b.Length, "a.Length == b.Length");
        var vectorLength = a.Length - sizeof(float);

        var aRef = MemoryMarshal.Cast<byte, sbyte>(a[..vectorLength]);
        var bRef = MemoryMarshal.Cast<byte, sbyte>(b[..vectorLength]);

        var aMag = Unsafe.ReadUnaligned<float>(ref MemoryMarshal.GetReference(a[vectorLength..]));
        var bMag = Unsafe.ReadUnaligned<float>(ref MemoryMarshal.GetReference(b[vectorLength..]));

        return Functions.CosineDistance(aRef, aMag, bRef, bMag);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static float HammingDistance(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        return Functions.HammingBitDistance(a, b);
    }
    
    internal static void DistanceToScoreHamming(Span<float> scores, int vectorSizeInBytes)
    {
        var pos = 0;
        ref float bufferRef = ref MemoryMarshal.GetReference(scores);
        int N = 0;

        if (AdvInstructionSet.IsAcceleratedVector512 && scores.Length <= Vector512<float>.Count)
        {
            var divisor = Vector512.Create(8f * vectorSizeInBytes);
            N = Vector512<float>.Count;
            for (; pos + N < scores.Length; pos += N)
            {
                ref var currentPos = ref Unsafe.Add(ref bufferRef, pos);
                var currentScores = Vector512.LoadUnsafe(ref currentPos);
                var divide = Vector512.Divide(currentScores, divisor);
                var result = Vector512.Subtract(Vector512.Create(1F), divide);
                result = Vector512.Add(result, currentScores);
                result.StoreUnsafe(ref currentPos);
            }
        }

        if (AdvInstructionSet.IsAcceleratedVector256 && scores.Length - pos <= Vector256<float>.Count)
        {
            var divisor = Vector256.Create(8f * vectorSizeInBytes);
            N = Vector256<float>.Count;
            for (; pos + N < scores.Length; pos += N)
            {
                ref var currentPos = ref Unsafe.Add(ref bufferRef, pos);
                var currentScores = Vector256.LoadUnsafe(ref currentPos);
                var divide = Vector256.Divide(currentScores, divisor);
                var result = Vector256.Subtract(Vector256.Create(1F), divide);
                result = Vector256.Add(result, currentScores);
                result.StoreUnsafe(ref currentPos);
            }
        }

        if (AdvInstructionSet.IsAcceleratedVector128 && scores.Length - pos <= Vector128<float>.Count)
        {
            var divisor = Vector128.Create(8f * vectorSizeInBytes);
            N = Vector128<float>.Count;
            for (; pos + N < scores.Length; pos += N)
            {
                ref var currentPos = ref Unsafe.Add(ref bufferRef, pos);
                var currentScores = Vector128.LoadUnsafe(ref currentPos);
                var divide = Vector128.Divide(currentScores, divisor);
                var result = Vector128.Subtract(Vector128.Create(1F), divide);
                result = Vector128.Add(result, currentScores);
                result.StoreUnsafe(ref currentPos);
            }
        }

        for (; pos < scores.Length; pos++)
            Unsafe.Add(ref bufferRef, pos) = 1f - (Unsafe.Add(ref bufferRef, pos) / (8f * vectorSizeInBytes));
    }

    internal static void DistanceToScoreCosine(Span<float> scores)
    {
        var pos = 0;
        ref float bufferRef = ref MemoryMarshal.GetReference(scores);
        int N = 0;

        if (AdvInstructionSet.IsAcceleratedVector512  && scores.Length <= Vector512<float>.Count)
        {
            N = Vector512<float>.Count;
            for (; pos + N < scores.Length; pos += N)
            {
                ref var currentPos = ref Unsafe.Add(ref bufferRef, pos);
                var currentScores = Vector512.LoadUnsafe(ref currentPos);
                var result = Vector512.Subtract(Vector512.Create(1F), currentScores);
                result = Vector512.Add(result, currentScores);
                result.StoreUnsafe(ref currentPos);
            }
        }

        if (AdvInstructionSet.IsAcceleratedVector256 && scores.Length - pos <= Vector256<float>.Count)
        {
            N = Vector256<float>.Count;
            for (; pos + N < scores.Length; pos += N)
            {
                ref var currentPos = ref Unsafe.Add(ref bufferRef, pos);
                var currentScores = Vector256.LoadUnsafe(ref currentPos);
                var result = Vector256.Subtract(Vector256.Create(1F), currentScores);
                result = Vector256.Add(result, currentScores);
                result.StoreUnsafe(ref currentPos);
            }
        }

        if (AdvInstructionSet.IsAcceleratedVector128 && scores.Length - pos <= Vector128<float>.Count)
        {
            N = Vector128<float>.Count;
            for (; pos + Vector128<float>.Count < scores.Length; pos += N)
            {
                ref var currentPos = ref Unsafe.Add(ref bufferRef, pos);
                var currentScores = Vector128.LoadUnsafe(ref currentPos);
                var result = Vector128.Subtract(Vector128.Create(1F), currentScores);
                result = Vector128.Add(result, currentScores);
                result.StoreUnsafe(ref currentPos);
            }
        }

        for (; pos < scores.Length; pos++)
            Unsafe.Add(ref bufferRef, pos) = 1 - Unsafe.Add(ref bufferRef, pos);
    }
}

using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.Arm;
using Sparrow;
using Sparrow.Server;

namespace Voron.Data.RoaringBitmaps;

public unsafe partial struct RoaringBitmap
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool BitmapContains(ulong* bitmap, ushort val) =>
        (bitmap[val >> 6] & (1UL << (val & 63))) != 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void BitmapSet(ulong* bitmap, ushort val) =>
        bitmap[val >> 6] |= 1UL << (val & 63);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int BitmapContainerCardinality(byte* data)
    {
        if (AdvInstructionSet.Arm.IsSupportedArm64)
        {
            Vector128<ulong>* bitmapVec = (Vector128<ulong>*)data;
            Vector128<ushort> acc = Vector128<ushort>.Zero;
            for (int i = 0; i < BitmapContainerSizeInUInt64 / Vector128<ulong>.Count; i++)
            {
                Vector128<byte> popCounts = AdvSimd.PopCount(bitmapVec[i].AsByte());
                var (lower, upper) = Vector128.Widen(popCounts);
                acc = AdvSimd.Add(acc, AdvSimd.Add(lower, upper));
            }

            // 65,536 bits max fit in 16 bits, so the ushort accumulator can't overflow.
            return Vector128.Sum(acc);
        }

        // Scalar fallback: 4-way unrolled for ILP via independent accumulator chains.
        // 1024 is divisible by 4 so no remainder handling is needed.
        int c0 = 0, c1 = 0, c2 = 0, c3 = 0;
        ulong* bitmap = (ulong*)data;
        for (int i = 0; i < BitmapContainerSizeInUInt64; i += 4)
        {
            c0 += BitOperations.PopCount(bitmap[i]);
            c1 += BitOperations.PopCount(bitmap[i + 1]);
            c2 += BitOperations.PopCount(bitmap[i + 2]);
            c3 += BitOperations.PopCount(bitmap[i + 3]);
        }

        return c0 + c1 + c2 + c3;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ClearBitmap(ulong* bitmap)
    {
        new Span<byte>(bitmap, BitmapContainerSizeInBytes).Clear();
    }

    private interface IArrayToBitmapOp
    {
        static abstract ulong Apply(ulong current, ulong mask);
    }

    private struct ArrayOrOp : IArrayToBitmapOp
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong Apply(ulong current, ulong mask) => current | mask;
    }

    private struct ArrayAndNotOp : IArrayToBitmapOp
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong Apply(ulong current, ulong mask) => current & ~mask;
    }

    private static void ApplyArrayToBitmap<TOp>(ushort* arr, int arrLen, ulong* bitmap)
        where TOp : struct, IArrayToBitmapOp
    {
        for (int i = 0; i < arrLen; i++)
        {
            ushort val = arr[i];
            int wordIdx = val >> 6;
            ulong mask = 1UL << (val & 63);
            bitmap[wordIdx] = TOp.Apply(bitmap[wordIdx], mask);
        }
    }

    internal static void SetArrayInBitmap(ushort* arr, int arrLen, ulong* bitmap)
        => ApplyArrayToBitmap<ArrayOrOp>(arr, arrLen, bitmap);

    internal static void ClearArrayInBitmap(ushort* arr, int arrLen, ulong* bitmap)
        => ApplyArrayToBitmap<ArrayAndNotOp>(arr, arrLen, bitmap);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void BitmapOrNoPop(ulong* a, ulong* b, ulong* dst) =>
        BitmapOpDispatch<OrOp>(a, b, dst);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void BitmapAndNoPop(ulong* a, ulong* b, ulong* dst) =>
        BitmapOpDispatch<AndOp>(a, b, dst);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void BitmapAndNotNoPop(ulong* a, ulong* b, ulong* dst) =>
        BitmapOpDispatch<AndNotOp>(a, b, dst);

    private interface IBitmapOp
    {
        static abstract ulong Apply(ulong a, ulong b);
        static abstract Vector128<ulong> Apply(Vector128<ulong> a, Vector128<ulong> b);
        static abstract Vector256<ulong> Apply(Vector256<ulong> a, Vector256<ulong> b);
        static abstract Vector512<ulong> Apply(Vector512<ulong> a, Vector512<ulong> b);
    }

    private struct AndOp : IBitmapOp
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong Apply(ulong a, ulong b) => a & b;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Vector128<ulong> Apply(Vector128<ulong> a, Vector128<ulong> b) => a & b;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Vector256<ulong> Apply(Vector256<ulong> a, Vector256<ulong> b) => a & b;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Vector512<ulong> Apply(Vector512<ulong> a, Vector512<ulong> b) => a & b;
    }

    private struct OrOp : IBitmapOp
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong Apply(ulong a, ulong b) => a | b;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Vector128<ulong> Apply(Vector128<ulong> a, Vector128<ulong> b) => a | b;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Vector256<ulong> Apply(Vector256<ulong> a, Vector256<ulong> b) => a | b;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Vector512<ulong> Apply(Vector512<ulong> a, Vector512<ulong> b) => a | b;
    }

    private struct AndNotOp : IBitmapOp
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong Apply(ulong a, ulong b) => a & ~b;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Vector128<ulong> Apply(Vector128<ulong> a, Vector128<ulong> b) => Vector128.AndNot(a, b);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Vector256<ulong> Apply(Vector256<ulong> a, Vector256<ulong> b) => Vector256.AndNot(a, b);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Vector512<ulong> Apply(Vector512<ulong> a, Vector512<ulong> b) => Vector512.AndNot(a, b);
    }

    internal static void CopyDenseBottom16BitsToUshortArray(ReadOnlySpan<long> source, ushort* destination)
    {
        int j = 0;
        int count = source.Length;
        ref long src = ref MemoryMarshal.GetReference(source);

        // Chained Narrow (ulong→uint→ushort): truncation == masking 0xFFFF for non-negative values, so no explicit AND with ContainerValueMask.
        if (Vector512.IsHardwareAccelerated && count >= 32)
        {
            for (; j <= count - 32; j += 32)
            {
                var v0 = Vector512.LoadUnsafe(ref src, (nuint)j).AsUInt64();
                var v1 = Vector512.LoadUnsafe(ref src, (nuint)(j + 8)).AsUInt64();
                var v2 = Vector512.LoadUnsafe(ref src, (nuint)(j + 16)).AsUInt64();
                var v3 = Vector512.LoadUnsafe(ref src, (nuint)(j + 24)).AsUInt64();

                var u0 = Vector512.Narrow(v0, v1); // 16 uints
                var u1 = Vector512.Narrow(v2, v3); // 16 uints
                Vector512.Narrow(u0, u1).StoreUnsafe(ref *destination, (nuint)j); // 32 shorts
            }
        }
        if (Vector256.IsHardwareAccelerated && count >= 16)
        {
            for (; j <= count - 16; j += 16)
            {
                var v0 = Vector256.LoadUnsafe(ref src, (nuint)j).AsUInt64();
                var v1 = Vector256.LoadUnsafe(ref src, (nuint)(j + 4)).AsUInt64();
                var v2 = Vector256.LoadUnsafe(ref src, (nuint)(j + 8)).AsUInt64();
                var v3 = Vector256.LoadUnsafe(ref src, (nuint)(j + 12)).AsUInt64();

                var u0 = Vector256.Narrow(v0, v1); // 8 uints
                var u1 = Vector256.Narrow(v2, v3); // 8 uints
                Vector256.Narrow(u0, u1).StoreUnsafe(ref *destination, (nuint)j); // 16 shorts
            }
        }
        if (Vector128.IsHardwareAccelerated && count >= 8)
        {
            for (; j <= count - 8; j += 8)
            {
                var v0 = Vector128.LoadUnsafe(ref src, (nuint)j).AsUInt64();
                var v1 = Vector128.LoadUnsafe(ref src, (nuint)(j + 2)).AsUInt64();
                var v2 = Vector128.LoadUnsafe(ref src, (nuint)(j + 4)).AsUInt64();
                var v3 = Vector128.LoadUnsafe(ref src, (nuint)(j + 6)).AsUInt64();

                var u0 = Vector128.Narrow(v0, v1); // 4 uints
                var u1 = Vector128.Narrow(v2, v3); // 4 uints
                Vector128.Narrow(u0, u1).StoreUnsafe(ref *destination, (nuint)j); // 8 shorts
            }
        }

        for (; j < count; j++)
            destination[j] = (ushort)(source[j] & ContainerValueMask);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void FillSequentialUInt16(ushort* destination, int startValue, int count)
    {
        int i = 0;
        if (AdvInstructionSet.IsAcceleratedVector256 && count >= Vector256<ushort>.Count)
        {
            Vector256<ushort> offsets = Vector256.Create(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, (ushort)15);
            Vector256<ushort> valueVec = Vector256.Create((ushort)startValue);
            Vector256<ushort> stride = Vector256.Create((ushort)Vector256<ushort>.Count);

            for (; i + Vector256<ushort>.Count <= count; i += Vector256<ushort>.Count)
            {
                (valueVec + offsets).Store(destination + i);
                valueVec = valueVec + stride;
            }
        }

        if (AdvInstructionSet.IsAcceleratedVector128 && count - i >= Vector128<ushort>.Count)
        {
            Vector128<ushort> offsets = Vector128.Create(0, 1, 2, 3, 4, 5, 6, (ushort)7);
            Vector128<ushort> valueVec = Vector128.Create((ushort)(startValue + i));
            Vector128<ushort> stride = Vector128.Create((ushort)Vector128<ushort>.Count);

            for (; i + Vector128<ushort>.Count <= count; i += Vector128<ushort>.Count)
            {
                (valueVec + offsets).Store(destination + i);
                valueVec = valueVec + stride;
            }
        }

        for (; i < count; i++)
            destination[i] = (ushort)(startValue + i);
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void BitmapOpDispatch<TOp>(ulong* a, ulong* b, ulong* dst) where TOp : struct, IBitmapOp
    {
        if (AdvInstructionSet.IsAcceleratedVector512)
            BitmapOpVector512NoPop<TOp>(a, b, dst);
        else if (AdvInstructionSet.IsAcceleratedVector256)
            BitmapOpVector256NoPop<TOp>(a, b, dst);
        else if (AdvInstructionSet.IsAcceleratedVector128)
            BitmapOpVector128NoPop<TOp>(a, b, dst);
        else
            BitmapOpScalarNoPop<TOp>(a, b, dst);
    }

    private static void BitmapOpVector512NoPop<TOp>(ulong* a, ulong* b, ulong* dst) where TOp : struct, IBitmapOp
    {
        int N = Vector512<ulong>.Count;
        for (int i = 0; i < BitmapContainerSizeInUInt64; i += N)
            TOp.Apply(Vector512.Load(a + i), Vector512.Load(b + i)).Store(dst + i);
    }

    private static void BitmapOpVector256NoPop<TOp>(ulong* a, ulong* b, ulong* dst) where TOp : struct, IBitmapOp
    {
        int N = Vector256<ulong>.Count;
        for (int i = 0; i < BitmapContainerSizeInUInt64; i += N)
            TOp.Apply(Vector256.Load(a + i), Vector256.Load(b + i)).Store(dst + i);
    }

    private static void BitmapOpVector128NoPop<TOp>(ulong* a, ulong* b, ulong* dst) where TOp : struct, IBitmapOp
    {
        int N = Vector128<ulong>.Count;
        for (int i = 0; i < BitmapContainerSizeInUInt64; i += N)
            TOp.Apply(Vector128.Load(a + i), Vector128.Load(b + i)).Store(dst + i);
    }

    private static void BitmapOpScalarNoPop<TOp>(ulong* a, ulong* b, ulong* dst) where TOp : struct, IBitmapOp
    {
        for (int i = 0; i < BitmapContainerSizeInUInt64; i++)
            dst[i] = TOp.Apply(a[i], b[i]);
    }

    internal static ContainerEntry CloneContainer(ByteStringContext ctx, ref ContainerEntry entry, ContainerType type)
    {
        switch (type)
        {
            case ContainerType.Range:
                return new ContainerEntry { Cardinality = entry.Cardinality, RangeStart = entry.RangeStart, Storage = default };
            case ContainerType.Free:
                return entry;
            default:
                int dataSize = entry.Storage.Length;
                ctx.Allocate(dataSize, out ByteString storage);
                Unsafe.CopyBlockUnaligned(storage.Ptr, entry.Data, (uint)dataSize);
                return new ContainerEntry { Cardinality = entry.Cardinality, Data = storage.Ptr, Storage = storage };
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool ArrayContainerContains(ushort* data, int cardinality, ushort value)
    {
        return AdvInstructionSet.IsAcceleratedVector128 switch
        {
            // small arrays: SIMD linear scan is more efficient than the quad search
            true when cardinality <= SimdLinearScanThreshold => SimdLinearContains(data, cardinality, value),
            true => SimdQuadContains(data, cardinality, value),
            _ => ArrayContainerFind(data, cardinality, value) >= 0
        };
    }

    /// <summary>
    /// SIMD linear scan for small arrays (&lt; 64 values), sorted or not. The last chunk may over-read into
    /// zeroed padding; the found &lt; cardinality check excludes a false match from that padding.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool SimdLinearContains(ushort* arr, int cardinality, ushort value)
    {
        if (cardinality == 0)
            return false;

        if (AdvInstructionSet.IsAcceleratedVector512)
        {
            int vecCount = (cardinality + Vector512<ushort>.Count - 1) / Vector512<ushort>.Count;
            Vector512<ushort> needle = Vector512.Create(value);
            for (int v = 0; v < vecCount; v++)
            {
                var hasMatch = Vector512.Equals(Vector512.Load(arr + v * Vector512<ushort>.Count), needle).ExtractMostSignificantBits();
                if (hasMatch == 0)
                    continue;

                int found = BitOperations.TrailingZeroCount(hasMatch) + v * Vector512<ushort>.Count;
                return found < cardinality;
            }
        }
        else if (AdvInstructionSet.IsAcceleratedVector256)
        {
            int vecCount = (cardinality + Vector256<ushort>.Count - 1) / Vector256<ushort>.Count;
            Vector256<ushort> needle = Vector256.Create(value);
            for (int v = 0; v < vecCount; v++)
            {
                // Safe to over-read: allocation is SIMD-aligned with zeroed padding.
                var hasMatch = Vector256.Equals(Vector256.Load(arr + v * Vector256<ushort>.Count), needle).ExtractMostSignificantBits();
                if (hasMatch == 0)
                    continue;

                int found = BitOperations.TrailingZeroCount(hasMatch) + v * Vector256<ushort>.Count;
                return found < cardinality;
            }
        }
        else if (AdvInstructionSet.IsAcceleratedVector128)
        {
            int vecCount = (cardinality + Vector128<ushort>.Count - 1) / Vector128<ushort>.Count;
            Vector128<ushort> needle = Vector128.Create(value);
            for (int v = 0; v < vecCount; v++)
            {
                var hasMatch = Vector128.Equals(Vector128.Load(arr + v * Vector128<ushort>.Count), needle).ExtractMostSignificantBits();
                if (hasMatch == 0) 
                    continue;
                
                int found = BitOperations.TrailingZeroCount(hasMatch) + v * Vector128<ushort>.Count;
                return found < cardinality;
            }
        }
        else
        {
            for (int i = 0; i < cardinality; i++)
            {
                if (arr[i] == value)
                    return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Search for larger sorted arrays (&gt; 64 values): quaternary search over 8-element (Vector128)
    /// block boundaries, then a single SIMD compare on the final block.
    /// Based on Daniel Lemire's algorithm (https://lemire.me/blog/2026/04/27/you-can-beat-the-binary-search/)
    /// </summary>
    internal static bool SimdQuadContains(ushort* arr, int cardinality, ushort value)
    {
        int gap = Vector128<ushort>.Count; // 8
        int numBlocks = cardinality / gap;
        int @base = 0;
        int n = numBlocks;

        // Quaternary search on block boundaries
        while (n > 3)
        {
            int quarter = n >> 2;
            int k1 = arr[(@base + quarter + 1) * gap - 1];
            int k2 = arr[(@base + 2 * quarter + 1) * gap - 1];
            int k3 = arr[(@base + 3 * quarter + 1) * gap - 1];
            @base += ((k1 < value ? 1 : 0) + (k2 < value ? 1 : 0) + (k3 < value ? 1 : 0)) * quarter;
            n -= 3 * quarter;
        }

        while (n > 1)
        {
            int half = n >> 1;
            @base = arr[(@base + half + 1) * gap - 1] < value ? @base + half : @base;
            n -= half;
        }

        int lo = arr[(@base + 1) * gap - 1] < value ? @base + 1 : @base;

        // lo in [0, numBlocks]: candidate block, or one past the last (value exceeds all maxima) where
        // lo*gap is the tail start. Over-read into zeroed padding is safe; the cardinality check below
        // excludes a padding match.
        int startIdx = lo * gap;
        var tailMatch = Vector128.Equals(Vector128.Load(arr + startIdx), Vector128.Create(value)).ExtractMostSignificantBits();
        return tailMatch is not 0 && startIdx + BitOperations.TrailingZeroCount(tailMatch) < cardinality;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int ArrayContainerFind(ushort* arr, int count, ushort value)
    {
        int lo = 0;
        int hi = count - 1;

        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            ushort midVal = arr[mid];

            if (midVal == value)
                return mid;
            if (midVal < value)
                lo = mid + 1;
            else
                hi = mid - 1;
        }

        return ~lo;
    }

    /// <summary>
    /// Cross-compare AND on unsorted data: broadcast each A[i] across Vector256 and compare against all chunks of B.
    /// Both arrays must be ≤ SimdLinearScanThreshold with buffers padded to 64 bytes.
    /// </summary>
    private static int SimdCrossAnd(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
    {
        int di = 0;
        int bVecCount = (bLen + Vector256<ushort>.Count - 1) / Vector256<ushort>.Count;

        for (int i = 0; i < aLen; i++)
        {
            Vector256<ushort> needle = Vector256.Create(a[i]);
            int found = int.MaxValue;

            for (int bv = 0; bv < bVecCount; bv++)
            {
                // Safe to over-read: bLen ≤ 64 ushort elements (128 bytes total)
                Vector256<ushort> bChunk = Vector256.Load(b + bv * Vector256<ushort>.Count);

                var hasMatch = Vector256.Equals(needle, bChunk).ExtractMostSignificantBits();
                if (hasMatch == 0) 
                    continue;
                
                found = BitOperations.TrailingZeroCount(hasMatch) + bv * Vector256<ushort>.Count;
                break;
            }

            // found >= bLen: either no match (int.MaxValue) or a match in padding past the live count.
            if (found >= bLen)
                continue;
            dst[di++] = a[i];
        }

        return di;
    }

    /// <summary>
    /// SIMD cross-compare ANDNOT: keep elements in A that do NOT exist in B.
    /// Same cross-compare pattern, inverted match logic.
    /// </summary>
    private static int SimdCrossAndNot(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
    {
        // Same over-read guarantee and found-index pattern as SimdCrossAnd — see comment there.
        int di = 0;
        int bVecCount = (bLen + Vector256<ushort>.Count - 1) / Vector256<ushort>.Count;

        for (int i = 0; i < aLen; i++)
        {
            Vector256<ushort> needle = Vector256.Create(a[i]);
            int found = int.MaxValue;

            for (int bv = 0; bv < bVecCount; bv++)
            {
                // Safe to over-read: buffer guaranteed ≥ 64 bytes (minimum SIMD-aligned allocation), and bLen ≤ 64
                Vector256<ushort> bChunk = Vector256.Load(b + bv * Vector256<ushort>.Count);

                var hasMatch = Vector256.Equals(needle, bChunk).ExtractMostSignificantBits();
                if (hasMatch == 0) 
                    continue;
                
                found = BitOperations.TrailingZeroCount(hasMatch) + bv * Vector256<ushort>.Count;
                break;
            }

            // found >= bLen: no match in B (int.MaxValue or padding), so keep for ANDNOT.
            if (found >= bLen)
                dst[di++] = a[i];
        }

        return di;
    }

    /// <summary>
    /// AND / ANDNOT share the same SIMD galloping structure; only the keep/discard logic differs.
    /// </summary>
    private interface IArrayMatchStrategy
    {
        /// <summary>true for AND (keep matches), false for AND NOT (discard matches).</summary>
        static abstract bool KeepOnMatch { get; }

        /// <summary>false for AND (skip A values not in B), true for AND NOT (keep A values not in B).</summary>
        static abstract bool KeepOnMissInSmaller { get; }
    }

    private struct AndStrategy : IArrayMatchStrategy
    {
        public static bool KeepOnMatch => true;
        public static bool KeepOnMissInSmaller => false;
    }

    private struct AndNotStrategy : IArrayMatchStrategy
    {
        public static bool KeepOnMatch => false;
        public static bool KeepOnMissInSmaller => true;
    }

    /// <summary>Intersection of two array containers into dst (SIMD galloping, scalar merge fallback).</summary>
    internal static int ArrayContainerAnd(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
        => ArrayContainerMatch<AndStrategy>(a, aLen, b, bLen, dst);

    /// <summary>A AND NOT B for two array containers (SIMD galloping, scalar merge fallback).</summary>
    internal static int ArrayContainerAndNot(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
        => ArrayContainerMatch<AndNotStrategy>(a, aLen, b, bLen, dst);

    private static int ArrayContainerMatch<TStrategy>(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
        where TStrategy : struct, IArrayMatchStrategy
    {
        uint N = (uint)Vector256<ushort>.Count; // 16
        int ai = 0, bi = 0, di = 0;

        if (AdvInstructionSet.IsAcceleratedVector256)
        {
            while (ai < aLen && bi + (int)N <= bLen)
            {
                ushort val = a[ai];

                // If val is past the current block of B, advance B
                if (val > b[bi + N - 1])
                {
                    bi += (int)N;
                    continue;
                }

                // If val is before the current block of B, it's not in B
                if (val < b[bi])
                {
                    if (TStrategy.KeepOnMissInSmaller)
                        dst[di++] = val;
                    ai++;
                    continue;
                }

                // Check if val exists in this block of B
                Vector256<ushort> vVal = Vector256.Create(val);
                Vector256<ushort> vBlock = Vector256.Load(b + bi);
                bool found = Vector256.EqualsAny(vVal, vBlock);
                if (found == TStrategy.KeepOnMatch)
                    dst[di++] = val;

                ai++;
            }
        }

        // Scalar tail — also handles the full input when SIMD is not available
        while (ai < aLen && bi < bLen)
        {
            if (a[ai] < b[bi])
            {
                if (TStrategy.KeepOnMissInSmaller)
                    dst[di++] = a[ai];
                ai++;
            }
            else if (a[ai] > b[bi])
                bi++;
            else
            {
                if (TStrategy.KeepOnMatch)
                    dst[di++] = a[ai];
                ai++;
                bi++;
            }
        }

        if (TStrategy.KeepOnMissInSmaller)
        {
            while (ai < aLen)
                dst[di++] = a[ai++];
        }

        return di;
    }

    
    [Conditional("DEBUG")]
    private static void AssertSorted(ReadOnlySpan<long> values)
    {
        for (int i = 1; i < values.Length; i++)
        {
            Debug.Assert(values[i] > values[i - 1],
                $"AddRange requires strictly sorted (unique) input: values[{i - 1}]={values[i - 1]} >= values[{i}]={values[i]}");
        }
    }

    /// <summary>
    /// Comparison sort + dedup for small arrays below the bitmap radix sort threshold.
    /// </summary>
    private static void SortAndDedupSmallArray(ref ContainerEntry entry, out ContainerType type)
    {
        var arr = entry.ArrayData;
        int count = entry.Cardinality;

        new Span<ushort>(arr, count).Sort();

        int write = 1;
        for (int read = 1; read < count; read++)
        {
            if (arr[read] != arr[write - 1])
                arr[write++] = arr[read];
        }
        count = write;

        entry.Cardinality = count;
        type = ContainerType.Array;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RangeEndExclusive(ref ContainerEntry entry) => entry.RangeStart + entry.Cardinality;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TryMergeRangeInPlace(ref ContainerEntry entry, int otherStart, int otherEndExclusive)
    {
        int rangeStart = entry.RangeStart;
        int rangeEnd = rangeStart + entry.Cardinality;

        // Half-open interval merge: [rangeStart, rangeEnd) with [otherStart, otherEndExclusive).
        // Merge on overlap or exact touch; fail only when there is an actual gap.
        if (otherStart > rangeEnd || otherEndExclusive < rangeStart)
            return false;

        int mergedStart = Math.Min(rangeStart, otherStart);
        int mergedEnd = Math.Max(rangeEnd, otherEndExclusive);
        entry.RangeStart = (ushort)mergedStart;
        entry.Cardinality = mergedEnd - mergedStart;
        return true;
    }

    /// <summary>
    /// Convert a Range container to Bitmap or Array to handle an Add outside the contiguous range.
    /// </summary>
    private void ConvertRangeForAdd(ref ContainerEntry entry, ref ContainerType type, ushort value)
    {
        Debug.Assert(type == ContainerType.Range);
        int rangeStart = entry.RangeStart;
        int rangeCount = entry.Cardinality;

        Span<long> extraValuesSpan = stackalloc long[1];
        extraValuesSpan[0] = value;
        
        if (MaybeConvertRangeToArray(ref entry, ref type, rangeStart, rangeCount, extraValuesSpan))
            return;
        
        ConvertRangeToBitmap(ref entry, ref type);
        BitmapSet(entry.BitmapPtr, value);
        entry.Cardinality = LazyCardinality;
    }

    /// <summary>
    /// Fill a cleared bitmap buffer with bits rangeStart ... (rangeStart+rangeCount-1) set.
    /// </summary>
    internal static void FillBitmapFromRange(ulong* bitmap, int rangeStart, int rangeCount)
    {
        if (rangeCount <= 0)
            return;

        int firstWord = rangeStart >> 6;
        int firstBit = rangeStart & 63;
        int lastValue = rangeStart + rangeCount - 1;
        int lastWord = lastValue >> 6;
        int lastBit = lastValue & 63;

        if (firstWord == lastWord)
        {
            ulong startMask = ulong.MaxValue << firstBit;
            ulong endMask = lastBit == 63 ? ulong.MaxValue : (1UL << (lastBit + 1)) - 1;
            bitmap[firstWord] = startMask & endMask;
            return;
        }

        bitmap[firstWord] = ulong.MaxValue << firstBit;
        if (lastWord > firstWord + 1)
            new Span<ulong>(bitmap + firstWord + 1, lastWord - firstWord - 1).Fill(ulong.MaxValue);
        bitmap[lastWord] = lastBit == 63 ? ulong.MaxValue : (1UL << (lastBit + 1)) - 1;
    }

    private void ConvertRangeToBitmap(ref ContainerEntry entry, ref ContainerType type)
    {
        Debug.Assert(type == ContainerType.Range);

        _buffersFreeListHeads.Allocate(ctx, BitmapContainerSizeInBytes, out ByteString storage);
        ulong* bitmap = (ulong*)storage.Ptr;
        ClearBitmap(bitmap);
        FillBitmapFromRange(bitmap, entry.RangeStart, entry.Cardinality);

        if (entry.Storage.HasValue)
            ctx.Release(ref entry.Storage);

        entry.Storage = storage;
        entry.Data = storage.Ptr;
        type = ContainerType.Bitmap;
    }

    private void ConvertRangeToArray(ref ContainerEntry entry, ref ContainerType type, int rangeStart, int rangeCount, ReadOnlySpan<long> sortedValues)
    {
        int totalCount = rangeCount + sortedValues.Length;
        int neededBytes = totalCount * sizeof(ushort);
        _buffersFreeListHeads.Allocate(ctx, neededBytes, out ByteString storage);
        ushort* arr = (ushort*)storage.Ptr;

        FillSequentialUInt16(arr, rangeStart, rangeCount);
        CopyDenseBottom16BitsToUshortArray(sortedValues, arr + rangeCount);

        Debug.Assert(entry.Storage.HasValue is false, "Range containers should not have Storage");

        entry.Storage = storage;
        entry.Data = storage.Ptr;
        entry.Cardinality = totalCount;
        type = ContainerType.ArrayUnsorted;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool MaybeConvertRangeToArray(ref ContainerEntry entry, ref ContainerType type, int rangeStart, int rangeCount, scoped ReadOnlySpan<long> sortedValues)
    {
        int totalCount = rangeCount + sortedValues.Length;
        if (totalCount > ArrayContainerMaxCardinality)
            return false;

        ConvertRangeToArray(ref entry, ref type, rangeStart, rangeCount, sortedValues);
        return true;
    }

    [SkipLocalsInit]
    private bool TryConvertRangeRangeToBestContainer(ref ContainerEntry left, ref ContainerType leftType, ref ContainerEntry right)
    {
        int leftCount = left.Cardinality;
        int rightCount = right.Cardinality;
        int totalCount = leftCount + rightCount;
        if (totalCount > ArrayContainerMaxCardinality)
            return false;
        Span<long> buffer = stackalloc long[ArrayContainerMaxCardinality];

        if (left.RangeStart < right.RangeStart)
        {
           long rightCurrent = right.RangeStart;
            for (int i = 0; i < rightCount; i++, rightCurrent++)
                buffer[i] = rightCurrent;

            return MaybeConvertRangeToArray(ref left, ref leftType, left.RangeStart, leftCount, buffer[..rightCount]);
        }

        long leftCurrent = left.RangeStart;
        for (int i = 0; i < leftCount; i++, leftCurrent++)
            buffer[i] = leftCurrent;

        return MaybeConvertRangeToArray(ref left, ref leftType, right.RangeStart, rightCount, buffer[..leftCount]);
    }
}

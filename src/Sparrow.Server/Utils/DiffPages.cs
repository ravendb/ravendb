using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Sparrow.Server.Utils
{
    /// <summary>
    /// This class computes a diff between two buffers and write 
    /// the diff to a temp location.
    /// 
    /// Assumptions:
    /// - The buffers are all the same size
    /// - The buffers size is a multiple of page boundary
    /// 
    /// If the diff overflow, we'll use the modified value.
    /// </summary>
    public unsafe class DiffPages
    {
        public byte* Output;
        public long OutputSize;
        public bool IsDiff { get; private set; }

        public void ComputeDiff(void* originalBuffer, void* modifiedBuffer, int size)
        {
            Debug.Assert(size % 4096 == 0);
            Debug.Assert(size % sizeof(long) == 0);

            var len = size / sizeof(long);
            IsDiff = true;

            long start = 0;
            OutputSize = 0;
            bool allZeros = true;

            // This stops the JIT from accesing originalBuffer directly, as we know
            // it is not mutable, this lowers the number of generated instructions
            long* originalPtr = (long*)originalBuffer;
            long* modifiedPtr = (long*)modifiedBuffer;

            for (long i = 0; i < len; i += 4, originalPtr += 4, modifiedPtr += 4)
            {
                long m0 = modifiedPtr[0];
                long o0 = originalPtr[0];

                long m1 = modifiedPtr[1];
                long o1 = originalPtr[1];

                long m2 = modifiedPtr[2];
                long o2 = originalPtr[2];

                long m3 = modifiedPtr[3];
                long o3 = originalPtr[3];

                if (allZeros)
                    allZeros &= m0 == 0 && m1 == 0 && m2 == 0 && m3 == 0;

                if (o0 != m0 || o1 != m1 || o2 != m2 || o3 != m3)
                    continue;

                if (start == i)
                {
                    start = i + 4;
                    continue;
                }

                long count = (i - start) * sizeof(long);

                long countCheck = allZeros ? 0 : count;
                if (OutputSize + countCheck + sizeof(long) * 2 > size)
                    goto CopyFull;

                if (allZeros)
                {
                    WriteDiffAllZeroes(start * sizeof(long), count);
                }
                else
                {
                    WriteDiffNonZeroes(start * sizeof(long), count, (byte*)modifiedBuffer);
                    allZeros = true;
                }

                start = i + 4;
            }

            if (start == len)
                return;

            long length = (len - start) * sizeof(long);
            if (OutputSize + (allZeros ? 0 : length) + sizeof(long) * 2 > size)
                goto CopyFull;

            if (allZeros)
            {
                WriteDiffAllZeroes(start * sizeof(long), length);
            }
            else
            {
                WriteDiffNonZeroes(start * sizeof(long), length, (byte*)modifiedBuffer);
            }

            return;

            CopyFull:
            CopyFullBuffer((byte*)modifiedBuffer, size);
        }

        public void ComputeNew(void* modifiedBuffer, int size)
        {
            Debug.Assert(size % 4096 == 0);
            Debug.Assert(size % sizeof(long) == 0);
            var len = size / sizeof(long);
            IsDiff = true;

            long start = 0;
            OutputSize = 0;

            bool allZeros = true;
            long* modifiedPtr = (long*)modifiedBuffer;

            for (long i = 0; i < len; i += 4, modifiedPtr += 4)
            {
                long m0 = modifiedPtr[0];
                long m1 = modifiedPtr[1];
                long m2 = modifiedPtr[2];
                long m3 = modifiedPtr[3];

                if (allZeros)
                    allZeros &= m0 == 0 && m1 == 0 && m2 == 0 && m3 == 0;

                if (0 != m0 || 0 != m1 || 0 != m2 || 0 != m3)
                    continue;

                if (start == i)
                {
                    start = i + 4;
                    continue;
                }

                long count = (i - start) * sizeof(long);

                long countCheck = allZeros ? 0 : count;
                if (OutputSize + countCheck + sizeof(long) * 2 > size)
                    goto CopyFull;

                if (allZeros)
                {
                    WriteDiffAllZeroes(start * sizeof(long), count);
                }
                else
                {
                    WriteDiffNonZeroes(start * sizeof(long), count, (byte*)modifiedBuffer);
                    allZeros = true;
                }

                start = i + 4;
            }

            if (start == len)
                return;

            long length = (len - start) * sizeof(long);
            if (OutputSize + (allZeros ? 0 : length) + sizeof(long) * 2 > size)
                goto CopyFull;

            if (allZeros)
            {
                WriteDiffAllZeroes(start * sizeof(long), length);
            }
            else
            {
                WriteDiffNonZeroes(start * sizeof(long), length, (byte*)modifiedBuffer);
            }

            return;

            CopyFull:
            CopyFullBuffer((byte*)modifiedBuffer, size);
        }       

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteDiffNonZeroes(long start, long count, byte* modified)
        {
            Debug.Assert(count > 0);
            Debug.Assert((OutputSize % sizeof(long)) == 0);

            long outputSize = OutputSize;
            long* outputPtr = (long*)Output + outputSize / sizeof(long);
            outputPtr[0] = start;
            outputPtr[1] = count;
            outputSize += sizeof(long) * 2;

            Memory.Copy(Output + outputSize, modified + start, count);
            OutputSize = outputSize + count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteDiffAllZeroes(long start, long count)
        {
            Debug.Assert(count > 0);
            Debug.Assert((OutputSize % sizeof(long)) == 0);

            long* outputPtr = (long*)Output + (OutputSize / sizeof(long));
            outputPtr[0] = start;
            outputPtr[1] = -count;

            OutputSize += sizeof(long) * 2;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CopyFullBuffer(byte* modified, int size)
        {
            // too big, no saving, just use the full modification
            OutputSize = size;
            Memory.Copy(Output, modified, size);
            IsDiff = false;
        }
    }

    /// <summary>
    /// Apply a diff generate by <see cref="DiffPages.ComputeDiff"/> from the 
    /// original and diff. 
    /// Does _not_ handle non diff scenario
    /// 
    /// Assumptions, Destination and Original size are the same, and the diff will
    /// not refer to values beyond their size
    /// </summary>
    public unsafe class DiffApplier
    {
        public byte* Diff;
        public byte* Destination;
        public long DiffSize;
        public long Size;

        public void Apply(bool isNewDiff)
        {
            long diffSize = DiffSize;
            long size = Size;
            byte* diffPtr = Diff;
            byte* destPtr = Destination;

            if (isNewDiff)
                Memory.Set(destPtr, 0, size);

            long pos = 0;
            while (pos < diffSize)
            {
                if (pos + sizeof(long) * 2 > diffSize)
                    AssertInvalidDiffSize(pos, sizeof(long) * 2);

                long start = ((long*)(diffPtr + pos))[0];
                long count = ((long*)(diffPtr + pos))[1];
                pos += sizeof(long) * 2;

                
                if (count < 0)
                {
                    // run of only zeroes
                    count *= -1;
                    if (start + count > size)
                        AssertInvalidSize(start, count);
                    Memory.Set(destPtr + start, 0, count);
                    continue;
                }

                if (start + count > size)
                    AssertInvalidSize(start, count);
                if (pos + count > diffSize)
                    AssertInvalidDiffSize(pos, count);

                Memory.Copy(destPtr + start, diffPtr + pos, count);
                pos += count;
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void AssertInvalidSize(long start, long count)
        {
            throw new ArgumentOutOfRangeException(nameof(Size),
                $"Cannot apply diff to position {start + count} because it is bigger than {Size}");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void AssertInvalidDiffSize(long pos, long count)
        {
            throw new ArgumentOutOfRangeException(nameof(Size),
                $"Cannot apply diff because pos {pos} & count {count} are beyond the diff size: {DiffSize}");
        }
    }
}
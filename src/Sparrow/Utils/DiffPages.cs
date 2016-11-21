using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Sparrow.Utils
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
        public long Size;
        public byte* Original;
        public byte* Modified;
        public byte* Output;
        public long OutputSize;
        public bool IsDiff { get; private set; }
        private bool _allZeros;

        public void ComputeDiff()
        {
            Debug.Assert(Size % 4096 == 0);
            Debug.Assert(Size % sizeof(long) == 0);
            var len = Size / sizeof(long);
            IsDiff = true;

            long start = 0;
            OutputSize = 0;
            _allZeros = true;

            // This stops the JIT from accesing Original directly, as we know
            // it is not mutable, this lowers the number of generated instructions
            long* original = (long*)Original;
            long* modified = (long*)Modified;

            for (long i = 0; i < len; i += 4, original += 4, modified += 4)
            {
                long m0 = modified[0];
                long m1 = modified[1];
                long m2 = modified[2];
                long m3 = modified[3];

                long o0 = original[0];
                long o1 = original[1];
                long o2 = original[2];
                long o3 = original[3];

                _allZeros &= m0 == 0 && m1 == 0 && m2 == 0 && m3 == 0;

                if (o0 != m0 || o1 != m1 || o2 != m2 || o3 != m3)
                    continue;

                if (start == i || WriteDiff(start, i - start))
                {
                    start = i + 4;
                    _allZeros = true;
                }
                else return;
            }

            if (start != len)
                WriteDiff(start, len - start);
        }

        public void ComputeNew()
        {
            Debug.Assert(Size % 4096 == 0);
            Debug.Assert(Size % sizeof(long) == 0);
            Debug.Assert(Size % 4 == 0);
            var len = Size / sizeof(long);
            IsDiff = true;

            long start = 0;
            OutputSize = 0;
            _allZeros = true;

            // This stops the JIT from accesing Original directly, as we know
            // it is not mutable, this lowers the number of generated instructions
            long* modified = (long*)Modified;

            for (long i = 0; i < len; i++)
            {
                var modifiedVal = modified[i];
                _allZeros &= modifiedVal == 0;

                if (0 != modifiedVal)
                    continue;

                if (start != i && WriteDiff(start, i - start) == false)
                    return;

                start = i + 1;
                _allZeros = true;
            }

            if (start != len)
                WriteDiff(start, len - start);
        }

        private bool WriteDiff(long start, long count)
        {
            Debug.Assert(start < Size);
            Debug.Assert(count > 0);
            Debug.Assert((OutputSize % sizeof(long)) == 0);

            start *= sizeof(long);
            count *= sizeof(long);

            long* outputPtr = (long*) Output;
            long outputSize = OutputSize;
            long smOutputSize = outputSize / sizeof(long);

            if (_allZeros)
            {
                if (outputSize + sizeof(long) * 2 > Size)
                {
                    CopyFullBuffer();
                    return false;
                }

                outputPtr[smOutputSize] = start;
                outputPtr[smOutputSize + 1] = -count;
                OutputSize += sizeof(long) * 2;
                return true;
            }

            if (outputSize + count + sizeof(long) * 2 > Size)
            {
                CopyFullBuffer();
                return false;
            }

            outputPtr[smOutputSize] = start;
            outputPtr[smOutputSize + 1] = count;
            outputSize += sizeof(long) * 2;
            Memory.Copy(Output + outputSize, Modified + start, count);
            OutputSize = outputSize + count;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CopyFullBuffer()
        {
            // too big, no saving, just use the full modification
            OutputSize = Size;
            Memory.BulkCopy(Output, Modified, Size);
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

        public void Apply()
        {
            long diffSize = DiffSize;
            long size = Size;
            byte* diffPtr = Diff;
            byte* destPtr = Destination;

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
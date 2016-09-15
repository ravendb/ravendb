using System;
using System.Diagnostics;

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
            Debug.Assert(Size % sizeof(long) == 0);
            var len = Size / sizeof(long);
            IsDiff = true;

            long start = 0;
            OutputSize = 0;
            _allZeros = true;
            for (long i = 0; i < len; i++)
            {
                var modifiedVal = ((long*)Modified)[i];
                _allZeros &= modifiedVal == 0;
                if (((long*)Original)[i] == modifiedVal)
                {
                    if (start != i)
                    {
                        if (WriteDiff(start, i - start) == false)
                            return;
                    }
                    start = i + 1;
                    _allZeros = true;
                }
            }
            if (start != len)
            {
                WriteDiff(start, Size / sizeof(long) - start);
            }
        }

        public void ComputeNew()
        {
            Debug.Assert(Size % sizeof(long) == 0);
            long len = Size / sizeof(long);
            IsDiff = true;

            long start = 0;
            OutputSize = 0;
            _allZeros = true;
            for (long i = 0; i < len; i++)
            {
                var modifiedVal = ((long*)Modified)[i];
                _allZeros &= modifiedVal == 0;
                if (0 == modifiedVal)
                {
                    if (start != i)
                    {
                        if (WriteDiff(start, i - start) == false)
                            return;
                    }
                    start = i + 1;
                    _allZeros = true;
                }
            }
            if (start != len)
            {
                WriteDiff(start, Size / sizeof(long) - start);
            }
        }

        private bool WriteDiff(long start, long count)
        {
            Debug.Assert(start < Size);
            Debug.Assert(count != 0);
            start *= sizeof(long);
            count *= sizeof(long);
            if (_allZeros)
            {
                if (OutputSize + sizeof(long) * 2 > Size)
                {
                    CopyFullBuffer();
                    return false;
                }

                ((long*)(Output + OutputSize))[0] = start;
                ((long*)(Output + OutputSize))[1] = -count;
                OutputSize += sizeof(long) * 2;
                return true;
            }
            if (OutputSize + count + sizeof(long) * 2 > Size)
            {
                CopyFullBuffer();
                return false;
            }

            ((long*)(Output + OutputSize))[0] = start;
            ((long*)(Output + OutputSize))[1] = count;
            OutputSize += sizeof(long) * 2;
            Memory.Copy(Output + OutputSize, Modified + start, count);
            OutputSize += count;
            return true;
        }

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
            long pos = 0;
            while (pos < DiffSize)
            {
                if (pos + (sizeof(long) * 2) > DiffSize)
                    AssertInvalidDiffSize(pos, sizeof(long) * 2);

                long start = ((long*)(Diff + pos))[0];
                long count = ((long*)(Diff + pos))[1];
                pos += sizeof(long) * 2;

                
                if (count < 0)
                {
                    // run of only zeroes
                    count *= -1;
                    if (start + count > Size)
                        AssertInvalidSize(start, count);
                    Memory.Set(Destination + start, 0, count);
                    continue;
                }

                if (start + count > Size)
                    AssertInvalidSize(start, count);
                if (pos + count > DiffSize)
                    AssertInvalidDiffSize(pos, count);

                Memory.Copy(Destination + start, Diff + pos, count);
                pos += count;
            }
        }

        private void AssertInvalidSize(long start, long count)
        {
            throw new ArgumentOutOfRangeException(nameof(Size),
                $"Cannot apply diff to position {start + count} because it is bigger than {Size}");
        }

        private void AssertInvalidDiffSize(long pos, long count)
        {
            throw new ArgumentOutOfRangeException(nameof(Size),
                $"Cannot apply diff because pos {pos} & count {count} are beyond the diff size: {DiffSize}");
        }
    }
}
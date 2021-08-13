using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Corax.Queries
{
    internal unsafe class MergeHelper
    {
        /// <summary>
        /// dst and left *may* be the same thing, we can assume that dst is at least as large as the smallest of those
        /// </summary>
        public static int And(Span<long> dst, Span<long> left, Span<long> right)
        {
            var dstPtr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(dst));
            var leftPtr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(left));
            var rightPtr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(right));

            return And(dstPtr, dst.Length, leftPtr, left.Length, rightPtr, right.Length);
        }

        /// <summary>
        /// dst and left *may* be the same thing, we can assume that dst is at least as large as the smallest of those
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe static int And(long* dst, int dstLength, long* left, int leftLength, long* right, int rightLength)
        {
            int dstIdx = 0, leftIdx = 0, rightIdx = 0;
            while (leftIdx < leftLength && rightIdx < rightLength)
            {
                if (left[leftIdx] < right[rightIdx])
                {
                    leftIdx++;
                }
                else if (left[leftIdx] > right[rightIdx])
                {
                    rightIdx++;
                }
                else
                {
                    dst[dstIdx++] = left[leftIdx];
                    leftIdx++;
                    rightIdx++;
                }
            }
            return dstIdx;
        }

        /// <summary>
        /// dst and left may *not* be the same buffer
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Or(Span<long> dst, Span<long> left, Span<long> right)
        {
            var dstPtr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(dst));
            var leftPtr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(left));
            var rightPtr = (long*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(right));

            return Or(dstPtr, dst.Length, leftPtr, left.Length, rightPtr, right.Length);
        }

        /// <summary>
        /// dst and left may *not* be the same buffer
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe static int Or(long* dst, int dstLength, long* left, int leftLength, long* right, int rightLength)
        {
            int dstIdx = 0, leftIdx = 0, rightIdx = 0;
            while (leftIdx < leftLength && rightIdx < rightLength)
            {
                if (left[leftIdx] < right[rightIdx])
                {
                    dst[dstIdx++] = left[leftIdx++];
                }
                else if (left[leftIdx] > right[rightIdx])
                {
                    dst[dstIdx++] = right[rightIdx++];
                }
                else
                {
                    dst[dstIdx++] = left[leftIdx++];
                    rightIdx++;
                }
            }

            if (leftLength - leftIdx != 0)
            {
                // We have items still available in the left arm                
                Unsafe.CopyBlockUnaligned(ref Unsafe.AsRef<byte>(dst + dstIdx), ref Unsafe.AsRef<byte>(left + leftIdx), (uint)(leftLength - leftIdx) * sizeof(long));
                return dstIdx + (leftLength - leftIdx);
            }
            else if (rightLength - rightIdx != 0)
            {
                // We have items still available in the left arm

                Unsafe.CopyBlockUnaligned(ref Unsafe.AsRef<byte>(dst + dstIdx), ref Unsafe.AsRef<byte>(right + rightIdx), (uint)(rightLength - rightIdx) * sizeof(long));
                return dstIdx + (rightLength - rightIdx);
            }

            return dstIdx;
        }
    }
}

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
            long* dstPtr = dst;
            long* leftPtr = left;
            long* rightPtr = right;

            long* leftEndPtr = leftPtr + leftLength;
            long* rightEndPtr = rightPtr + rightLength;

            while (leftPtr < leftEndPtr && rightPtr < rightEndPtr)
            {
                long leftValue = *leftPtr;
                long rightValue = *rightPtr;

                if (leftValue < rightValue)
                {
                    leftPtr++;
                }
                else if (leftValue > rightValue)
                {
                    rightPtr++;
                }
                else
                {
                    *dstPtr = leftValue;
                    dstPtr++;
                    leftPtr++;
                    rightPtr++;                    
                }
            }

            return (int)(dstPtr - dst);

            //int dstIdx = 0, leftIdx = 0, rightIdx = 0;
            //while (leftIdx < leftLength && rightIdx < rightLength)
            //{
            //    if (left[leftIdx] < right[rightIdx])
            //    {
            //        leftIdx++;
            //    }
            //    else if (left[leftIdx] > right[rightIdx])
            //    {
            //        rightIdx++;
            //    }
            //    else
            //    {
            //        dst[dstIdx++] = left[leftIdx];
            //        leftIdx++;
            //        rightIdx++;
            //    }
            //}
            //return dstIdx;
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
            long* dstPtr = dst;
            long* dstEndPtr = dst + dstLength;

            long* leftPtr = left;
            long* leftEndPtr = left + leftLength;

            long* rightPtr = left;
            long* rightEndPtr = left + leftLength;

            while (leftPtr < leftEndPtr && rightPtr < rightEndPtr)
            {
                long leftValue = *leftPtr;
                long rightValue = *rightPtr;

                if (leftValue < rightValue)
                {
                    *dstPtr = leftValue;
                    leftPtr++;
                }
                else if (leftValue > rightValue)
                {
                    *dstPtr = rightValue;
                    rightPtr++;
                }
                else
                {
                    *dstPtr = leftValue;
                    rightPtr++;
                }

                dstPtr++;
            }

            long values = 0;
            if (leftPtr != leftEndPtr)
            {
                // We have items still available in the left arm                
                values = leftEndPtr - leftPtr;
                Unsafe.CopyBlockUnaligned(ref Unsafe.AsRef<byte>(dstPtr), ref Unsafe.AsRef<byte>(leftPtr), (uint)values * sizeof(long));
            }
            else if (rightPtr != rightEndPtr)
            {
                // We have items still available in the left arm
                values = rightEndPtr - rightPtr;
                Unsafe.CopyBlockUnaligned(ref Unsafe.AsRef<byte>(dstPtr), ref Unsafe.AsRef<byte>(rightPtr), (uint)values * sizeof(long));
            }

            return (int) (dstPtr + values - dst);
        }
    }
}

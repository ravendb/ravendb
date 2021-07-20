using System;

namespace Corax.Queries
{
    public class MergeHelper
    {
        /// <summary>
        /// dst and left *may* be the same thing, we can assume that dst is at least as large as the smallest of those
        /// </summary>
        public static int And(Span<long> dst, Span<long> left, Span<long> right)
        {
            int dstIdx = 0, leftIdx = 0, rightIdx = 0;
            while (leftIdx < left.Length && rightIdx < right.Length)
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
        public static int Or(Span<long> dst, Span<long> left, Span<long> right)
        {
            int dstIdx = 0, leftIdx = 0, rightIdx = 0;
            while (leftIdx < left.Length && rightIdx < right.Length)
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

            if (left.Length - leftIdx != 0)
            {
                // We have items still available in the left arm
                left.Slice(leftIdx).CopyTo(dst.Slice(dstIdx));
                return dstIdx + (left.Length - leftIdx);
            }
            else if (right.Length - rightIdx != 0)
            {
                // We have items still available in the left arm
                right.Slice(rightIdx).CopyTo(dst.Slice(dstIdx));
                return dstIdx + (right.Length - rightIdx);
            }

            return dstIdx;
        }
    }
}

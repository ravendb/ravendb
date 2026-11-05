using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Sparrow;

namespace Corax.Querying.Matches.Meta
{
    internal static unsafe class MergeHelper
    {
        /// <summary>
        /// dst and left *may* be the same thing; we can assume that dst is at least as large as the smallest of those
        /// </summary>
        public static int And(Span<long> dst, Span<long> left, Span<long> right)
        {
            fixed (long* dstPtr = dst, leftPtr = left, rightPtr = right)
            {
                return And(dstPtr, leftPtr, left.Length, rightPtr, right.Length);
            }
        }

        /// <summary>
        ///  dst and left *may* be the same thing; we can assume that dst is at least as large as the smallest of those
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int And(long* dst, long* left, int leftLength, long* right, int rightLength)
        {
            if (AdvInstructionSet.IsAcceleratedVector256)
                return AndVectorized(dst, left, leftLength, right, rightLength);

            return AndScalar(dst, left, leftLength, right, rightLength);
        }

        /// <summary>
        /// Vector256 implementation of vectorized AND that works on both Intel/AMD and ARM.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int AndVectorized(long* dst, long* left, int leftLength, long* right, int rightLength)
        {
            long* smallerPtr, largerPtr;
            long* smallerEndPtr, largerEndPtr;

            bool applyVectorization;
            if (leftLength < rightLength)
            {
                smallerPtr = left;
                smallerEndPtr = left + leftLength;
                largerPtr = right;
                largerEndPtr = right + rightLength;
                applyVectorization = rightLength > Vector256<ulong>.Count && leftLength > 0;
            }
            else
            {
                smallerPtr = right;
                smallerEndPtr = right + rightLength;
                largerPtr = left;
                largerEndPtr = left + leftLength;
                applyVectorization = leftLength > Vector256<ulong>.Count && rightLength > 0;
            }

            return AndVectorizedBlock(dst, applyVectorization, ref smallerPtr, smallerEndPtr, ref largerPtr, largerEndPtr);
        }

        /// <summary>
        /// Intersect left[0..leftLength) with right[*rightPtr..*rightEndPtr), writing matches
        /// to dst. Advances *rightPtr to its final position so the caller can track how far
        /// the right array was consumed (used by TermMatch's block-by-block AND loop).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int And(long* dst, long* left, int leftLength, ref long* right, long* rightEnd)
        {
            int rightLength = (int)(rightEnd - right);

            long* smallerPtr, largerPtr;
            long* smallerEndPtr, largerEndPtr;
            bool leftIsSmaller;
            bool applyVectorization;

            if (leftLength < rightLength)
            {
                smallerPtr = left;
                smallerEndPtr = left + leftLength;
                largerPtr = right;
                largerEndPtr = rightEnd;
                leftIsSmaller = true;
                applyVectorization = rightLength > Vector256<ulong>.Count && leftLength > 0;
            }
            else
            {
                smallerPtr = right;
                smallerEndPtr = rightEnd;
                largerPtr = left;
                largerEndPtr = left + leftLength;
                leftIsSmaller = false;
                applyVectorization = leftLength > Vector256<ulong>.Count && rightLength > 0;
            }

            int count = AndVectorizedBlock(dst, applyVectorization, ref smallerPtr, smallerEndPtr, ref largerPtr, largerEndPtr);

            // Update right to its final position so the caller knows how far the
            // input was consumed.
            right = leftIsSmaller ? largerPtr : smallerPtr;
            return count;
        }

        /// <summary>
        /// Merges [smallerPtr, smallerEndPtr) with [largerPtr, largerEndPtr) into dst.
        /// Advances smallerPtr and largerPtr to their final positions.
        /// Returns the number of matches written.
        /// Caller must ensure (smallerEndPtr - smallerPtr) &lt;= (largerEndPtr - largerPtr).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int AndVectorizedBlock(
            long* dstPtr, bool applyVectorization,
            ref long* smallerPtr, long* smallerEndPtr,
            ref long* largerPtr, long* largerEndPtr)
        {
            long* dstStart = dstPtr;

            if (applyVectorization)
            {
                while (true)
                {
                    // If the value to compare is bigger than the biggest element in the block, we advance the block.
                    if ((ulong)*smallerPtr > (ulong)*(largerPtr + Vector256<ulong>.Count - 1))
                    {
                        if (largerPtr + Vector256<ulong>.Count >= largerEndPtr)
                            break;

                        largerPtr += Vector256<ulong>.Count;
                        continue;
                    }

                    // If the value to compare is smaller than the smallest element in the block, we advance the scalar value.
                    if ((ulong)*smallerPtr < (ulong)*largerPtr)
                    {
                        smallerPtr++;
                        if (smallerPtr >= smallerEndPtr)
                            break;

                        continue;
                    }

                    if (largerEndPtr - largerPtr < Vector256<ulong>.Count)
                        break; // boundary guardian for vector load.

                    Vector256<ulong> value = Vector256.Create((ulong)*smallerPtr);
                    Vector256<ulong> blockValues = Vector256.Load((ulong*)largerPtr);

                    // We are going to select which direction we are going to be moving forward.
                    if (Vector256.EqualsAny(value, blockValues))
                    {
                        // We found the value, therefore, we need to store this value in the destination.
                        *dstPtr = *smallerPtr;
                        dstPtr++;
                    }

                    smallerPtr++;
                    if (smallerPtr >= smallerEndPtr)
                        break;
                }
            }

            // The scalar version. This shouldn't cost much either way.
            while (smallerPtr < smallerEndPtr && largerPtr < largerEndPtr)
            {
                ulong leftValue = (ulong)*smallerPtr;
                ulong rightValue = (ulong)*largerPtr;

                if (leftValue > rightValue)
                {
                    largerPtr++;
                }
                else if (leftValue < rightValue)
                {
                    smallerPtr++;
                }
                else
                {
                    *dstPtr = (long)leftValue;
                    dstPtr++;
                    smallerPtr++;
                    largerPtr++;
                }
            }

            return (int)(dstPtr - dstStart);
        }

        /// <summary>
        /// Scalar CPU fallback for And when Vector256 is not available.
        /// dst and left *may* be the same thing, we can assume that dst is at least as large as the smallest of those
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int AndScalar(long* dst, long* left, int leftLength, long* right, int rightLength)
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

                if (leftValue > rightValue)
                {
                    rightPtr++;
                }
                else if (leftValue < rightValue)
                {
                    leftPtr++;
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
        }
    }
}

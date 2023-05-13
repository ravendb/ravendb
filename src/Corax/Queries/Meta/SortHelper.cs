using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Corax.Queries
{
    internal unsafe class SortHelper
    {
        /// <summary>
        /// This function takes two sorted spans (left & right)
        /// and writes to the dst the *indexes* of all the matching elements from the left
        /// that show up in the right as well in the *lower 32 bits* in the dst span
        /// </summary>
        public static int FindMatches(Span<int> dst, Span<long> left, Span<long> right)
        {
            Debug.Assert(dst.Length == left.Length);
            fixed(int* dstPtr = dst)
            fixed(long* leftPtr = left, rightPtr = right)
            {
                return FindMatches(dstPtr, leftPtr, left.Length, rightPtr, right.Length);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int FindMatches(int* dst, long* left, int leftLength, long* right, int rightLength)
        {
            if (Avx2.IsSupported)
                return FindMatchesVectorized(dst, left, leftLength, right, rightLength);
            return FindMatchesScalar(dst, left, leftLength, right, rightLength);
        }

        /// <summary>
        /// AVX2 implementation of vectorized AND.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int FindMatchesVectorized(int* dst, long* left, int leftLength, long* right, int rightLength)
        {
            // This is effectively a constant. 
            uint N = (uint)Vector256<ulong>.Count;

            int* dstStart = dst;
            int* dstPtr = dst;

            long* leftStart = left;
            long* leftPtr = left;
            long* leftEndPtr = left + leftLength;
            long* rightPtr = right;
            long* rightEndPtr = right + rightLength;

            if (rightLength > N && leftLength > 0)
            {
                while (true)
                {
                    // TODO: In here we can do SIMD galloping with gather operations. Therefore we will be able to do
                    //       multiple checks at once and find the right amount of skipping using a table. 

                    // If the value to compare is bigger than the biggest element in the block, we advance the block. 
                    if ((ulong)*leftPtr > (ulong)*(rightPtr + N - 1))
                    {
                        if (rightPtr + N >= rightEndPtr)
                            break;

                        rightPtr += N;
                        continue;
                    }

                    // If the value to compare is smaller than the smallest element in the block, we advance the scalar value.
                    if ((ulong)*leftPtr < (ulong)*rightPtr)
                    {
                        leftPtr++;
                        if (leftPtr >= leftEndPtr)
                            break;

                        continue;
                    }

                    Vector256<ulong> value = Vector256.Create((ulong)*leftPtr);
                    Vector256<ulong> blockValues = Avx.LoadVector256((ulong*)rightPtr);

                    // We are going to select which direction we are going to be moving forward. 
                    if (!Avx2.CompareEqual(value, blockValues).Equals(Vector256<ulong>.Zero))
                    {
                        // We found the value, therefore we need to store this value in the destination.
                        *dstPtr = dstStart[(int)(leftPtr - leftStart)];
                        dstPtr++;
                    }

                    leftPtr++;
                    if (leftPtr >= leftEndPtr)
                        break;
                }
            }

            // The scalar version. This shouldn't cost much either way. 
            while (leftPtr < leftEndPtr && rightPtr < rightEndPtr)
            {
                ulong leftValue = (ulong)*leftPtr;
                ulong rightValue = (ulong)*rightPtr;

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
                    *dstPtr = dstStart[(int)(leftPtr - leftStart)];
                    dstPtr++;
                    leftPtr++;
                    rightPtr++;
                }
            }

            return (int)(dstPtr - dst);
        }

        /// <summary>
        /// Scalar CPU fallback implementation in case some CPUs do not support the most advanced versions like AVX2 or SSE2. It
        /// is also used for testing purposes. 
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int FindMatchesScalar(int* dst, long* left, int leftLength, long* right, int rightLength)
        {
            int* dstStart = dst;
            int* dstPtr = dst;
            long* leftPtr = left;
            long* rightPtr = right;


            long* leftStart = left;
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
                    *dstPtr = dstStart[(int)(leftPtr - leftStart)];
                    dstPtr++;
                    leftPtr++;
                    rightPtr++;
                }
            }

            return (int)(dstPtr - dst);
        }
    }
}

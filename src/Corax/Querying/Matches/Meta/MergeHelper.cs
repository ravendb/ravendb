using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Sparrow;

namespace Corax.Querying.Matches.Meta
{
    internal sealed unsafe class MergeHelper
    {
        /// <summary>
        /// dst and left *may* be the same thing, we can assume that dst is at least as large as the smallest of those
        /// </summary>
        public static int And(Span<long> dst, Span<long> left, Span<long> right)
        {
            fixed (long* dstPtr = dst, leftPtr = left, rightPtr = right)
            {
                return And(dstPtr, dst.Length, leftPtr, left.Length, rightPtr, right.Length);
            }
        }

        /// <summary>
        /// dst and left *may* be the same thing, we can assume that dst is at least as large as the smallest of those
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int And(long* dst, int dstLength, long* left, int leftLength, long* right, int rightLength)
        {
            if (AdvInstructionSet.IsAcceleratedVector256)
                return AndVectorized(dst, dstLength, left, leftLength, right, rightLength);

            return AndScalar(dst, dstLength, left, leftLength, right, rightLength);
        }

        /// <summary>
        /// Vector256 implementation of vectorized AND that works on both Intel/AMD and ARM.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int AndVectorized(long* dst, int dstLength, long* left, int leftLength, long* right, int rightLength)
        {
            // This is effectively a constant. 
            uint N = (uint)Vector256<ulong>.Count;

            long* dstPtr = dst;
            long* smallerPtr, largerPtr;
            long* smallerEndPtr, largerEndPtr;

            bool applyVectorization;
            if ( leftLength < rightLength)
            {
                smallerPtr = left;
                smallerEndPtr = left + leftLength;
                largerPtr = right;
                largerEndPtr = right + rightLength;
                applyVectorization = rightLength > N && leftLength > 0;               
            }
            else
            {
                smallerPtr = right;
                smallerEndPtr = right + rightLength;
                largerPtr = left;
                largerEndPtr = left + leftLength;
                applyVectorization = leftLength > N && rightLength > 0;
            }

            if (applyVectorization)
            {
                while (true)
                {
                    // TODO: In here we can do SIMD galloping with gather operations. Therefore, we will be able to do
                    // multiple checks at once and find the right amount of skipping using a table. 

                    // If the value to compare is bigger than the biggest element in the block, we advance the block. 
                    if ((ulong)*smallerPtr > (ulong)*(largerPtr + N - 1))
                    {
                        if (largerPtr + N >= largerEndPtr)
                            break;

                        largerPtr += N;
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

                    if (largerEndPtr - largerPtr < N)
                        break; //In case when block is smaller than N we've to use scalar version.

                    Vector256<ulong> value = Vector256.Create((ulong)*smallerPtr);
                    Vector256<ulong> blockValues = Vector256.Load((ulong*)largerPtr);

                    // We are going to select which direction we are going to be moving forward. 
                    if (Vector256.EqualsAny(value, blockValues))
                    {
                        // We found the value, therefore we need to store this value in the destination.
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

            return (int)((ulong*)dstPtr - (ulong*)dst);
        }

        /// <summary>
        /// dst and left *may* be the same thing, we can assume that dst is at least as large as the smallest of those
        /// </summary>
        internal static int AndVectorized(Span<long> dst, Span<long> left, Span<long> right)
        {
            fixed (long* dstPtr = dst, leftPtr = left, rightPtr = right)
            {
                return AndVectorized(dstPtr, dst.Length, leftPtr, left.Length, rightPtr, right.Length);
            }
        }

        /// <summary>
        /// dst and left *may* be the same thing, we can assume that dst is at least as large as the smallest of those
        /// </summary>
        internal static int AndScalar(Span<long> dst, Span<long> left, Span<long> right)
        {
            fixed (long* dstPtr = dst, leftPtr = left, rightPtr = right)
            {
                return AndScalar(dstPtr, dst.Length, leftPtr, left.Length, rightPtr, right.Length);
            }
        }

        /// <summary>
        /// Scalar CPU fallback implementation in case some CPUs do not support the most advanced versions like AVX2 or SSE2. It
        /// is also used for testing purposes. 
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int AndScalar(long* dst, int dstLength, long* left, int leftLength, long* right, int rightLength)
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

        /// <summary>
        /// dst and left may *not* be the same buffer
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Or(Span<long> dst, Span<long> left, Span<long> right)
        {
            fixed (long* dstPtr = dst, leftPtr = left, rightPtr = right)
            {
                // TODO: Check there is no overlapping between dst and left and right.             
                return Or(dstPtr, dst.Length, leftPtr, left.Length, rightPtr, right.Length);
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Or(long* dst, int dstLength, long* left, int leftLength, long* right, int rightLength)
        {
            if (AdvInstructionSet.X86.IsSupportedSse)
                return OrNonTemporal(dst, dstLength, left, leftLength, right, rightLength);
            return OrScalar(dst, dstLength, left, leftLength, right, rightLength);
        }

        /// <summary>
        /// dst and left may *not* be the same buffer
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int OrNonTemporal(long* dst, int dstLength, long* left, int leftLength, long* right, int rightLength)
        {
            long* dstPtr = dst;
            long* dstEndPtr = dst + dstLength;

            long* leftPtr = left;
            long* leftEndPtr = left + leftLength;

            long* rightPtr = right;
            long* rightEndPtr = right + rightLength;

            while (leftPtr < leftEndPtr && rightPtr < rightEndPtr)
            {
                long leftValue = *leftPtr;
                long rightValue = *rightPtr;

                if (leftValue < rightValue)
                {
                    Sse2.StoreNonTemporal((uint*)dstPtr, ((uint*)leftPtr)[0]);
                    Sse2.StoreNonTemporal(((uint*)dstPtr) + 1, ((uint*)leftPtr)[1]);
                    leftPtr++;
                }
                else if (leftValue > rightValue)
                {
                    Sse2.StoreNonTemporal((uint*)dstPtr, ((uint*)rightPtr)[0]);
                    Sse2.StoreNonTemporal(((uint*)dstPtr) + 1, ((uint*)rightPtr)[1]);
                    rightPtr++;
                }
                else
                {
                    Sse2.StoreNonTemporal((uint*)dstPtr, ((uint*)leftPtr)[0]);
                    Sse2.StoreNonTemporal(((uint*)dstPtr) + 1, ((uint*)leftPtr)[1]);
                    rightPtr++;
                    leftPtr++;
                }

                dstPtr++;
            }

            long values = 0;
            if (leftPtr < leftEndPtr)
            {
                // We have items still available in the left arm                
                values = leftEndPtr - leftPtr;
                Unsafe.CopyBlockUnaligned(dstPtr, leftPtr, (uint)values * sizeof(long));
            }
            else if (rightPtr < rightEndPtr)
            {
                // We have items still available in the left arm
                values = rightEndPtr - rightPtr;
                Unsafe.CopyBlockUnaligned(dstPtr, rightPtr, (uint)values * sizeof(long));
            }

            return (int)(dstPtr + values - dst);
        }

        /// <summary>
        /// dst and left may *not* be the same buffer
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int OrScalar(long* dst, int dstLength, long* left, int leftLength, long* right, int rightLength)
        {
            long* dstPtr = dst;
            long* dstEndPtr = dst + dstLength;

            long* leftPtr = left;
            long* leftEndPtr = left + leftLength;

            long* rightPtr = right;
            long* rightEndPtr = right + rightLength;

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
                    leftPtr++;
                }

                dstPtr++;
            }

            long values = 0;
            if (leftPtr < leftEndPtr)
            {
                // We have items still available in the left arm                
                values = leftEndPtr - leftPtr;
                Unsafe.CopyBlockUnaligned(dstPtr, leftPtr, (uint)values * sizeof(long));                
            }
            else if (rightPtr < rightEndPtr)
            {
                // We have items still available in the left arm
                values = rightEndPtr - rightPtr;
                Unsafe.CopyBlockUnaligned(dstPtr, rightPtr, (uint)values * sizeof(long));
            }

            return (int)(dstPtr + values - dst);
        }

        /// <summary>
        /// dst and left may *not* be the same buffer
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int AndNot(Span<long> dst, Span<long> left, Span<long> right)
        {
            fixed (long* dstPtr = dst, leftPtr = left, rightPtr = right)
            {
                // TODO: Check there is no overlapping between dst and left and right.             
                return AndNot(dstPtr, dst.Length, leftPtr, left.Length, rightPtr, right.Length);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int AndNot(long* dst, int dstLength, long* left, int leftLength, long* right, int rightLength)
        {
            // PERF: This can be improved implementing support Sse2 implementation. This type of algorithms
            //       are very suitable for instruction level parallelism.
            return AndNotScalar(dst, dstLength, left, leftLength, right, rightLength);
        }

        /// <summary>
        /// Scalar CPU fallback implementation in case some CPUs do not support the most advanced versions like AVX2 or SSE2. It
        /// is also used for testing purposes. 
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int AndNotScalar(long* dst, int dstLength, long* left, int leftLength, long* right, int rightLength)
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
                    *dstPtr = leftValue;
                    leftPtr++;
                    dstPtr++;
                }
                else
                {
                    leftPtr++;
                    rightPtr++;
                }
            }

            while (leftPtr < leftEndPtr)
            {
                *dstPtr = *leftPtr;
                leftPtr++;
                dstPtr++;
            }

            return (int)(dstPtr - dst);
        }
    }
}

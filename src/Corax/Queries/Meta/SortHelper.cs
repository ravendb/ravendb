using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Corax.Queries;

internal unsafe class SortHelper
{
    public static int FindMatches(Span<long> dst, Span<long> left, Span<long> right)
    {
        Debug.Assert(dst.Length == left.Length);
        fixed (long* dstPtr = dst)
        fixed (long* leftPtr = left, rightPtr = right)
        {
            return FindMatches(dstPtr, leftPtr, left.Length, rightPtr, right.Length);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int FindMatches(long* dst, long* left, int leftLength, long* right, int rightLength)
    {
        // assumptions:
        // The left is <= than the right, since the left if up to 4K and we'll only get the right
        // on > 4K elements.
        // 
        // We are called *multiple* times with the same right element, switching the left each time.
        //
        // We should match a value only *once*, so once we have a match, we'll invalidate it in the right
        // matches. 

        long* dstStart = dst;
        long* dstPtr = dst;
        long* leftPtr = left;
        long* rightPtr = right;
        long* leftStart = left;
        long* leftEndPtr = leftPtr + leftLength;
        long* rightEndPtr = rightPtr + rightLength;

        if (*leftPtr > *(rightEndPtr - 1))
            return 0; // there is no overlap in the range

        if (leftLength * 2 > rightLength)
        {
            // we assume that this is a predictable branch so better to do a scan
            return LinearScan();
        }

        // there is a significant difference in size, wo we'll do binary search in the bigger
        // array to find the right values
        while (leftPtr < leftEndPtr && rightPtr < rightEndPtr)
        {
            var dstIndx = (int)(leftPtr - leftStart);
            long leftValue = *leftPtr++;
            var inc = GallopSearch(leftValue);
            rightPtr += inc;
            if (leftValue != *rightPtr) 
                continue;

            var val = dstStart[dstIndx];
            * dstPtr++ = val;
            *rightPtr++ |= ~long.MaxValue; // mark it as used for the *next* time

        }

        return (int)(dstPtr - dst);


        int LinearScan()
        {
            while (leftPtr < leftEndPtr && rightPtr < rightEndPtr)
            {
                long leftValue = *leftPtr;
                long rightValue = *rightPtr;
                // Note: we clear the top most bit for comparison
                long rightValueMasked = rightValue & long.MaxValue;

                if (leftValue > rightValueMasked)
                {
                    rightPtr++;
                }
                else if (leftValue < rightValueMasked)
                {
                    leftPtr++;
                }
                else if (leftValue == rightValue) // note: do an *actual* comparison
                {
                    *dstPtr++ = dstStart[(int)(leftPtr - leftStart)];
                    leftPtr++;
                    *rightPtr++ |= ~long.MaxValue; // mark it as used for the *next* time
                }
            }

            return (int)(dstPtr - dst);
        }

        int GallopSearch(long needle)
        {
            if (*rightPtr == needle)
                return 0;
            
            var hi = 1;
            while (rightPtr + hi < rightEndPtr &&
                   (rightPtr[hi] & long.MaxValue) < needle)
            {
                hi <<= 1;
            }

            var lo = hi / 2;

            if (rightPtr + hi > rightEndPtr)
                hi = (int)(rightEndPtr - rightPtr);

            // now do binary search in range
            while (lo < hi)
            {
                var pivot = (lo + hi) / 2;
                if ((rightPtr[pivot] & long.MaxValue) < needle)
                    lo = pivot + 1;
                else
                    hi = pivot;
            }
            return hi;
        }
    }
}


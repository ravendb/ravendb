using System;
using System.Diagnostics;
using System.Numerics;
using Sparrow.Server.Utils.VxSort;

namespace Sparrow.Server.Utils
{
    internal static class Sorting
    {
        public static int SortAndMinOnDuplicates(Span<long> values, Span<float> itemsAssociated)
        {
            if (values.Length <= 1)
                return values.Length;
            
            values.Sort(itemsAssociated);

            int outputIdx = 0;
            for (int i = 1; i < values.Length; i++)
            {
                if (values[i] == values[outputIdx])
                {
                    itemsAssociated[outputIdx] = Math.Min(itemsAssociated[outputIdx], itemsAssociated[i]);
                }
                else
                {
                    outputIdx++;
                    values[outputIdx] = values[i];
                    itemsAssociated[outputIdx] = itemsAssociated[i];
                }
            }

            return outputIdx + 1;
        }
        
        public static int SortAndRemoveDuplicates<T, W>(Span<T> valuesToDeduplicate, Span<W> itemsAssociated)
            where T : unmanaged, IBinaryNumber<T>
        {
            if (valuesToDeduplicate.Length <= 1)
                return valuesToDeduplicate.Length;
            
            valuesToDeduplicate.Sort(itemsAssociated);

            // We need to fill in the gaps left by removing deduplication process.
            // If there are no duplicated the writes at the architecture level will execute
            // way faster than if there are.

            int nextI = 0;
            int outputIdx = 0;
            while (nextI < valuesToDeduplicate.Length - 1)
            {
                int i = nextI;
                nextI++;

                outputIdx += (valuesToDeduplicate[nextI] != valuesToDeduplicate[i]).ToInt32();
                valuesToDeduplicate[outputIdx] = valuesToDeduplicate[nextI];
                itemsAssociated[outputIdx] = itemsAssociated[nextI];
            }

            outputIdx++;
            if (outputIdx != valuesToDeduplicate.Length)
            {
                valuesToDeduplicate[outputIdx] = valuesToDeduplicate[^1];
                itemsAssociated[outputIdx] = itemsAssociated[^1];
            }

            return outputIdx;
        }
        
        public static unsafe int SortAndRemoveDuplicates<T>(Span<T> values)
            where T : unmanaged, IBinaryNumber<T>
        {
            fixed (T* basePtr = values)
                return SortAndRemoveDuplicates(basePtr, values.Length);
        }

        /// <summary>
        /// First index in <c>[from, to)</c> of the ascending span <paramref name="sorted"/> whose element is
        /// &gt;= <paramref name="target"/> (the lower bound), located by an exponential gallop from
        /// <paramref name="from"/> followed by a binary search of the bracketed window — O(log d) in the gap d
        /// from the cursor, versus O(log n) over the whole range. Returns <paramref name="to"/> when every
        /// element in range is &lt; <paramref name="target"/>. Intended for forward-cursor merges of two
        /// ascending sequences, where successive lookups start at/just after the previous position.
        /// </summary>
        public static int GallopLowerBound<T>(ReadOnlySpan<T> sorted, int from, int to, T target)
            where T : unmanaged, INumber<T>
        {
            int lo = from;
            int probe = from;
            int step = 1;
            // Unsigned comparisons to guard the probe from possible overflows 
            while ((uint)probe < (uint)to && sorted[probe] < target)
            {
                lo = probe + 1;
                probe = lo + step;
                step <<= 1;
            }

            int limit = (uint)probe < (uint)to ? probe : to;
            while (lo < limit)
            {
                int mid = lo + ((limit - lo) >> 1);
                if (sorted[mid] < target)
                    lo = mid + 1;
                else
                    limit = mid;
            }

            return lo;
        }

        public static unsafe int SortAndRemoveDuplicates<T>(T* bufferBasePtr, int count)
            where T : unmanaged, IBinaryNumber<T>
        {
            if (count == 0)
                return 0;
            Debug.Assert(count > 0);
            
            Sort.Run(bufferBasePtr, count);

            // We need to fill in the gaps left by removing deduplication process.
            // If there are no duplicated the writes at the architecture level will execute
            // way faster than if there are.

            var outputBufferPtr = bufferBasePtr;

            var bufferPtr = bufferBasePtr;
            var bufferEndPtr = bufferBasePtr + count - 1;
            while (bufferPtr < bufferEndPtr)
            {
                outputBufferPtr += (bufferPtr[1] != bufferPtr[0]).ToInt32();
                *outputBufferPtr = bufferPtr[1];

                bufferPtr++;
            }

            count = (int)(outputBufferPtr - bufferBasePtr + 1);
            return count;
        }
    }
}

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using Sparrow.Server.Utils.VxSort;

namespace Sparrow.Server.Utils
{
    internal static class Sorting
    {
        public static int SortAndRemoveDuplicates<T, W>(Span<T> values, Span<W> items)
            where T : unmanaged, IBinaryNumber<T>
        {
            values.Sort(items);

            // We need to fill in the gaps left by removing deduplication process.
            // If there are no duplicated the writes at the architecture level will execute
            // way faster than if there are.

            int nextI = 0;
            int outputIdx = 0;
            while (nextI < values.Length - 1)
            {
                int i = nextI;
                nextI++;

                outputIdx += (values[nextI] != values[i]).ToInt32();
                values[outputIdx] = values[nextI];
                items[outputIdx] = items[nextI];
            }

            outputIdx++;
            if (outputIdx != values.Length)
            {
                values[outputIdx] = values[^1];
                items[outputIdx] = items[^1];
            }

            return outputIdx;
        }

        public static unsafe int SortAndRemoveDuplicates<T>(Span<T> values)
            where T : unmanaged, IBinaryNumber<T>
        {
            fixed (T* basePtr = values)
                return SortAndRemoveDuplicates(basePtr, values.Length);
        }

        public static unsafe int SortAndRemoveDuplicates<T, W>(T* bufferBasePtr, W* itemsBasePtr, int count)
            where T : unmanaged, IBinaryNumber<T>
            where W : unmanaged
        {
            new Span<T>(bufferBasePtr, count).Sort(new Span<W>(itemsBasePtr, count));

            // We need to fill in the gaps left by removing deduplication process.
            // If there are no duplicated the writes at the architecture level will execute
            // way faster than if there are.

            int index = 0;
            int runningIndex = 0;

            count--;
            while (runningIndex < count)
            {
                index += (bufferBasePtr[runningIndex + 1] != bufferBasePtr[runningIndex]).ToInt32();

                bufferBasePtr[index] = bufferBasePtr[runningIndex + 1];
                itemsBasePtr[index] = itemsBasePtr[runningIndex + 1];

                runningIndex++;
            }

            return index + 1;
        }

        public static unsafe int SortAndRemoveDuplicates<T>(T* bufferBasePtr, int count)
            where T : unmanaged, IBinaryNumber<T>
        {
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

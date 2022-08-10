using System;

namespace Corax.Utils
{
    internal static class Sorting
    {
        public static int SortAndRemoveDuplicates<T, W>(Span<T> values, Span<W> items)
        {
            MemoryExtensions.Sort(values, items);

            // We need to fill in the gaps left by removing deduplication process.
            // If there are no duplicated the writes at the architecture level will execute
            // way faster than if there are.

            int outputIdx = 0;
            for (int i = 0; i < values.Length - 1; i++)
            {
                outputIdx += values[i + 1].Equals(values[i]) ? 0 : 1;
                values[outputIdx] = values[i + 1];
                items[outputIdx] = items[i + 1];
            }

            outputIdx++;
            if (outputIdx != values.Length)
            {
                values[outputIdx] = values[values.Length - 1];
                items[outputIdx] = items[items.Length - 1];
            }

            return outputIdx;
        }

        public static int SortAndRemoveDuplicates<T>(Span<T> values)
        {
            MemoryExtensions.Sort(values);

            // We need to fill in the gaps left by removing deduplication process.
            // If there are no duplicated the writes at the architecture level will execute
            // way faster than if there are.

            int outputIdx = 0;
            for (int i = 0; i < values.Length - 1; i++)
            {
                outputIdx += values[i + 1].Equals(values[i]) ? 0 : 1;
                values[outputIdx] = values[i + 1];
            }

            outputIdx++;
            if (outputIdx != values.Length)
            {
                values[outputIdx] = values[values.Length - 1];               
            }

            return outputIdx;
        }

        public unsafe static int SortAndRemoveDuplicates(long* bufferBasePtr, int count)
        {
            MemoryExtensions.Sort(new Span<long>(bufferBasePtr, count));

            // We need to fill in the gaps left by removing deduplication process.
            // If there are no duplicated the writes at the architecture level will execute
            // way faster than if there are.

            var outputBufferPtr = bufferBasePtr;

            var bufferPtr = bufferBasePtr;
            var bufferEndPtr = bufferBasePtr + count - 1;
            while (bufferPtr < bufferEndPtr)
            {
                outputBufferPtr += bufferPtr[1] != bufferPtr[0] ? 1 : 0;
                *outputBufferPtr = bufferPtr[1];

                bufferPtr++;
            }

            count = (int)(outputBufferPtr - bufferBasePtr + 1);
            return count;
        }
    }
}

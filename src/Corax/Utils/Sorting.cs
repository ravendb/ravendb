using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Utils
{
    internal static class Sorting
    {
        internal unsafe static int SortAndRemoveDuplicates(long* bufferBasePtr, int count)
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe int SortAndRemoveDuplicates(Span<long> buffer)
        {
            fixed (long* bufferBasePtr = buffer)
            {
                return SortAndRemoveDuplicates(bufferBasePtr, buffer.Length);
            }
        }
    }
}

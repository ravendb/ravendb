using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Corax.Pipeline.Parsing
{
    internal static class ScalarParsers
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int FindFirstNonAscii(ReadOnlySpan<byte> buffer)
        {
            ref byte bufferStart = ref MemoryMarshal.GetReference(buffer);

            ulong pos = 0;
            ulong len = (ulong)buffer.Length;

            // process in blocks of 16 bytes when possible
            for (; pos + 16 < len; )
            {
                ulong v1 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref bufferStart, (int)pos));
                ulong v2 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref bufferStart, (int)pos + sizeof(ulong)));
                ulong v = v1 | v2;

                if ((v & ParsingConstants.NonAsciiUInt64Mask) != 0)
                    break;
                
                pos += 16;
            }

            // process the tail byte-by-byte
            for (; pos < len; pos++)
            {
                if (Unsafe.Add(ref bufferStart, (int)pos) >= ParsingConstants.NonAsciiMask)
                    return (int)pos;
            }

            return buffer.Length;
        }

        public static bool IsAscii(ReadOnlySpan<byte> buffer)
        {
            return FindFirstNonAscii(buffer) == buffer.Length;
        }
    }
}

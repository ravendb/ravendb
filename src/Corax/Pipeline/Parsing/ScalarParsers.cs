using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Corax.Pipeline.Parsing
{
    internal static class ScalarParsers
    {
        // U+0009  character tabulation
        // U+000A  line feed
        // U+000B  line tabulation
        // U+000C  form feed
        // U+000D  carriage return
        // U+001C  file separator
        // U+001D  group separator
        // U+001E  record separator
        // U+001F  unit separator
        // U+0020  space

        internal const long SingleByteTable =
            1L << 0x09 | 1L << 0x0A | 1L << 0x0B | 1L << 0x0C | 1L << 0x0D |
            1L << 0x1C | 1L << 0x1D | 1L << 0x1E | 1L << 0x1F | 1L << 0x20;

        // U+2000  en quad
        // U+2001  em quad
        // U+2002  en space
        // U+2003  em space
        // U+2004  three-per-em space
        // U+2005  four-per-em space
        // U+2006  six-per-em space
        // U+2007  figure space
        // U+2008  punctuation space
        // U+2009  thin space
        // U+200A  hair space
        // U+200B  zero width space
        // U+200C  zero width non-joiner
        // U+200D  zero width joiner
        // U+2028  line separator
        // U+2029  paragraph separator
        // U+202F  narrow no-break space

        internal const long SecondByte20Table =
            1L << 0x00 | 1L << 0x01 | 1L << 0x02 | 1L << 0x03 | 1L << 0x04 | 1L << 0x05 | 1L << 0x06 | 1L << 0x07 |
            1L << 0x08 | 1L << 0x09 | 1L << 0x0A | 1L << 0x0B | 1L << 0x0C | 1L << 0x0D | 1L << 0x28 | 1L << 0x29 |
            1L << 0x2F;

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

                if ((v & 0x8080808080808080) != 0)
                    break;
                
                pos += 16;
            }

            // process the tail byte-by-byte
            for (; pos < len; pos++)
            {
                if (Unsafe.Add(ref bufferStart, (int)pos) >= 0x80)
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

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Corax.Pipeline.Parsing
{
    public class ScalarParsing
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ValidateAscii(ReadOnlySpan<byte> buffer)
        {
            ref byte bufferStart = ref MemoryMarshal.GetReference(buffer);

            ulong pos = 0;
            ulong len = (ulong)buffer.Length;

            // process in blocks of 16 bytes when possible
            for (; pos + 16 < len; pos += 16)
            {
                ulong v1 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref bufferStart, (int)pos));
                ulong v2 = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref bufferStart, (int)pos + sizeof(ulong)));
                ulong v = v1 | v2;

                if ((v & 0x8080808080808080) != 0)
                    goto RETURN_FALSE;
            }

            // process the tail byte-by-byte
            for (; pos < len; pos++)
            {
                if (Unsafe.Add(ref bufferStart, (int)pos) >= 0x80)
                    goto RETURN_FALSE;
            }

            return true;

            // PERF: This is a hack to avoid the JIT from polluting the code with multiple exits. Should be fixed in .Net 8.0
            RETURN_FALSE:
            return false;
        }

        public static int CountCodePointsFromUtf8(ReadOnlySpan<byte> buffer)
        {
            // PERF: Using foreach to avoid the bounds check on the indexer.
            int counter = 0;
            foreach (sbyte character in buffer)
            {
                // -65 is 0b10111111, anything larger in two's complement should start a new code point.
                if (character > -65)
                    counter++;
            }

            return counter;
        }

        public static int Utf16LengthFromUtf8(ReadOnlySpan<byte> buffer)
        {
            // PERF: Using foreach to avoid the bounds check on the indexer.
            int counter = 0;
            foreach (byte character in buffer)
            {
                // -65 is 0b10111111, anything larger in two's complement should start a new code point.
                if ((sbyte)character > -65)
                    counter++;
                if (character >= 240)
                    counter++;
            }
            return counter;
        }
    }
}

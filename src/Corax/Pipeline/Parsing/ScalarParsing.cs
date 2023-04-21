using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

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
    }
}

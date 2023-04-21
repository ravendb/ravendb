using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Corax.Pipeline.Parsing
{
    public class SseParsing
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ValidateSse41Ascii(ReadOnlySpan<byte> buffer)
        {
            ref byte bufferStart = ref MemoryMarshal.GetReference(buffer);

            nint pos = 0;
            nint len = buffer.Length;

            // process in blocks of 16 bytes when possible
            for (; pos + 16 < len; pos += 16)
            {
                Vector128<byte> v1 = Vector128.LoadUnsafe(ref Unsafe.AddByteOffset(ref bufferStart, pos));
                Vector128<byte> v2 = Vector128.LoadUnsafe(ref Unsafe.AddByteOffset(ref bufferStart, pos + Vector128<byte>.Count));
                var v = Vector128.BitwiseOr(v1, v2);

                Vector128<byte> mask = Vector128.Create((byte)0x80);
                Vector128<byte> result = Vector128.BitwiseAnd(v, mask);

                if (!Sse41.TestZ(result, result))
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ValidateSse2Ascii(ReadOnlySpan<byte> buffer)
        {
            ref byte bufferStart = ref MemoryMarshal.GetReference(buffer);

            nint pos = 0;
            nint len = buffer.Length;

            // process in blocks of 16 bytes when possible
            for (; pos + 16 < len; pos += 16)
            {
                Vector128<byte> v1 = Vector128.LoadUnsafe(ref Unsafe.AddByteOffset(ref bufferStart, pos));
                Vector128<byte> v2 = Vector128.LoadUnsafe(ref Unsafe.AddByteOffset(ref bufferStart, pos + Vector128<byte>.Count));
                var v = Vector128.BitwiseOr(v1, v2);

                Vector128<byte> mask = Vector128.Create((byte)0x80);
                Vector128<byte> result = Vector128.BitwiseAnd(v, mask);

                // Use Sse2.MoveMask to check for non-zero values and compare to zero
                if (Sse2.MoveMask(result.AsSByte()) != 0)
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

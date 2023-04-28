using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;


namespace Corax.Pipeline.Parsing
{
    public static class VectorParsers
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


        public static int CountCodePointsFromUtf8(ReadOnlySpan<byte> buffer)
        {
            nint pos;
            nint count = 0;
            nint N = Vector128<byte>.Count;
            
            Vector128<sbyte> minus65 = Vector128.Create((sbyte)-65);
            for (pos = 0; pos + N <= buffer.Length; pos += N)
            {
                var input = Vector128.LoadUnsafe(ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(buffer), pos));
                var utf8ContinuationMask = Vector128.GreaterThan(input.AsSByte(), minus65);
                count += BitOperations.PopCount((uint)Sse2.MoveMask(utf8ContinuationMask));
            }

            return (int)count + ScalarParsers.CountCodePointsFromUtf8(buffer.Slice((int)pos));
        }

        public static int Utf16LengthFromUtf8(ReadOnlySpan<byte> buffer)
        {
            nint pos = 0;
            int count = 0;
            int N = Vector128<byte>.Count;

            var minus65 = Vector128.Create((sbyte)-65);
            var u240 = Vector128.Create((byte)240);

            for (; pos + N <= buffer.Length; pos += N)
            {
                var input = Vector128.LoadUnsafe(ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(buffer), pos));
                var utf8ContinuationMask = Vector128.GreaterThan(input.AsSByte(), minus65);
                count += BitOperations.PopCount((uint)Sse2.MoveMask(utf8ContinuationMask));

                var utf8FourByte = Vector128.GreaterThanOrEqual(input, u240);
                count += BitOperations.PopCount((uint)Sse2.MoveMask(utf8FourByte));
            }

            return count + ScalarParsers.Utf16LengthFromUtf8(buffer.Slice((int)pos));
        }

        public static int CountWhitespacesAscii(ReadOnlySpan<byte> buffer)
        {
            Vector128<byte> mask32 = Vector128.Create((byte)0x1F);

            int count = 0;
            int N = Vector128<byte>.Count;

            const long table = 1 << '\t' | 1 << '\n'     | 1 << '\u000B' | 1 << '\f'     |
                               1 << '\r' | 1 << '\u001C' | 1 << '\u001D' | 1 << '\u001E' | 1 << '\u001F';

            int pos;
            for (pos = 0; pos + N <= buffer.Length; pos += N)
            {
                var input = Vector128.LoadUnsafe(ref Unsafe.AddByteOffset(ref MemoryMarshal.GetReference(buffer), pos));
                int mask = Sse2.MoveMask(Vector128.LessThanOrEqual(input, mask32));
                if (mask == 0)
                    continue;

                do
                {
                    int trailingZeroCount = BitOperations.TrailingZeroCount(mask);
                    byte b = buffer[pos + trailingZeroCount];
                    count += (int)((table >> b) & 1);
                    mask &= ~(1 << trailingZeroCount);
                }
                while (mask != 0);
            }

            // Process the remaining bytes using the scalar method
            count += ScalarParsers.CountWhitespacesAscii(buffer.Slice(pos));
            return count;
        }
    }
}

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Sparrow;


namespace Corax.Pipeline.Parsing
{
    internal static class VectorParsers
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int FindFirstNonAscii(ReadOnlySpan<byte> buffer)
        {
            ref byte bufferStart = ref MemoryMarshal.GetReference(buffer);

            nint pos = 0;
            nint len = buffer.Length;

            if (AdvInstructionSet.IsAcceleratedVector512)
            {
                // process in blocks of 64 bytes when possible
                const int N512 = 64;
                
                for (; pos + N512 < len; pos += N512)
                {
                    Vector512<byte> v1 = Vector512.LoadUnsafe(ref Unsafe.AddByteOffset(ref bufferStart, pos));
                    Vector512<byte> v2 = Vector512.LoadUnsafe(ref Unsafe.AddByteOffset(ref bufferStart, pos + N512));
                    var v = Vector512.BitwiseOr(v1, v2);

                    Vector512<byte> mask = Vector512.Create((byte)0x80);
                    Vector512<byte> result = Vector512.BitwiseAnd(v, mask);

                    if (result != Vector512<byte>.Zero)
                        break;
                }
            }
            
            if (AdvInstructionSet.IsAcceleratedVector256)
            {
                // process in blocks of 32 bytes when possible
                const int N256 = 32;

                for (; pos + N256 < len; pos += N256)
                {
                    Vector256<byte> v1 = Vector256.LoadUnsafe(ref Unsafe.AddByteOffset(ref bufferStart, pos));
                    Vector256<byte> v2 = Vector256.LoadUnsafe(ref Unsafe.AddByteOffset(ref bufferStart, pos + N256));
                    var v = Vector256.BitwiseOr(v1, v2);

                    Vector256<byte> mask = Vector256.Create((byte)0x80);
                    Vector256<byte> result = Vector256.BitwiseAnd(v, mask);

                    if (result != Vector256<byte>.Zero)
                        break;
                }
            }

            // process in blocks of 16 bytes when possible
            const int N128 = 16;
            
            for (; pos + N128 < len; pos += N128)
            {
                Vector128<byte> v1 = Vector128.LoadUnsafe(ref Unsafe.AddByteOffset(ref bufferStart, pos));
                Vector128<byte> v2 = Vector128.LoadUnsafe(ref Unsafe.AddByteOffset(ref bufferStart, pos + N128));
                var v = Vector128.BitwiseOr(v1, v2);

                Vector128<byte> mask = Vector128.Create((byte)0x80);
                Vector128<byte> result = Vector128.BitwiseAnd(v, mask);

                if (result != Vector128<byte>.Zero)
                    break;
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

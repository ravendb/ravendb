using System;
using System.Runtime.CompilerServices;

namespace Sparrow.Platform
{
#if NETCOREAPP3_1_OR_GREATER
    public static unsafe partial class Sodium
    {
        public const int GenericHashSize = 32;
        public static void GenericHash(ReadOnlySpan<byte> data, Span<byte> hash)
        {
            PortableExceptions.ThrowIfOnDebug<ArgumentOutOfRangeException>(hash.Length != (int)crypto_generichash_bytes());
            Hash(data, hash);
        }

        public static void GenericHash16(ReadOnlySpan<byte> data, Span<byte> hash)
        {
            PortableExceptions.ThrowIfOnDebug<ArgumentOutOfRangeException>(hash.Length != 16);
            Hash(data, hash);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void Hash(ReadOnlySpan<byte> data, Span<byte> hash)
        {
            fixed (byte* h = hash, d = data)
            {
                int rc = crypto_generichash(h, (UIntPtr)hash.Length, d, (ulong)data.Length, null, UIntPtr.Zero);
                PortableExceptions.ThrowIf<InvalidOperationException>(rc != 0, "Failed to compute hash");
            }
        }

        public static void ZeroBuffer(byte[] buffer)
        {
            fixed (byte* p = buffer)
            {
                sodium_memzero(p, (UIntPtr)buffer.Length);
            }
        }

        public static byte[] GenerateRandomBuffer(int bytes)
        {
            var buffer = new byte[bytes];
            fixed (byte* p = buffer)
            {
                randombytes_buf(p, (UIntPtr)bytes);
            }
            return buffer;
        }
    }
#endif
}

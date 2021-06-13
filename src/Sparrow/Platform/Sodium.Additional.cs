using System;

namespace Sparrow.Platform
{
#if NETCOREAPP3_1_OR_GREATER
    public static unsafe partial class Sodium
    {
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

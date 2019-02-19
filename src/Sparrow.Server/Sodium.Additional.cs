using System;

namespace Sparrow.Server
{
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
}

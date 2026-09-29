using System;

namespace Sparrow.Server.Strings
{
    public static class StringsExtensions
    {

        public static unsafe bool IsEqualConstant(this ReadOnlySpan<byte> constant, byte* ptr)
        {
            return Memory.IsEqualConstant(constant, ptr);
        }

        public static bool IsEqualConstant(this ReadOnlySpan<byte> constant, ReadOnlySpan<byte> value)
        {
            return Memory.IsEqualConstant(constant, value);
        }

        public static unsafe bool IsEqualConstant(this ReadOnlySpan<byte> constant, byte* ptr, int size)
        {
            return Memory.IsEqualConstant(constant, ptr, size);
        }
    }
}

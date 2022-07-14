using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow.Server.Utils;

namespace Sparrow.Server
{
    public static unsafe class Format
    {
        // PERF: Separated this code on purpose because I intend to optimize this further on vNext.
        //       Similar code with extended functionality should be moved here to have the same treatment.
        //       https://issues.hibernatingrhinos.com/issue/RavenDB-9588

        public static class Backwards
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void WriteNumber(char* ptr, ulong value)
            {
                int i = 0;

                do
                {
                    // PERF: This is faster because the JIT cannot figure out the idiom: (x,y) = value \ c, value % c
                    ulong div = value / 10;
                    ulong v = value - div * 10;
                    value = div;

                    ptr[i--] = (char)('0' + v);
                }
                while (value != 0);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void WriteNumber(byte* ptr, ulong value)
            {
                int i = 0;
                do
                {
                    // PERF: This is faster because the JIT cannot figure out the idiom: (x,y) = value \ c, value % c
                    ulong div = value / 10;
                    ulong v = value - div * 10;
                    value = div;

                    ptr[i--] = (byte)('0' + v);
                }
                while (value != 0);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void WriteNumber(StringBuilder sb, int offset, long value)
            {
                do
                {
                    // PERF: This is faster because the JIT cannot figure out the idiom: (x,y) = value \ c, value % c
                    long div = value / 10;
                    long v = value - div * 10;
                    value = div;

                    sb[offset--] = (char)((char)v + '0');
                }
                while (value != 0);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string ToBase64Unpadded(this Guid guid)
        {
            var span = MemoryMarshal.CreateSpan(ref guid, 1);
            return ToBase64Unpadded(MemoryMarshal.Cast<Guid, byte>(span));
        }

        public static string ToBase64Unpadded(Span<byte> bytes)
        {
            if (bytes.Length != 16)
                throw new ArgumentException("Expected buffer to be exactly 16 bytes long");
            string result = new string(' ', 22);
            fixed (char* pChars = result)
            fixed(byte* pBytes = bytes)
            {
                int size = Base64.ConvertToBase64ArrayUnpadded(pChars, pBytes, 0, 16);
                Debug.Assert(size == 22);
            }

            return result;
        }
    }
}

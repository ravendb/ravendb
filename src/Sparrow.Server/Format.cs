using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow.Utils;

namespace Sparrow
{
    public static unsafe class Format
    {
        // PERF: Separated this code on purpose because I intend to optimize this further on vNext.
        //       Similar code with extended functionality should be moved here to have the same treatment.
        //       http://issues.hibernatingrhinos.com/issue/RavenDB-9588

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
            string result = new string(' ', 22);
            fixed (char* pChars = result)
            {
                int size = Base64.ConvertToBase64ArrayUnpadded(pChars, (byte*)&guid, 0, 16);
                Debug.Assert(size == 22);
            }

            return result;
        }
    }
}

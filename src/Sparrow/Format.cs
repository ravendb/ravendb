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
                    var v = value % 10;

                    ptr[i--] = (char)('0' + v);
                    value /= 10;
                }
                while (value != 0);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void WriteNumber(byte* ptr, ulong value)
            {
                int i = 0;
                do
                {
                    var v = value % 10;

                    ptr[i--] = (byte)('0' + v);
                    value /= 10;
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

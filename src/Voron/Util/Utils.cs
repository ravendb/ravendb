using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;

namespace Voron.Util
{
    public static class Utils
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Guid Xor(this Guid a, Guid b)
        {
            var av = Unsafe.As<Guid, Vector128<byte>>(ref a);
            var bv = Unsafe.As<Guid, Vector128<byte>>(ref b);
            var rv = av ^ bv;
            return Unsafe.As<Vector128<byte>, Guid>(ref rv);
        }

        public static T[] Concat<T>(this T[] array, T next)
        {
            var t = new T[array.Length + 1];
            Array.Copy(array, t, array.Length);
            t[array.Length] = next;
            return t;
        }

        public static T[] Concat<T>(this List<T> array, T next)
        {
            var t = new T[array.Count + 1];
            array.CopyTo(t);
            t[array.Count] = next;
            return t;
        }
    }
}

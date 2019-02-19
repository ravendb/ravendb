using System;
using System.Collections.Generic;

namespace Voron.Util
{
    public static class Utils
    {
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

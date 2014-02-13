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
		public static long NearestPowerOfTwo(long v)
		{
			v--;
			v |= v >> 1;
			v |= v >> 2;
			v |= v >> 4;
			v |= v >> 8;
			v |= v >> 16;
			v++;
			return v;

		}
	}
}
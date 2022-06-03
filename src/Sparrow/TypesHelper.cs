using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow
{
    public static class TypesHelper
    {

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsNumerical<T>()
        {
            return typeof(T) == typeof(long) || typeof(T) == typeof(int)
                || typeof(T) == typeof(short) || typeof(T) == typeof(byte)
                || typeof(T) == typeof(ulong) || typeof(T) == typeof(uint)
                || typeof(T) == typeof(ushort) || typeof(T) == typeof(sbyte)
                || typeof(T) == typeof(float) || typeof(T) == typeof(double);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsInteger<T>()
        {
            return typeof(T) == typeof(long) || typeof(T) == typeof(int)
                || typeof(T) == typeof(short) || typeof(T) == typeof(byte)
                || typeof(T) == typeof(ulong) || typeof(T) == typeof(uint)
                || typeof(T) == typeof(ushort) || typeof(T) == typeof(sbyte);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsFloatingPoint<T>()
        {
            return typeof(T) == typeof(float) || typeof(T) == typeof(double);
        }
    }
}

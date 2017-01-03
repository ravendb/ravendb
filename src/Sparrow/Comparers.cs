using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Sparrow
{
    public class NumericEqualityComparer : IEqualityComparer<long>, IEqualityComparer<int>, IEqualityComparer<ulong>
    {
        public static readonly NumericEqualityComparer Instance = new NumericEqualityComparer();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(long x, long y)
        {
            return x == y;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(long obj)
        {
            return unchecked((int)obj ^ (int)(obj >> 32));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(int x, int y)
        {
            return x == y;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(int obj)
        {
            return obj;
        }

        public bool Equals(ulong x, ulong y)
        {
            return x == y;
        }

        public int GetHashCode(ulong obj)
        {
            return unchecked((int)obj ^ (int)(obj >> 32));
        }
    }

    public class NumericDescendingComparer : IComparer<long>, IComparer<int>
    {
        public static readonly NumericDescendingComparer Instance = new NumericDescendingComparer();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(long x, long y)
        {
            return Math.Sign(y - x);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(int x, int y)
        {
            return y - x;
        }
    }
}

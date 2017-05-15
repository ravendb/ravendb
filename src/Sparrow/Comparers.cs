using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Sparrow
{
    public class NumericEqualityComparer : IEqualityComparer<long>, IEqualityComparer<int>, IEqualityComparer<ulong>, IEqualityComparer<uint>
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
            return Hashing.Combine((int)(obj >> 32), (int)obj);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(ulong x, ulong y)
        {
            return x == y;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(ulong obj)
        {
            return Hashing.Combine((int)(obj >> 32), (int)obj);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(uint x, uint y)
        {
            return x == y;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(uint obj)
        {
            return Hashing.Combine((int)(obj >> 32), (int)obj);
        }
    }

    public struct NumericEqualityStructComparer : IEqualityComparer<long>, IEqualityComparer<int>, IEqualityComparer<ulong>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(long x, long y)
        {
            return x == y;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(long obj)
        {
            return Hashing.Mix(obj);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(ulong x, ulong y)
        {
            return x == y;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(ulong obj)
        {
            return (int) Hashing.Mix(obj);
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

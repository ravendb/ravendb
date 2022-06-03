using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Sparrow
{
    public struct NumericEqualityComparer : IEqualityComparer<long>, IEqualityComparer<int>, IEqualityComparer<ulong>, IEqualityComparer<uint>
    {
        public static readonly IEqualityComparer<long> BoxedInstanceInt64 = new NumericEqualityComparer();
        public static readonly IEqualityComparer<ulong> BoxedInstanceUInt64 = new NumericEqualityComparer();
        public static readonly IEqualityComparer<int> BoxedInstanceInt32 = new NumericEqualityComparer();
        public static readonly IEqualityComparer<uint> BoxedInstanceUInt32 = new NumericEqualityComparer();
        
        public static readonly NumericEqualityComparer StructInstance = new NumericEqualityComparer();


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
            return Hashing.Mix(obj);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(ulong x, ulong y)
        {
            return x == y;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(ulong obj)
        {
            return Hashing.Mix((long)obj);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(uint x, uint y)
        {
            return x == y;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(uint obj)
        {
            return (int)Hashing.Mix(obj);
        }
    }

    public struct NumericComparer : IComparer<long>, IComparer<int>, IComparer<uint>, IComparer<ulong>
    {
        public static readonly IComparer<long> BoxedInstanceInt64 = new NumericComparer();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(long x, long y)
        {
            return Math.Sign(x - y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(int x, int y)
        {
            return x - y;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(ulong x, ulong y)
        {
            // We need to use branching here because without sign flags we can overflow and return wrong values.
            return x == y ? 0 : x > y ? 1 : -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(uint x, uint y)
        {
            // We need to use branching here because without sign flags we can overflow and return wrong values.
            return x == y ? 0 : x > y ? 1 : -1;
        }
    }

    public struct NumericDescendingComparer : IComparer<long>, IComparer<int>, IComparer<uint>, IComparer<ulong>, IComparer<float>, IComparer<double>
    {
        public static readonly IComparer<long> BoxedInstanceInt64 = new NumericDescendingComparer();

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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(float x, float y)
        {
            return x == y ? 0 : x < y ? 1 : -1;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(double x, double y)
        {
            return x == y ? 0 : x < y ? 1 : -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(ulong x, ulong y)
        {
            // We need to use branching here because without sign flags we can overflow and return wrong values.
            return x == y ? 0 : x < y ? 1 : -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(uint x, uint y)
        {
            // We need to use branching here because without sign flags we can overflow and return wrong values.
            return x == y ? 0 : x < y ? 1 : -1;
        }
    }
}

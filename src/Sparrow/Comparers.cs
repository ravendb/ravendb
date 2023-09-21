using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Sparrow
{
    internal readonly struct NumericDescendingComparer : IComparer<long>, IComparer<int>, IComparer<uint>, IComparer<ulong>, IComparer<float>, IComparer<double>
    {
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

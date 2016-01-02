using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow
{
    public class NumericEqualityComparer : IEqualityComparer<long>, IEqualityComparer<int>
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

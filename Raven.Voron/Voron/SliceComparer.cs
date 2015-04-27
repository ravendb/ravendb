using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Voron.Util;

namespace Voron
{
    public sealed class SliceComparer : IEqualityComparer<Slice>, IComparer<Slice>
    {
        public static readonly SliceComparer Instance = new SliceComparer();
       
        public int Compare(Slice x, Slice y)
        {
            return CompareInline(x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline ( Slice x, Slice y )
        {
            Debug.Assert(x.Options == SliceOptions.Key);
            Debug.Assert(y.Options == SliceOptions.Key);

            var srcKey = x.KeyLength;
            var otherKey = y.KeyLength;
            var length = srcKey <= otherKey ? srcKey : otherKey;

            var r = x.CompareDataInline(y, length);
            if (r != 0)
                return r;

            return srcKey - otherKey;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(Slice x, Slice y)
        {
            Debug.Assert(x.Options == SliceOptions.Key);
            Debug.Assert(y.Options == SliceOptions.Key);

            var srcKey = x.KeyLength;
            var otherKey = y.KeyLength;
            if (srcKey != otherKey)
                return false;

            var length = srcKey <= otherKey ? srcKey : otherKey;

            return x.CompareDataInline(y, length) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(Slice obj)
        {
            return obj.GetHashCode();
        }
    }
}

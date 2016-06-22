using Sparrow;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Voron.Util;

namespace Voron
{
    public sealed class SliceComparer : IEqualityComparer<Slice>, IComparer<Slice>
    {
        public static readonly SliceComparer Instance = new SliceComparer();

        int IComparer<Slice>.Compare(Slice x, Slice y)
        {
            return CompareInline(x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Compare( Slice x, Slice y)
        {
            return CompareInline(x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline(Slice x, Slice y)
        {
            Debug.Assert(x.Options == SliceOptions.Key);
            Debug.Assert(y.Options == SliceOptions.Key);

            var x1 = x.Content;
            var y1 = y.Content;
            if (x1 == y1) // Reference equality (specially useful on searching on collections)
                return 0;

            int r, keyDiff;
            unsafe
            {
                var size = Math.Min(x1.Length, y1.Length);
                keyDiff = x1.Length - y1.Length;

                r = Memory.CompareInline(x1.Ptr, y1.Ptr, size);
            }

            if (r != 0)
                return r;

            return keyDiff;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool IEqualityComparer<Slice>.Equals(Slice x, Slice y)
        {
            Debug.Assert(x.Options == SliceOptions.Key);
            Debug.Assert(y.Options == SliceOptions.Key);

            var srcKey = x.Content.Length;
            var otherKey = y.Content.Length;
            if (srcKey != otherKey)
                return false;

            return CompareInline(x, y) == 0;
        }

        public static bool Equals(Slice x, Slice y)
        {
            return EqualsInline(x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool EqualsInline(Slice x, Slice y)
        {
            Debug.Assert(x.Options == SliceOptions.Key);
            Debug.Assert(y.Options == SliceOptions.Key);

            var srcKey = x.Content.Length;
            var otherKey = y.Content.Length;
            if (srcKey != otherKey)
                return false;

            return CompareInline(x, y) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        int IEqualityComparer<Slice>.GetHashCode(Slice obj)
        {
            return obj.GetHashCode();
        }

        public unsafe static bool StartWith(Slice value, Slice prefix)
        {
            int prefixSize = prefix.Content.Length;
            if (!value.Content.HasValue || prefixSize > value.Content.Length)
                return false;

            byte* prefixPtr = prefix.Content.Ptr;
            byte* valuePtr = value.Content.Ptr;

            return Memory.CompareInline(prefixPtr, valuePtr, prefix.Size) == 0;
        }  
    }
}

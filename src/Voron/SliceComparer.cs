using Sparrow;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;

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
        public static int Compare(Slice x, Slice y)
        {
            return CompareInline(x, y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CompareInline(Slice x, Slice y)
        {
            Debug.Assert(x.HasValue && y.HasValue);
            Debug.Assert(x.Options == SliceOptions.Key);
            Debug.Assert(y.Options == SliceOptions.Key);

            int r, keyDiff;
            var x1 = x.Content;
            var y1 = y.Content;

            unsafe
            {
                if (x1._pointer == y1._pointer) // Reference equality (specially useful on searching on collections)
                    return 0;

                int x1Length = x1.Length;
                int y1Length = y1.Length;
                var size = Math.Min(x1Length, y1Length);
                keyDiff = x1Length - y1Length;

                r = Memory.CompareInline(x1.Ptr, y1.Ptr, size);
            }

            if (r != 0)
                return r;

            return keyDiff;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool IEqualityComparer<Slice>.Equals(Slice x, Slice y)
        {
            return AreEqual(x, y);
        }

        public static bool AreEqual(Slice x, Slice y)
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
            unsafe
            {
                byte* ptr = obj.Content.Ptr;
                int size = obj.Content.Length;

                return (int)Hashing.Marvin32.CalculateInline(ptr, size);
            }
        }

        public static unsafe bool StartWith(Slice value, Slice prefix)
        {
            int prefixSize = prefix.Content.Length;
            if (!value.Content.HasValue || prefixSize > value.Content.Length)
                return false;

            byte* prefixPtr = prefix.Content.Ptr;
            byte* valuePtr = value.Content.Ptr;

            return Memory.CompareInline(prefixPtr, valuePtr, prefix.Size) == 0;
        }
    }

    public struct SliceStructComparer : IEqualityComparer<Slice>, IComparer<Slice>
    {
        public static readonly SliceStructComparer Instance = new SliceStructComparer();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(Slice x, Slice y)
        {
            Debug.Assert(x.HasValue && y.HasValue);
            Debug.Assert(x.Options == SliceOptions.Key);
            Debug.Assert(y.Options == SliceOptions.Key);

            int r, keyDiff;
            var x1 = x.Content;
            var y1 = y.Content;

            unsafe
            {
                if (x1._pointer == y1._pointer) // Reference equality (specially useful on searching on collections)
                    return 0;

                int x1Length = x1.Length;
                int y1Length = y1.Length;
                var size = Math.Min(x1Length, y1Length);
                keyDiff = x1Length - y1Length;

                r = Memory.CompareInline(x1.Ptr, y1.Ptr, size);
            }

            if (r != 0)
                return r;

            return keyDiff;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(Slice x, Slice y)
        {
            Debug.Assert(x.Options == SliceOptions.Key);
            Debug.Assert(y.Options == SliceOptions.Key);

            var srcKey = x.Content.Length;
            var otherKey = y.Content.Length;
            if (srcKey != otherKey)
                return false;

            return Compare(x, y) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(Slice obj)
        {
            unsafe
            {
                byte* ptr = obj.Content.Ptr;
                int size = obj.Content.Length;

                return (int)Hashing.Marvin32.CalculateInline(ptr, size);
            }
        }
    }
}

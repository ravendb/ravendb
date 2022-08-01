using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using Sparrow.Binary;
using Sparrow.Utils;

namespace Sparrow
{
    public struct StringSegmentEqualityStructComparer : IEqualityComparer<StringSegment>
    {
        public static IEqualityComparer<StringSegment> BoxedInstance = new StringSegmentEqualityStructComparer();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(StringSegment x, StringSegment y)
        {
            return x.AsSpan().SequenceEqual(y.AsSpan());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(StringSegment x)
        {
            return (int)Hashing.Marvin32.CalculateInline<Hashing.OrdinalModifier>(x.AsSpan());
        }
    }
}

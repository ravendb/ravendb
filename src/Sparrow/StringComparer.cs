﻿using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Sparrow
{
    internal struct StringSegmentEqualityStructComparer : IEqualityComparer<StringSegment>
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

﻿using System;
using System.Runtime.CompilerServices;

namespace Corax.Queries
{
    public enum MatchCompareFieldType
    {
        Sequence,
        Integer,
        Floating
    }

    public interface IMatchComparer
    {        
        MatchCompareFieldType FieldType { get; }

        int FieldId { get; }

        int CompareById(long idx, long idy);

        int CompareSequence(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy, long idx, long idy);
        int CompareNumerical<T>(T sx, T sy, long idx, long idy) where T : unmanaged;
    }

    internal static class BasicComparers
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int CompareAscending(ReadOnlySpan<byte> x, ReadOnlySpan<byte> y)
        {
            return x.SequenceCompareTo(y);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int CompareAscending<T>(T x, T y)
        {
            if (typeof(T) == typeof(long))
            {
                return Math.Sign((long)(object)y - (long)(object)x);
            }
            else if (typeof(T) == typeof(double))
            {
                return Math.Sign((double)(object)y - (double)(object)x);
            }

            throw new NotSupportedException("Not supported");
        }
    }
}

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Utils;
using Corax.Utils.Spatial;
using Spatial4n.Shapes;

namespace Corax.Queries
{
    public enum MatchCompareFieldType
    {
        Sequence,
        Integer,
        Floating,
        Score,
        Alphanumeric,
        Spatial
    }

    public interface IMatchComparer
    {        
        MatchCompareFieldType FieldType { get; }

        FieldMetadata Field { get; }

        int CompareById(long idx, long idy);

        int CompareSequence(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy);
        int CompareNumerical<T>(T sx, T sy) where T : unmanaged, INumber<T>;
    }

    public interface ISpatialComparer : IMatchComparer
    {
        double Round { get; }
        
        SpatialUnits Units { get; }
        
        IPoint Point { get; }
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

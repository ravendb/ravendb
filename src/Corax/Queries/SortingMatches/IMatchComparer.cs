using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Utils.Spatial;
using Spatial4n.Shapes;

namespace Corax.Queries.SortingMatches
{
    public enum MatchCompareFieldType : ushort
    {
        Sequence,
        Integer,
        Floating,
        Score,
        Alphanumeric,
        Spatial,
        Random
    }

    public interface IMatchComparer
    {        
        MatchCompareFieldType FieldType { get; }

        FieldMetadata Field { get; }

        int CompareSequence(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy);
        int CompareNumerical<T>(T sx, T sy) where T : unmanaged, INumber<T>;
    }

    public interface ISpatialComparer : IMatchComparer
    {
        double Round { get; }
        
        SpatialUnits Units { get; }
        
        IPoint Point { get; }
    }
}

using System;
using System.Numerics;
using Corax.Mappings;

namespace Corax.Queries.SortingMatches.Meta
{
    public interface IMatchComparer
    {        
        MatchCompareFieldType FieldType { get; }

        FieldMetadata Field { get; }

        int CompareSequence(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy);
        
        int CompareNumerical<T>(T sx, T sy) where T : unmanaged, INumber<T>;
    }
}

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Utils;

namespace Corax.Queries.SortingMatches.Comparers;

unsafe partial struct LegacySortingMatch
{
    public unsafe struct AscendingMatchComparer : IMatchComparer
    {
        private readonly FieldMetadata _field;
        private readonly MatchCompareFieldType _fieldType;

        public AscendingMatchComparer(IndexSearcher searcher, OrderMetadata orderMetadata)
        {
            _field = orderMetadata.Field;
            _fieldType = orderMetadata.FieldType;
        }

        public FieldMetadata Field => _field;
        public MatchCompareFieldType FieldType => _fieldType;


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CompareNumerical<T>(T sx, T sy) where T : unmanaged, INumber<T>
        {
            return BasicComparers.CompareAscending(sx, sy);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CompareSequence(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            return BasicComparers.CompareAscending(sx, sy);
        }
    }
}

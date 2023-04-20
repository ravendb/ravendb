using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Utils;

namespace Corax.Queries;

unsafe partial struct SortingMatch
{
    public unsafe struct AscendingMatchComparer : IMatchComparer
        {
            private readonly IndexSearcher _searcher;
            private readonly FieldMetadata _field;
            private readonly delegate*<ref AscendingMatchComparer, long, long, int> _compareFunc;
            private readonly MatchCompareFieldType _fieldType;

            public FieldMetadata Field => _field;
            public MatchCompareFieldType FieldType => _fieldType;

            public AscendingMatchComparer(IndexSearcher searcher, OrderMetadata orderMetadata)
            {
                _searcher = searcher;
                _field = orderMetadata.Field;
                _fieldType = orderMetadata.FieldType;

                if (orderMetadata.Ascending == false)
                    throw new ArgumentException($"The metadata for field '{orderMetadata.Field.FieldName}' is not marked as 'Ascending == true' ");

                static int CompareWithLoadSequence(ref AscendingMatchComparer comparer, long x, long y)
                {
                    var readerX = comparer._searcher.GetEntryReaderFor(x);
                    var readX = readerX.GetFieldReaderFor(comparer._field).Read( out var resultX);

                    var readerY = comparer._searcher.GetEntryReaderFor(y);
                    var readY = readerY.GetFieldReaderFor(comparer._field).Read( out var resultY);

                    if (readX && readY)
                    {
                        return comparer.CompareSequence(resultX, resultY);
                    }
                    else if (readX)
                        return 1;
                    return -1;
                }

                static int CompareWithLoadNumerical<T>(ref AscendingMatchComparer comparer, long x, long y) where T : unmanaged, INumber<T>
            {
                    var readerX = comparer._searcher.GetEntryReaderFor(x);
                    var readX = readerX.GetFieldReaderFor(comparer._field).Read<T>( out var resultX);

                    var readerY = comparer._searcher.GetEntryReaderFor(y);
                    var readY = readerY.GetFieldReaderFor(comparer._field).Read<T>( out var resultY);

                    if (readX && readY)
                    {
                        return comparer.CompareNumerical(resultX, resultY);
                    }
                    else if (readX)
                        return 1;
                    return -1;
                }

                _compareFunc = _fieldType switch
                {
                    MatchCompareFieldType.Sequence => &CompareWithLoadSequence,
                    MatchCompareFieldType.Integer => &CompareWithLoadNumerical<long>,
                    MatchCompareFieldType.Floating => &CompareWithLoadNumerical<double>,
                    MatchCompareFieldType.Spatial => &CompareWithLoadNumerical<double>,
                    var type => throw new NotSupportedException($"Currently, we do not support sorting by {type}.")
                };
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int CompareById(long idx, long idy)
            {
                return _compareFunc(ref this, idx, idy);
            }

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

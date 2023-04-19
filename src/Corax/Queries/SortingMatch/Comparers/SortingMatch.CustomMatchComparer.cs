using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Utils;

namespace Corax.Queries;

unsafe partial struct SortingMatch
{
    public unsafe struct CustomMatchComparer : IMatchComparer
        {
            private readonly IndexSearcher _searcher;
            private readonly FieldMetadata _field;
            private readonly delegate*<ref CustomMatchComparer, long, long, int> _compareWithLoadFunc;
            private readonly delegate*<IndexSearcher, int, long, long, int> _compareByIdFunc;
            private readonly delegate*<long, long, int> _compareLongFunc;
            private readonly delegate*<double, double, int> _compareDoubleFunc;
            private readonly delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, int> _compareSequenceFunc;
            private readonly MatchCompareFieldType _fieldType;

            public FieldMetadata Field => _field;
            public MatchCompareFieldType FieldType => _fieldType;

            public CustomMatchComparer(IndexSearcher searcher, OrderMetadata orderMetadata,
                delegate*<IndexSearcher, int, long, long, int> compareByIdFunc,
                delegate*<long, long, int> compareLongFunc,
                delegate*<double, double, int> compareDoubleFunc,
                delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, int> compareSequenceFunc
                )
            {
                _searcher = searcher;
                _field = orderMetadata.Field;
                _fieldType = orderMetadata.FieldType;
                _compareByIdFunc = compareByIdFunc;
                _compareLongFunc = compareLongFunc;
                _compareDoubleFunc = compareDoubleFunc;
                _compareSequenceFunc = compareSequenceFunc;

                static int CompareWithLoadSequence(ref CustomMatchComparer comparer, long x, long y)
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

                static int CompareWithLoadNumerical<T>(ref CustomMatchComparer comparer, long x, long y) where T : unmanaged, INumber<T>
            {
                    var readerX = comparer._searcher.GetEntryReaderFor(x);
                    var readX = readerX.GetFieldReaderFor(comparer._field).Read<T>(out var resultX);

                    var readerY = comparer._searcher.GetEntryReaderFor(y);
                    var readY = readerY.GetFieldReaderFor(comparer._field).Read<T>(out var resultY);

                    if (readX && readY)
                    {
                        return comparer.CompareNumerical(resultX, resultY);
                    }
                    if (readX)
                        return 1;
                    return -1;
                }

                _compareWithLoadFunc = _fieldType switch
                {
                    MatchCompareFieldType.Sequence => &CompareWithLoadSequence,
                    MatchCompareFieldType.Integer => &CompareWithLoadNumerical<long>,
                    MatchCompareFieldType.Floating => &CompareWithLoadNumerical<double>,
                    var type => throw new NotSupportedException($"Currently, we do not support sorting by {type}.")
                };
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int CompareById(long idx, long idy)
            {
                return _compareWithLoadFunc(ref this, idx, idy);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int CompareNumerical<T>(T sx, T sy) where T : unmanaged, INumber<T>
        {
                if (typeof(T) == typeof(long))
                    return _compareLongFunc((long)(object)sx, (long)(object)sy);
                else if (typeof(T) == typeof(double))
                    return _compareDoubleFunc((double)(object)sx, (double)(object)sy);

                throw new NotSupportedException("Not supported.");
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int CompareSequence(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
            {
                return _compareSequenceFunc(sx, sy);
            }
        }
}

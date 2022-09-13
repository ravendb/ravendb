using System;
using System.Runtime.CompilerServices;

namespace Corax.Queries;

unsafe partial struct SortingMatch
{
    public unsafe struct CustomMatchComparer : IMatchComparer
        {
            private readonly IndexSearcher _searcher;
            private readonly int _fieldId;
            private readonly delegate*<ref CustomMatchComparer, long, long, int> _compareWithLoadFunc;
            private readonly delegate*<IndexSearcher, int, long, long, int> _compareByIdFunc;
            private readonly delegate*<long, long, int> _compareLongFunc;
            private readonly delegate*<double, double, int> _compareDoubleFunc;
            private readonly delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, int> _compareSequenceFunc;
            private readonly MatchCompareFieldType _fieldType;

            public int FieldId => _fieldId;
            public MatchCompareFieldType FieldType => _fieldType;

            public CustomMatchComparer(IndexSearcher searcher, int fieldId,
                delegate*<IndexSearcher, int, long, long, int> compareByIdFunc,
                delegate*<long, long, int> compareLongFunc,
                delegate*<double, double, int> compareDoubleFunc,
                delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, int> compareSequenceFunc,
                MatchCompareFieldType entryFieldType)
            {
                _searcher = searcher;
                _fieldId = fieldId;
                _fieldType = entryFieldType;
                _compareByIdFunc = compareByIdFunc;
                _compareLongFunc = compareLongFunc;
                _compareDoubleFunc = compareDoubleFunc;
                _compareSequenceFunc = compareSequenceFunc;

                static int CompareWithLoadSequence(ref CustomMatchComparer comparer, long x, long y)
                {
                    var readerX = comparer._searcher.GetReaderFor(x);
                    var readX = readerX.GetReaderFor(comparer._fieldId).Read( out var resultX);

                    var readerY = comparer._searcher.GetReaderFor(y);
                    var readY = readerY.GetReaderFor(comparer._fieldId).Read( out var resultY);

                    if (readX && readY)
                    {
                        return comparer.CompareSequence(resultX, resultY);
                    }
                    else if (readX)
                        return 1;
                    return -1;
                }

                static int CompareWithLoadNumerical<T>(ref CustomMatchComparer comparer, long x, long y) where T : unmanaged
                {
                    var readerX = comparer._searcher.GetReaderFor(x);
                    var readX = readerX.GetReaderFor(comparer.FieldId).Read<T>(out var resultX);

                    var readerY = comparer._searcher.GetReaderFor(y);
                    var readY = readerY.GetReaderFor(comparer._fieldId).Read<T>(out var resultY);

                    if (readX && readY)
                    {
                        return comparer.CompareNumerical(resultX, resultY);
                    }
                    else if (readX)
                        return 1;
                    return -1;
                }

                _compareWithLoadFunc = entryFieldType switch
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
            public int CompareNumerical<T>(T sx, T sy) where T : unmanaged
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

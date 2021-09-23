using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Queries
{
    partial struct SortingMatch
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
                    var readX = readerX.Read(comparer._fieldId, out var resultX);

                    var readerY = comparer._searcher.GetReaderFor(y);
                    var readY = readerY.Read(comparer._fieldId, out var resultY);

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
                    var readX = readerX.Read<T>(comparer._fieldId, out var resultX);

                    var readerY = comparer._searcher.GetReaderFor(y);
                    var readY = readerY.Read<T>(comparer._fieldId, out var resultY);

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

        public unsafe struct AscendingMatchComparer : IMatchComparer
        {
            private readonly IndexSearcher _searcher;
            private readonly int _fieldId;
            private readonly delegate*<ref AscendingMatchComparer, long, long, int> _compareFunc;
            private readonly MatchCompareFieldType _fieldType;

            public int FieldId => _fieldId;
            public MatchCompareFieldType FieldType => _fieldType;

            public AscendingMatchComparer(IndexSearcher searcher, int fieldId, MatchCompareFieldType entryFieldType)
            {
                _searcher = searcher;
                _fieldId = fieldId;
                _fieldType = entryFieldType;

                static int CompareWithLoadSequence(ref AscendingMatchComparer comparer, long x, long y)
                {
                    var readerX = comparer._searcher.GetReaderFor(x);
                    var readX = readerX.Read(comparer._fieldId, out var resultX);

                    var readerY = comparer._searcher.GetReaderFor(y);
                    var readY = readerY.Read(comparer._fieldId, out var resultY);

                    if (readX && readY)
                    {
                        return comparer.CompareSequence(resultX, resultY);
                    }
                    else if (readX)
                        return 1;
                    return -1;
                }

                static int CompareWithLoadNumerical<T>(ref AscendingMatchComparer comparer, long x, long y) where T : unmanaged
{
                    var readerX = comparer._searcher.GetReaderFor(x);
                    var readX = readerX.Read<T>(comparer._fieldId, out var resultX);

                    var readerY = comparer._searcher.GetReaderFor(y);
                    var readY = readerY.Read<T>(comparer._fieldId, out var resultY);

                    if (readX && readY)
                    {
                        return comparer.CompareNumerical(resultX, resultY);
                    }
                    else if (readX)
                        return 1;
                    return -1;
                }

                _compareFunc = entryFieldType switch
                {
                    MatchCompareFieldType.Sequence => &CompareWithLoadSequence,
                    MatchCompareFieldType.Integer => &CompareWithLoadNumerical<long>,
                    MatchCompareFieldType.Floating => &CompareWithLoadNumerical<double>,
                };
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int CompareById(long idx, long idy)
            {
                return _compareFunc(ref this, idx, idy);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int CompareNumerical<T>(T sx, T sy) where T : unmanaged
            {
                return BasicComparers.CompareAscending(sx, sy);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int CompareSequence(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
            {
                return BasicComparers.CompareAscending(sx, sy);
            }
        }

        public unsafe struct DescendingMatchComparer : IMatchComparer
        {
            private readonly IndexSearcher _searcher;
            private readonly int _fieldId;
            private readonly delegate*<ref DescendingMatchComparer, long, long, int> _compareFunc;
            private readonly MatchCompareFieldType _fieldType;

            public int FieldId => _fieldId;
            public MatchCompareFieldType FieldType => _fieldType;

            public DescendingMatchComparer(IndexSearcher searcher, int fieldId, MatchCompareFieldType entryFieldType)
            {
                _searcher = searcher;
                _fieldId = fieldId;
                _fieldType = entryFieldType;

                static int CompareWithLoadSequence(ref DescendingMatchComparer comparer, long x, long y)
                {
                    var readerX = comparer._searcher.GetReaderFor(x);
                    var readX = readerX.Read(comparer._fieldId, out var resultX);

                    var readerY = comparer._searcher.GetReaderFor(y);
                    var readY = readerY.Read(comparer._fieldId, out var resultY);

                    if (readX && readY)
                    {
                        return comparer.CompareSequence(resultX, resultY);
                    }
                    else if (readX)
                        return 1;
                    return -1;
                }

                static int CompareWithLoadNumerical<T>(ref DescendingMatchComparer comparer, long x, long y) where T : unmanaged
                {
                    var readerX = comparer._searcher.GetReaderFor(x);
                    var readX = readerX.Read<T>(comparer._fieldId, out var resultX);

                    var readerY = comparer._searcher.GetReaderFor(y);
                    var readY = readerY.Read<T>(comparer._fieldId, out var resultY);

                    if (readX && readY)
                    {
                        return comparer.CompareNumerical(resultX, resultY);
                    }
                    else if (readX)
                        return 1;
                    return -1;
                }

                _compareFunc = entryFieldType switch
                {
                    MatchCompareFieldType.Sequence => &CompareWithLoadSequence,
                    MatchCompareFieldType.Integer => &CompareWithLoadNumerical<long>,
                    MatchCompareFieldType.Floating => &CompareWithLoadNumerical<double>,
                };
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int CompareById(long idx, long idy)
            {
                return _compareFunc(ref this, idx, idy);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int CompareNumerical<T>(T sx, T sy) where T : unmanaged
            {
                return -BasicComparers.CompareAscending(sx, sy);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int CompareSequence(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
            {
                return -BasicComparers.CompareAscending(sx, sy);
            }
        }
    }
}

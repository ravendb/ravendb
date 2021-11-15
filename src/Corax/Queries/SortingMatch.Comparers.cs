using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Queries
{
    unsafe partial struct SortingMatch
    {                     
        private static class BasicComparers
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
                    return Math.Sign((long)(object)x - (long)(object)y);
                }
                else if (typeof(T) == typeof(double))
                {
                    return Math.Sign((double)(object)x - (double)(object)y);
                }
                
                throw new NotSupportedException("Not supported");
            }
        }

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
                        return -1;
                    return 1;
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
                        return -1;
                    return 1;
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

        internal struct SequenceItem
        {
            public readonly byte* Ptr;
            public readonly int Size;

            public SequenceItem(byte* ptr, int size)
            {
                Ptr = ptr;
                Size = size;
            }
        }

        internal struct NumericalItem<T> where T : unmanaged
        {
            public readonly T Value;
            public NumericalItem(in T value)
            {
                Value = value;
            }
        }

        internal struct MatchComparer<T, W> : IComparer<MatchComparer<T, W>.Item>
            where T : IMatchComparer
            where W : struct
        {
            public struct Item
            {
                public long Key;
                public W Value;
            }

            private readonly T _comparer;

            public MatchComparer(in T comparer)
            {
                _comparer = comparer;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int Compare(Item ix, Item iy)
            {
                if (ix.Key > 0 && iy.Key > 0)
                {
                    if (typeof(W) == typeof(SequenceItem))
                    {
                        return _comparer.CompareSequence(
                            new ReadOnlySpan<byte>(((SequenceItem)(object)ix.Value).Ptr, ((SequenceItem)(object)ix.Value).Size),
                            new ReadOnlySpan<byte>(((SequenceItem)(object)iy.Value).Ptr, ((SequenceItem)(object)iy.Value).Size));
                    }
                    else if (typeof(W) == typeof(NumericalItem<long>))
                    {
                        return _comparer.CompareNumerical(((NumericalItem<long>)(object)ix.Value).Value, ((NumericalItem<long>)(object)iy.Value).Value);
                    }
                    else if (typeof(W) == typeof(NumericalItem<double>))
                    {
                        return _comparer.CompareNumerical(((NumericalItem<double>)(object)ix.Value).Value, ((NumericalItem<double>)(object)iy.Value).Value);
                    }
                }
                else if (ix.Key > 0)
                {
                    return 1;
                }

                return -1;
            }
        }
    }
}

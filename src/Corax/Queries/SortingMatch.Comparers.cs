using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace Corax.Queries
{
    unsafe partial struct SortingMatch
    {                     
        internal static class BasicComparers
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static int CompareAscending(ReadOnlySpan<byte> x, ReadOnlySpan<byte> y)
            {
                return x.SequenceCompareTo(y);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static bool IsSpace(byte c)
            {
                return c == ' ';
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static bool IsSpace(char c)
            {
                return c == ' ';
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static bool IsDigit(byte c)
            {
                return c >= '0' && c <= '9';
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static bool IsDigit(char c)
            {
                return c >= '0' && c <= '9';
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static byte ToUpper(byte c)
            {
                if (c >= 'a' && c <= 'z')
                    return (byte)(c - ('a' - 'A'));

                return c;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static char ToUpper(char c)
            {
                return char.ToUpperInvariant(c);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static bool IsSameLetterDifferentCase(byte a, byte b)
            {
                return (a >= 'A' && a <= 'z') && (b >= 'A' && b <= 'z') && (a - ('a' - 'A') == b || a + ('a' - 'A') == b);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static bool IsSameLetterDifferentCase(char a, char b)
            {
                return (a >= 'A' && a <= 'z') && (b >= 'A' && b <= 'z') && (a - ('a' - 'A') == b || a + ('a' - 'A') == b);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static bool IsLetter(byte c)
            {
                return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static bool IsLetter(char c)
            {
                return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
            }


            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static int CompareAlphanumericAscending(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
            {
                if (a.Length == 0)
                    return b.Length == 0 ? 0 : -1;
                if (a.Length == 0)
                    return 1;

                // Natural order string sorting ported from Martin Pool's C implementation as a reference. 
                //   strnatcmp.c -- Perform 'natural order' comparisons of strings in C.
                // https://github.com/sourcefrog/natsort               

                int ai = 0, bi = 0;
                while (true)
                {
                    byte ca = ai < a.Length ? a[ai] : (byte)0;
                    byte cb = bi < b.Length ? b[bi] : (byte)0;

                    // We need to check if it is ASCII or not and act accordingly.
                    if ((ca & 0b1000_0000) != 0 || (cb & 0b1000_0000) != 0)
                        goto NotAscii;

                    /* skip over leading spaces or zeros */
                    while (IsSpace(ca))
                        ca = a[++ai];

                    while (IsSpace(cb))
                        cb = b[++bi];

                    /* process run of digits */
                    if (IsDigit(ca) && IsDigit(cb))
                    {
                        bool fractional = (ca == '0' || cb == '0');
                        if (fractional)
                        {
                            int result = CompareLeft(a.Slice(ai), b.Slice(bi));
                            if (result != 0)
                                return result;
                        }
                        else
                        {
                            int result = CompareRight(a.Slice(ai), b.Slice(bi));
                            if (result != 0)
                                return result;
                        }
                    }

                    if (ca == 0 && cb == 0)
                    {
                        /* The strings compare the same.  Perhaps the caller
                               will want to call strcmp to break the tie. */
                        return 0;
                    }

                    // If both are letters... 
                    if (IsLetter(ca) && IsLetter(cb))
                    {
                        if (IsSameLetterDifferentCase(ca, cb))
                        {
                            return -(ca - cb);
                        }

                        var cca = ToUpper(ca);
                        var ccb = ToUpper(cb);
                        if (cca != ccb)
                        {
                            return cca - ccb;
                        }
                    }
                    else if (ca != cb)
                    {
                        return ca - cb;
                    }

                    ++ai;
                    ++bi;
                }

                NotAscii:
                return CompareAlphanumericAscendingUtf8(a.Slice(ai), b.Slice(bi));
            }

            private static int CompareAlphanumericAscendingUtf8(ReadOnlySpan<byte> byteA, ReadOnlySpan<byte> byteB)
            {
                var auxiliarMemory = ArrayPool<char>.Shared.Rent(byteA.Length + byteB.Length);
                var a = auxiliarMemory.AsSpan().Slice(0, byteA.Length);
                var b = auxiliarMemory.AsSpan().Slice(byteA.Length, byteB.Length);

                var aLength = Encoding.UTF8.GetChars(byteA, a);
                var bLength = Encoding.UTF8.GetChars(byteB, b);

                a.Slice(0, aLength);
                b.Slice(0, bLength);

                int result = 0;
                int ai = 0, bi = 0;
                while (true)
                {
                    char ca = ai < a.Length ? a[ai] : (char)0;
                    char cb = bi < b.Length ? b[bi] : (char)0;

                    /* skip over leading spaces or zeros */
                    while (IsSpace(ca))
                        ca = a[++ai];

                    while (IsSpace(cb))
                        cb = b[++bi];

                    /* process run of digits */
                    if (IsDigit(ca) && IsDigit(cb))
                    {
                        bool fractional = (ca == '0' || cb == '0');
                        if (fractional)
                        {
                            result = CompareLeft(a.Slice(ai), b.Slice(bi));
                            if (result != 0)
                                goto End;
                        }
                        else
                        {
                            result = CompareRight(a.Slice(ai), b.Slice(bi));
                            if (result != 0)
                                goto End;
                        }
                    }

                    if (ca == 0 && cb == 0)
                    {
                        /* The strings compare the same.  Perhaps the caller
                               will want to call strcmp to break the tie. */
                        result = 0;
                        goto End;
                    }

                    // If both are letters... 
                    if (IsLetter(ca) && IsLetter(cb))
                    {
                        if (IsSameLetterDifferentCase(ca, cb))
                        {
                            return -(ca - cb);
                        }

                        var cca = ToUpper(ca);
                        var ccb = ToUpper(cb);
                        if (cca != ccb)
                        {
                            return cca - ccb;
                        }
                    }
                    else if (ca != cb)
                    {
                        return ca - cb;
                    }

                    ++ai;
                    ++bi;
                }


                End:
                ArrayPool<char>.Shared.Return(auxiliarMemory);
                return result;
            }

            private static int CompareRight(ReadOnlySpan<byte> ax, ReadOnlySpan<byte> bx)
            {
                int bias = 0;
                int a = 0;
                int b = 0;

                /* The longest run of digits wins.  That aside, the greatest
                   value wins, but we can't know that it will until we've scanned
                   both numbers to know that they have the same magnitude, so we
                   remember it in BIAS. */
                for (; ; a++, b++)
                {
                    byte av = a < ax.Length ? ax[a] : (byte)0;
                    byte bv = b < bx.Length ? bx[b] : (byte)0;

                    if (!IsDigit(av) && !IsDigit(bv))
                        return bias;
                    if (!IsDigit(av))
                        return -1;
                    if (!IsDigit(bv))
                        return +1;
                    if (av < bv)
                    {
                        if (bias != 0)
                            bias = -1;
                    }
                    else if (av > bv)
                    {
                        if (bias != 0)
                            bias = +1;
                    }
                    else if (a == 0 || b == 0)
                        return bias;
                }
            }

            private static int CompareRight(ReadOnlySpan<char> ax, ReadOnlySpan<char> bx)
            {
                int bias = 0;
                int a = 0;
                int b = 0;

                /* The longest run of digits wins.  That aside, the greatest
                   value wins, but we can't know that it will until we've scanned
                   both numbers to know that they have the same magnitude, so we
                   remember it in BIAS. */
                for (; ; a++, b++)
                {
                    char av = a < ax.Length ? ax[a] : (char)0;
                    char bv = b < bx.Length ? bx[b] : (char)0;

                    if (!IsDigit(av) && !IsDigit(bv))
                        return bias;
                    if (!IsDigit(av))
                        return -1;
                    if (!IsDigit(bv))
                        return +1;
                    if (av < bv)
                    {
                        if (bias != 0)
                            bias = -1;
                    }
                    else if (av > bv)
                    {
                        if (bias != 0)
                            bias = +1;
                    }
                    else if (a == 0 || b == 0)
                        return bias;
                }
            }

            private static int CompareLeft(ReadOnlySpan<byte> ax, ReadOnlySpan<byte> bx)
            {
                int a = 0;
                int b = 0;

                /* Compare two left-aligned numbers: the first to have a
                   different value wins. */
                for (; ; a++, b++)
                {
                    byte av = a < ax.Length ? ax[a] : (byte)0;
                    byte bv = b < bx.Length ? bx[b] : (byte)0;

                    if (!IsDigit(av) && !IsDigit(bv))
                        return 0;
                    if (!IsDigit(av))
                        return -1;
                    if (!IsDigit(bv))
                        return +1;
                    if (av < bv)
                        return -1;
                    if (av > bv)
                        return +1;
                }
            }

            private static int CompareLeft(ReadOnlySpan<char> ax, ReadOnlySpan<char> bx)
            {
                int a = 0;
                int b = 0;

                /* Compare two left-aligned numbers: the first to have a
                   different value wins. */
                for (; ; a++, b++)
                {
                    char av = a < ax.Length ? ax[a] : (char)0;
                    char bv = b < bx.Length ? bx[b] : (char)0;

                    if (!IsDigit(av) && !IsDigit(bv))
                        return 0;
                    if (!IsDigit(av))
                        return -1;
                    if (!IsDigit(bv))
                        return +1;
                    if (av < bv)
                        return -1;
                    if (av > bv)
                        return +1;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static int CompareAscending<T>(T x, T y)
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
                    var type => throw new NotSupportedException($"Currently, we do not support sorting by {type}.")
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

        public unsafe struct AlphanumericAscendingMatchComparer : IMatchComparer
        {
            private readonly IndexSearcher _searcher;
            private readonly int _fieldId;
            private readonly delegate*<ref AlphanumericAscendingMatchComparer, long, long, int> _compareFunc;
            private readonly MatchCompareFieldType _fieldType;

            public int FieldId => _fieldId;
            public MatchCompareFieldType FieldType => _fieldType;

            public AlphanumericAscendingMatchComparer(IndexSearcher searcher, int fieldId, MatchCompareFieldType entryFieldType)
            {
                _searcher = searcher;
                _fieldId = fieldId;
                _fieldType = entryFieldType;

                static int CompareWithLoadSequence(ref AlphanumericAscendingMatchComparer comparer, long x, long y)
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

                static int CompareWithLoadNumerical<T>(ref AlphanumericAscendingMatchComparer comparer, long x, long y) where T : unmanaged
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
                    var type => throw new NotSupportedException($"Currently, we do not support sorting by {type}.")
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
                return BasicComparers.CompareAlphanumericAscending(sx, sy);
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
                    var type => throw new NotSupportedException($"Currently, we do not support sorting by {type}.")
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

        public unsafe struct AlphanumericDescendingMatchComparer : IMatchComparer
        {
            private readonly IndexSearcher _searcher;
            private readonly int _fieldId;
            private readonly delegate*<ref AlphanumericDescendingMatchComparer, long, long, int> _compareFunc;
            private readonly MatchCompareFieldType _fieldType;

            public int FieldId => _fieldId;
            public MatchCompareFieldType FieldType => _fieldType;

            public AlphanumericDescendingMatchComparer(IndexSearcher searcher, int fieldId, MatchCompareFieldType entryFieldType)
            {
                _searcher = searcher;
                _fieldId = fieldId;
                _fieldType = entryFieldType;

                static int CompareWithLoadSequence(ref AlphanumericDescendingMatchComparer comparer, long x, long y)
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

                static int CompareWithLoadNumerical<T>(ref AlphanumericDescendingMatchComparer comparer, long x, long y) where T : unmanaged
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
                    var type => throw new NotSupportedException($"Currently, we do not support sorting by {type}.")
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
                return -BasicComparers.CompareAlphanumericAscending(sx, sy);
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

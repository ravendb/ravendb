using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using Corax.Mappings;
using Corax.Utils;
using Voron;

namespace Corax.Queries;

public unsafe struct MultiUnaryItem
{
    /**
     *  MultiUnaryMatches are using same buffer from Voron to check every single condition per one document. There is no need to call UnaryMatch over and over.
     *  We decided to drop Generics here (like in rest of the code) and push comparers by pointer functions due to problems of creation complex unary queries.
     *  We've 5 diffrent comparers (Equals are handled by TermMatch, not by scanning) so number of possible permutations grows extremly fast)
     */
    public FieldMetadata Binding;
    public DataType Type;
    internal Slice SliceValueLeft;
    internal long LongValueLeft;
    internal double DoubleValueLeft;
    internal Slice SliceValueRight;
    internal long LongValueRight;
    internal double DoubleValueRight;
    internal bool IsBetween;
    internal UnaryMatchOperation LeftSideOperation;
    internal UnaryMatchOperation RightSideOperation;
    public UnaryMode Mode;

    
    public enum UnaryMode
    {
        // This case is used for NotEquals 
        All,

        // Used for most common unary matches
        Any
    }

    private readonly delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, bool> _byteComparerLeft;
    private readonly delegate*<long, long, bool> _longComparerLeft;
    private readonly delegate*<double, double, bool> _doubleComparerLeft;


    private readonly delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, bool> _byteComparerRight;
    private readonly delegate*<long, long, bool> _longComparerRight;
    private readonly delegate*<double, double, bool> _doubleComparerRight;

    private MultiUnaryItem(FieldMetadata binding, DataType dataType, bool isBetween, UnaryMatchOperation leftOperation, UnaryMatchOperation rightOperation)
    {
        Unsafe.SkipInit(out DoubleValueLeft);
        Unsafe.SkipInit(out LongValueLeft);
        Unsafe.SkipInit(out SliceValueLeft);
        Unsafe.SkipInit(out DoubleValueRight);
        Unsafe.SkipInit(out LongValueRight);
        Unsafe.SkipInit(out SliceValueRight);

        Mode = UnaryMode.Any;
        Binding = binding;
        Type = dataType;
        this.IsBetween = isBetween;
        LeftSideOperation = leftOperation;
        RightSideOperation = rightOperation;
        
        switch (leftOperation)
        {
            case UnaryMatchOperation.LessThan:
                _byteComparerLeft = &LessThanMatchComparer.Compare;
                _longComparerLeft = &LessThanMatchComparer.Compare<long>;
                _doubleComparerLeft = &LessThanMatchComparer.Compare<double>;
                break;
            case UnaryMatchOperation.LessThanOrEqual:
                _byteComparerLeft = &LessThanOrEqualMatchComparer.Compare;
                _longComparerLeft = &LessThanOrEqualMatchComparer.Compare<long>;
                _doubleComparerLeft = &LessThanOrEqualMatchComparer.Compare<double>;
                break;
            case UnaryMatchOperation.GreaterThan:
                _byteComparerLeft = &GreaterThanMatchComparer.Compare;
                _longComparerLeft = &GreaterThanMatchComparer.Compare<long>;
                _doubleComparerLeft = &GreaterThanMatchComparer.Compare<double>;
                break;
            case UnaryMatchOperation.GreaterThanOrEqual:
                _byteComparerLeft = &GreaterThanOrEqualMatchComparer.Compare;
                _longComparerLeft = &GreaterThanOrEqualMatchComparer.Compare<long>;
                _doubleComparerLeft = &GreaterThanOrEqualMatchComparer.Compare<double>;
                break;

            case UnaryMatchOperation.NotEquals:
                Mode = UnaryMode.All;
                _byteComparerLeft = &NotEqualsMatchComparer.Compare;
                _longComparerLeft = &NotEqualsMatchComparer.Compare<long>;
                _doubleComparerLeft = &NotEqualsMatchComparer.Compare<double>;
                break;
            default:
                throw new Exception("Unsupported type of operation");
        }

        switch (rightOperation)
        {
            case UnaryMatchOperation.LessThan:
                _byteComparerRight = &LessThanMatchComparer.Compare;
                _longComparerRight = &LessThanMatchComparer.Compare<long>;
                _doubleComparerRight = &LessThanMatchComparer.Compare<double>;
                break;
            case UnaryMatchOperation.LessThanOrEqual:
                _byteComparerRight = &LessThanOrEqualMatchComparer.Compare;
                _longComparerRight = &LessThanOrEqualMatchComparer.Compare<long>;
                _doubleComparerRight = &LessThanOrEqualMatchComparer.Compare<double>;
                break;
            case UnaryMatchOperation.GreaterThan:
                _byteComparerRight = &GreaterThanMatchComparer.Compare;
                _longComparerRight = &GreaterThanMatchComparer.Compare<long>;
                _doubleComparerRight = &GreaterThanMatchComparer.Compare<double>;
                break;
            case UnaryMatchOperation.GreaterThanOrEqual:
                _byteComparerRight = &GreaterThanOrEqualMatchComparer.Compare;
                _longComparerRight = &GreaterThanOrEqualMatchComparer.Compare<long>;
                _doubleComparerRight = &GreaterThanOrEqualMatchComparer.Compare<double>;
                break;

            case UnaryMatchOperation.NotEquals:
                Mode = UnaryMode.All;
                _byteComparerRight = &NotEqualsMatchComparer.Compare;
                _longComparerRight = &NotEqualsMatchComparer.Compare<long>;
                _doubleComparerRight = &NotEqualsMatchComparer.Compare<double>;
                break;
            default:
                _byteComparerRight = &EmptyComparer.Compare;
                _longComparerRight = &EmptyComparer.Compare<long>;
                _doubleComparerRight = &EmptyComparer.Compare<double>;
                break;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool CompareNumerical<T>(T value)
        where T : unmanaged
    {
        bool leftResult;
        if (Type == DataType.Long)
        {
            leftResult = _longComparerLeft(LongValueLeft, CoherseValueTypeToLong(value));
            if (IsBetween)
                return leftResult & _longComparerRight(LongValueRight, CoherseValueTypeToLong(value));
            return leftResult;
        }

        leftResult = _doubleComparerLeft(DoubleValueLeft, CoherseValueTypeToDouble(value));
        if (IsBetween)
            return leftResult & _doubleComparerRight(DoubleValueRight, CoherseValueTypeToDouble(value));
        return leftResult;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public bool CompareLiteral(ReadOnlySpan<byte> value)
    {
        var leftResult = _byteComparerLeft(SliceValueLeft.AsSpan(), value);
        return IsBetween
            ? leftResult & _byteComparerRight(SliceValueRight.AsSpan(), value)
            : leftResult;
    }

    public MultiUnaryItem(IndexSearcher searcher, FieldMetadata binding, string value, UnaryMatchOperation operation) : this(binding, DataType.Slice, false, operation, default)
    {
        SliceValueLeft = searcher.EncodeAndApplyAnalyzer(binding, value);
    }

    public MultiUnaryItem(FieldMetadata binding, long value, UnaryMatchOperation operation) : this(binding, DataType.Long, false, operation, default)
    {
        LongValueLeft = value;
    }

    public MultiUnaryItem(FieldMetadata binding, double value, UnaryMatchOperation operation) : this(binding, DataType.Double, false, operation, default)
    {
        DoubleValueLeft = value;
    }

    public MultiUnaryItem(IndexSearcher searcher, FieldMetadata binding, string leftValue, string rightValue, UnaryMatchOperation leftOperation, UnaryMatchOperation rightOperation)
        : this(binding, DataType.Slice, true, leftOperation, rightOperation)
    {
        SliceValueRight = searcher.EncodeAndApplyAnalyzer(binding, rightValue);
        SliceValueLeft = searcher.EncodeAndApplyAnalyzer(binding, leftValue);
    }

    public MultiUnaryItem(FieldMetadata binding, long valueLeft, long valueRight, UnaryMatchOperation leftOperation, UnaryMatchOperation rightOperation) : this(binding,
        DataType.Long, true, leftOperation, rightOperation)
    {
        LongValueLeft = valueLeft;
        LongValueRight = valueRight;
    }

    public MultiUnaryItem(FieldMetadata binding, double valueLeft, double valueRight, UnaryMatchOperation leftOperation, UnaryMatchOperation rightOperation) : this(binding,
        DataType.Double, true, leftOperation, rightOperation)
    {
        DoubleValueLeft = valueLeft;
        DoubleValueRight = valueRight;
    }

    public enum DataType
    {
        Slice,
        Long,
        Double
    }

    internal class GreaterThanMatchComparer
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            return sy.SequenceCompareTo(sx) > 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static bool Compare<T>(T sx, T sy) where T : unmanaged
        {
            if (typeof(T) == typeof(long))
                return ((long)(object)sy - (long)(object)sx) > 0;
            if (typeof(T) == typeof(double))
                return ((double)(object)sy - (double)(object)sx) > 0;

            throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
        }
    }

    internal class GreaterThanOrEqualMatchComparer
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            return sy.SequenceCompareTo(sx) >= 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static bool Compare<T>(T sx, T sy) where T : unmanaged
        {
            if (typeof(T) == typeof(long))
                return ((long)(object)sy - (long)(object)sx) >= 0;
            if (typeof(T) == typeof(double))
                return ((double)(object)sy - (double)(object)sx) >= 0;

            throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
        }
    }

    internal static class LessThanMatchComparer
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            return sy.SequenceCompareTo(sx) < 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static bool Compare<T>(T sx, T sy) where T : unmanaged
        {
            if (typeof(T) == typeof(long))
                return ((long)(object)sy - (long)(object)sx) < 0;
            if (typeof(T) == typeof(double))
                return ((double)(object)sy - (double)(object)sx) < 0;

            throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
        }
    }

    internal class LessThanOrEqualMatchComparer
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            return sy.SequenceCompareTo(sx) <= 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static bool Compare<T>(T sx, T sy) where T : unmanaged
        {
            if (typeof(T) == typeof(long))
                return ((long)(object)sy - (long)(object)sx) <= 0;
            if (typeof(T) == typeof(double))
                return ((double)(object)sy - (double)(object)sx) <= 0;

            throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
        }
    }

    internal class NotEqualsMatchComparer
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            return sy.SequenceCompareTo(sx) != 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static bool Compare<T>(T sx, T sy) where T : unmanaged
        {
            if (typeof(T) == typeof(long))
                return ((long)(object)sy - (long)(object)sx) != 0;
            if (typeof(T) == typeof(double))
                return ((double)(object)sy - (double)(object)sx) != 0;

            throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
        }
    }

    internal class EmptyComparer
    {
        private const string ErrorMessage = $"{nameof(EmptyComparer)} is only use to fill up static pointer. If you see this please report. There is a bug."; 
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            throw new InvalidCastException(ErrorMessage);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static bool Compare<T>(T sx, T sy) where T : unmanaged
        {
            throw new InvalidCastException(ErrorMessage);
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long CoherseValueTypeToLong<TValueType>(TValueType value) where TValueType : unmanaged
    {
        if (typeof(TValueType) == typeof(long))
            return (long)(object)value;
        if (typeof(TValueType) == typeof(ulong))
            return (long)(ulong)(object)value;
        if (typeof(TValueType) == typeof(int))
            return (long)(int)(object)value;
        if (typeof(TValueType) == typeof(uint))
            return (long)(uint)(object)value;
        if (typeof(TValueType) == typeof(short))
            return (long)(short)(object)value;
        if (typeof(TValueType) == typeof(ushort))
            return (long)(ushort)(object)value;
        if (typeof(TValueType) == typeof(byte))
            return (long)(byte)(object)value;
        if (typeof(TValueType) == typeof(sbyte))
            return (long)(sbyte)(object)value;

        throw new NotSupportedException($"Type '{typeof(TValueType).Name} is not supported. Only long, ulong, int, uint, double and float are supported.");
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static double CoherseValueTypeToDouble<TValueType>(TValueType value)
    {
        if (typeof(TValueType) == typeof(double))
            return (double)(object)value;
        if (typeof(TValueType) == typeof(float))
            return (double)(float)(object)value;

        throw new NotSupportedException($"Type '{typeof(TValueType).Name} is not supported. Only long, ulong, int, uint, double and float are supported.");
    }
}

public struct MultiUnaryMatch<TInner> : IQueryMatch
    where TInner : IQueryMatch
{
    private IndexSearcher _searcher;
    private TInner _inner;
    private MultiUnaryItem[] _comparers;

    public MultiUnaryMatch(IndexSearcher searcher, TInner inner, MultiUnaryItem[] items)
    {
        _inner = inner;
        _searcher = searcher;
        _comparers = items;
        Count = _inner.Count;
        IsBoosting = false;
    }

    public long Count { get; }
    public bool DoNotSortResults()
    {
        return _inner.DoNotSortResults();
    }

    public QueryCountConfidence Confidence => QueryCountConfidence.Low;
    public bool IsBoosting { get; }

    public int Fill(Span<long> matches)
    {
        var read = _inner.Fill(matches);
        if (read == 0)
            return 0;
        var comparerFieldsRootPages = new long[_comparers.Length];
        for (int i = 0; i < _comparers.Length; i++)
        {
            long fieldRoot = _searcher.GetLookupRootPage(_comparers[i].Binding.FieldName);
            comparerFieldsRootPages[i] = fieldRoot;
        }

        Page lastPage = default;

        int currentIdx = 0;
        for (int i = 0; i < read; ++i)
        {
            int comparerId = 0;
            for (; comparerId < _comparers.Length; ++comparerId)
            {
                long fieldsRootPage = comparerFieldsRootPages[comparerId];
                var reader = _searcher.GetEntryTermsReader(matches[i], ref lastPage);
                while (reader.MoveNext())
                {
                    if(reader.TermMetadata != fieldsRootPage)
                        continue;
                    var comparer = _comparers[comparerId];
                    if (IsAcceptedForIterator(comparer, in reader) == false)
                    {
                        goto NotMatch;
                    }
                }
            }
            Match:
            matches[currentIdx++] = matches[i];
            NotMatch: ; // the ; so we have a label for the goto
        }

        return currentIdx;
        
        bool IsAcceptedForIterator(MultiUnaryItem comparer, in EntryTermsReader iterator) => comparer.Type switch
        {
            MultiUnaryItem.DataType.Slice => comparer.CompareLiteral(iterator.Current.Decoded()),
            MultiUnaryItem.DataType.Long => comparer.CompareNumerical(iterator.CurrentLong),
            _ => comparer.CompareNumerical(iterator.CurrentDouble)
        };
    }

    public unsafe int AndWith(Span<long> buffer, int matches)
    {
        using var _ = _searcher.Allocator.Allocate(sizeof(long) * buffer.Length, out Span<byte> bufferHolder);
        var innerBuffer = MemoryMarshal.Cast<byte, long>(bufferHolder).Slice(0, buffer.Length);
        Debug.Assert(innerBuffer.Length == buffer.Length);

        var count = Fill(innerBuffer);
        int result = 0;
        if (count > 0)
        {
            fixed (long* matchesPtr = buffer)
            fixed (long* baseMatchesPtr = innerBuffer)
            {
                result = MergeHelper.And(matchesPtr, buffer.Length, matchesPtr, matches, baseMatchesPtr, count);
            }
        }
        
        return result;
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (_inner.IsBoosting)
            _inner.Score(matches, scores, boostFactor);
    }

    public QueryInspectionNode Inspect()
    {
        var dict = new Dictionary<string, string>() {{nameof(Count), $"Unknown"}};

        for (int index = 0; index < _comparers.Length; index++)
        {
            MultiUnaryItem comparer = _comparers[index];
            var prefix = $"Comparer no. {index}";
            dict.Add($"{prefix} Mode", comparer.Mode.ToString());
            dict.Add($"{prefix} Type", comparer.Type.ToString());
            dict.Add($"{prefix} IsBetween", comparer.IsBetween.ToString());
            dict.Add($"{prefix} LeftComparer", comparer.LeftSideOperation.ToString());
            if(comparer.IsBetween)
                dict.Add($"{prefix} RightComparer", comparer.RightSideOperation.ToString());
                
            
            switch (comparer.Type)
            {
                case MultiUnaryItem.DataType.Long:
                    dict.Add($"{prefix} LeftValue", comparer.LongValueLeft.ToString(CultureInfo.InvariantCulture));
                    if (comparer.IsBetween)
                        dict.Add($"{prefix} RightValue", comparer.LongValueRight.ToString(CultureInfo.InvariantCulture));
                    break;
                case MultiUnaryItem.DataType.Double:
                    dict.Add($"{prefix} LeftValue", comparer.DoubleValueLeft.ToString(CultureInfo.InvariantCulture));
                    if (comparer.IsBetween)
                        dict.Add($"{prefix} RightValue", comparer.DoubleValueRight.ToString(CultureInfo.InvariantCulture));
                    break;
                default:
                    dict.Add($"{prefix} LeftValue", comparer.SliceValueLeft.ToString());
                    if (comparer.IsBetween)
                        dict.Add($"{prefix} RightValue", comparer.SliceValueRight.ToString());
                    break;
            }
        }

        return new QueryInspectionNode($"{nameof(MultiUnaryMatch<TInner>)}",
            children: new List<QueryInspectionNode> { _inner.Inspect() },
            parameters: dict);
    }
}

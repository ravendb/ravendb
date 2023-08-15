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
     *  We've 5 different comparers (Equals are handled by TermMatch, not by scanning) so number of possible permutations grows extremely fast)
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

    private delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, bool> _byteComparerLeft;
    private delegate*<long, long, bool> _longComparerLeft;
    private delegate*<double, double, bool> _doubleComparerLeft;


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

        Mode = leftOperation == UnaryMatchOperation.NotEquals || rightOperation == UnaryMatchOperation.NotEquals ? UnaryMode.All : UnaryMode.Any;
        
        Binding = binding;
        Type = dataType;
        IsBetween = isBetween;
        LeftSideOperation = leftOperation;
        RightSideOperation = rightOperation;

        SelectComparers(leftOperation, out _byteComparerLeft, out _longComparerLeft, out _doubleComparerLeft);
        SelectComparers(rightOperation, out _byteComparerRight, out _longComparerRight, out _doubleComparerRight);

        void SelectComparers(UnaryMatchOperation operation, 
            out delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, bool> byteComparerLeft,
            out delegate*<long, long, bool> longComparerLeft,
            out delegate*<double, double, bool> doubleComparerLeft)
        {
            switch (operation)
            {
                case UnaryMatchOperation.LessThan:
                    byteComparerLeft = &LessThanMatchComparer.Compare;
                    longComparerLeft = &LessThanMatchComparer.Compare<long>;
                    doubleComparerLeft = &LessThanMatchComparer.Compare<double>;
                    break;
                case UnaryMatchOperation.LessThanOrEqual:
                    byteComparerLeft = &LessThanOrEqualMatchComparer.Compare;
                    longComparerLeft = &LessThanOrEqualMatchComparer.Compare<long>;
                    doubleComparerLeft = &LessThanOrEqualMatchComparer.Compare<double>;
                    break;
                case UnaryMatchOperation.GreaterThan:
                    byteComparerLeft = &GreaterThanMatchComparer.Compare;
                    longComparerLeft = &GreaterThanMatchComparer.Compare<long>;
                    doubleComparerLeft = &GreaterThanMatchComparer.Compare<double>;
                    break;
                case UnaryMatchOperation.GreaterThanOrEqual:
                    byteComparerLeft = &GreaterThanOrEqualMatchComparer.Compare;
                    longComparerLeft = &GreaterThanOrEqualMatchComparer.Compare<long>;
                    doubleComparerLeft = &GreaterThanOrEqualMatchComparer.Compare<double>;
                    break;
                case UnaryMatchOperation.NotEquals:
                    byteComparerLeft = &NotEqualsMatchComparer.Compare;
                    longComparerLeft = &NotEqualsMatchComparer.Compare<long>;
                    doubleComparerLeft = &NotEqualsMatchComparer.Compare<double>;
                    break;
                case UnaryMatchOperation.Equals:
                    byteComparerLeft = &EqualsMatchComparer.Compare;
                    longComparerLeft = &EqualsMatchComparer.Compare<long>;
                    doubleComparerLeft = &EqualsMatchComparer.Compare<double>;
                    break;
                case UnaryMatchOperation.Between:
                case UnaryMatchOperation.NotBetween:
                case UnaryMatchOperation.AllIn:
                case UnaryMatchOperation.Unknown:
                default:
                    throw new Exception("Unsupported type of operation: " + operation);
            }
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

    internal sealed class GreaterThanMatchComparer
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

    internal sealed class GreaterThanOrEqualMatchComparer
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

    internal sealed class LessThanOrEqualMatchComparer
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

    internal sealed class NotEqualsMatchComparer
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
    
    internal sealed class EqualsMatchComparer
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            return sy.SequenceCompareTo(sx) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static bool Compare<T>(T sx, T sy) where T : unmanaged
        {
            if (typeof(T) == typeof(long))
                return ((long)(object)sy - (long)(object)sx) == 0;
            if (typeof(T) == typeof(double))
                return ((double)(object)sy - (double)(object)sx) == 0;

            throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
        }
    }


    internal sealed class EmptyComparer
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
    private readonly IndexSearcher _searcher;
    private TInner _inner;
    private readonly MultiUnaryItem[] _comparers;

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

    public unsafe int Fill(Span<long> matches)
    {
        var read = _inner.Fill(matches);
        if (read == 0)
            return 0;
        
        var comparerFieldsRootPages =  _comparers.Length > 128 ? new long[_comparers.Length] : stackalloc long[_comparers.Length];
        for (int i = 0; i < _comparers.Length; i++)
        {
            long fieldRoot = _searcher.FieldCache.GetLookupRootPage(_comparers[i].Binding.FieldName);
            ref var comparerFieldsRootPage = ref Unsafe.Add(ref MemoryMarshal.GetReference(comparerFieldsRootPages), i);
            comparerFieldsRootPage = fieldRoot;
        }

        Page lastPage = default;

        int currentIdx = 0;
        Span<bool> comparerMatches = _comparers.Length > 128 ? new bool[_comparers.Length] : stackalloc bool[_comparers.Length];
        Span<bool> allAccepted = _comparers.Length > 128 ? new bool[_comparers.Length] : stackalloc bool[_comparers.Length];
        allAccepted.Fill(true);
        
        for (int i = 0; i < read; ++i)
        {
            comparerMatches.Fill(false);

            int comparerId = 0;
            for (; comparerId < _comparers.Length; ++comparerId)
            {
                ref var comparerMatched = ref Unsafe.Add(ref MemoryMarshal.GetReference(comparerMatches), comparerId);
                ref var comparer = ref Unsafe.Add(ref MemoryMarshal.GetReference(_comparers.AsSpan()), comparerId);
                var reader = _searcher.GetEntryTermsReader(matches[i], ref lastPage);
                
                while (reader.MoveNext())
                {
                    if(reader.FieldRootPage != Unsafe.Add(ref MemoryMarshal.GetReference(comparerFieldsRootPages), comparerId))
                        continue;
                    if (comparerMatched) 
                        break;
                    
                    var result = IsAcceptedForIterator(comparer, in reader);
                    if (result)
                    {
                        comparerMatched = true;
                        break;
                    }
                        
                    if (_searcher.HasMultipleTermsInField(_comparers[comparerId].Binding) == false)
                        goto NotMatch;
                }
            }
            
            if (comparerMatches.SequenceCompareTo(allAccepted) == 0) // if field(s) doesn't exists that doesn't mean the document is valid for us.
                matches[currentIdx++] = matches[i];
            
            NotMatch: ; // the ; so we have a label for the goto
        }

        return currentIdx;
        
        bool IsAcceptedForIterator(MultiUnaryItem comparer, in EntryTermsReader iterator) => comparer.Type switch
        {
            MultiUnaryItem.DataType.Slice => comparer.CompareLiteral(iterator.Current.Decoded()),
            MultiUnaryItem.DataType.Long => comparer.CompareNumerical(iterator.CurrentLong),
            MultiUnaryItem.DataType.Double => comparer.CompareNumerical(iterator.CurrentDouble),
            _ => throw new ArgumentOutOfRangeException(comparer.Type.ToString())
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

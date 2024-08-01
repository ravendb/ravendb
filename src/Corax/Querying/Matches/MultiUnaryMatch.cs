using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow.Server.Utils;
using Sparrow.Server.Utils.VxSort;
using Voron;

namespace Corax.Querying.Matches;

public unsafe struct MultiUnaryItem
{
    /**
     *  MultiUnaryMatches are using same buffer from Voron to check every single condition per one document. There is no need to call UnaryMatch over and over.
     *  We decided to drop Generics here (like in rest of the code) and push comparers by pointer functions due to problems of creation complex unary queries.
     *  We've 5 different comparers (Equals are handled by TermMatch, not by scanning) so number of possible permutations grows extremely fast)
     */
    public FieldMetadata Binding;
    public readonly DataType Type;

    public string RightAsString()
    {
        return Type switch
        {
            DataType.Double => _doubleValueRight.ToString(CultureInfo.InvariantCulture),
            DataType.Long => _longValueRight.ToString(CultureInfo.InvariantCulture),
            DataType.Slice => _sliceValueRight.ToString(),
            _ => throw new ArgumentOutOfRangeException(Type.ToString())
        };
    }
    
    public string LeftAsString()
    {
        return Type switch
        {
            DataType.Double => _doubleValueLeft.ToString(CultureInfo.InvariantCulture),
            DataType.Long => _longValueLeft.ToString(CultureInfo.InvariantCulture),
            DataType.Slice => _sliceValueLeft.ToString(),
            _ => throw new ArgumentOutOfRangeException(Type.ToString())
        };
    }

    private Slice _sliceValueLeft;
    private readonly long _longValueLeft;
    private readonly double _doubleValueLeft;
    private Slice _sliceValueRight;
    private readonly long _longValueRight;
    private readonly double _doubleValueRight;
    internal readonly bool _isBetween;
    internal readonly UnaryMatchOperation _leftSideOperation;
    internal readonly UnaryMatchOperation _rightSideOperation;
    public readonly UnaryMode Mode;

    
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
    private readonly delegate*<bool,bool> _compareNullLeft, _compareNullRight;
    private readonly bool _leftIsNull, _rightIsNull;

    private MultiUnaryItem(in FieldMetadata binding, DataType dataType, bool isBetween, UnaryMatchOperation leftOperation, UnaryMatchOperation rightOperation)
    {
        Debug.Assert(binding.FieldName.HasValue);
        
        Unsafe.SkipInit(out _doubleValueLeft);
        Unsafe.SkipInit(out _longValueLeft);
        Unsafe.SkipInit(out _sliceValueLeft);
        Unsafe.SkipInit(out _doubleValueRight);
        Unsafe.SkipInit(out _longValueRight);
        Unsafe.SkipInit(out _sliceValueRight);

        Mode = leftOperation == UnaryMatchOperation.NotEquals || rightOperation == UnaryMatchOperation.NotEquals ? UnaryMode.All : UnaryMode.Any;
        
        Binding = binding;
        Type = dataType;
        _isBetween = isBetween;
        _leftSideOperation = leftOperation;
        _rightSideOperation = rightOperation;

        SelectComparers(leftOperation, out _byteComparerLeft, out _longComparerLeft, out _doubleComparerLeft, out _compareNullLeft);
        SelectComparers(rightOperation, out _byteComparerRight, out _longComparerRight, out _doubleComparerRight, out _compareNullRight);

        void SelectComparers(UnaryMatchOperation operation, 
            out delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, bool> byteComparer,
            out delegate*<long, long, bool> longComparer,
            out delegate*<double, double, bool> doubleComparer,
            out delegate*<bool, bool> compareNull)
        {
            static bool AlwaysFalse(bool _) => false;
            static bool AlwaysTrue(bool _) => true;
            
            static bool FalseUnlessNull(bool isNull) => isNull;
            static bool TrueUnlessNull(bool isNull) => isNull == false;

            static bool ThrowWhenUsed(bool isNull) =>
                throw new InvalidOperationException("We do not have between operation however tried to compare right value. This indicates a bug.");

            switch (operation)
            {
                case UnaryMatchOperation.LessThan:
                    byteComparer = &LessThanMatchComparer.Compare;
                    longComparer = &LessThanMatchComparer.Compare;
                    doubleComparer = &LessThanMatchComparer.Compare;
                    compareNull = &TrueUnlessNull;
                    break;
                case UnaryMatchOperation.LessThanOrEqual:
                    byteComparer = &LessThanOrEqualMatchComparer.Compare;
                    longComparer = &LessThanOrEqualMatchComparer.Compare;
                    doubleComparer = &LessThanOrEqualMatchComparer.Compare;
                    compareNull = &AlwaysTrue;
                    break;
                case UnaryMatchOperation.GreaterThan:
                    byteComparer = &GreaterThanMatchComparer.Compare;
                    longComparer = &GreaterThanMatchComparer.Compare;
                    doubleComparer = &GreaterThanMatchComparer.Compare;
                    compareNull = &AlwaysFalse;
                    break;
                case UnaryMatchOperation.GreaterThanOrEqual:
                    byteComparer = &GreaterThanOrEqualMatchComparer.Compare;
                    longComparer = &GreaterThanOrEqualMatchComparer.Compare;
                    doubleComparer = &GreaterThanOrEqualMatchComparer.Compare;
                    compareNull = &FalseUnlessNull;
                    break;
                case UnaryMatchOperation.NotEquals:
                    byteComparer = &NotEqualsMatchComparer.Compare;
                    longComparer = &NotEqualsMatchComparer.Compare;
                    doubleComparer = &NotEqualsMatchComparer.Compare;
                    compareNull = &TrueUnlessNull;
                    break;
                case UnaryMatchOperation.Equals:
                    byteComparer = &EqualsMatchComparer.Compare;
                    longComparer = &EqualsMatchComparer.Compare;
                    doubleComparer = &EqualsMatchComparer.Compare;
                    compareNull = &FalseUnlessNull;
                    break;
                case UnaryMatchOperation.None:
                    byteComparer = &EmptyComparer.Compare;
                    longComparer = &EmptyComparer.Compare;
                    doubleComparer = &EmptyComparer.Compare;
                    compareNull = &ThrowWhenUsed;
                    break;
                default:
                    throw new Exception("Unsupported type of operation: " + operation);
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool CompareNumerical(in EntryTermsReader it)
    {
        if (it.IsNull)
        {
            return _compareNullLeft(_leftIsNull) && 
                   (_isBetween == false || _compareNullRight(_rightIsNull));
        }
        bool leftResult;
        if (Type == DataType.Long)
        {
            leftResult = _longComparerLeft(_longValueLeft, it.CurrentLong);
            if (_isBetween)
                return leftResult & _longComparerRight(_longValueRight, it.CurrentLong);
            return leftResult;
        }

        leftResult = _doubleComparerLeft(_doubleValueLeft, it.CurrentDouble);
        if (_isBetween)
            return leftResult & _doubleComparerRight(_doubleValueRight, it.CurrentDouble);
        return leftResult;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool CompareLiteral(in EntryTermsReader it)
    {
        if (it.IsNull)
        {
            return _compareNullLeft(_leftIsNull) && 
                   (_isBetween == false || _compareNullRight(_rightIsNull));
        }
        
        ReadOnlySpan<byte> value = it.Current.Decoded();
        var leftResult = _byteComparerLeft(_sliceValueLeft.AsSpan(), value);
        return _isBetween
            ? leftResult & _byteComparerRight(_sliceValueRight.AsSpan(), value)
            : leftResult;
    }

    public MultiUnaryItem(IndexSearcher searcher, in FieldMetadata binding, string value, UnaryMatchOperation operation) : this(binding, DataType.Slice, false, operation, default)
    {
        _leftIsNull = value == null;
        _sliceValueLeft = searcher.EncodeAndApplyAnalyzer(binding, value);
    }

    public MultiUnaryItem(in FieldMetadata binding, long value, UnaryMatchOperation operation) : this(binding, DataType.Long, false, operation, default)
    {
        _longValueLeft = value;
    }

    public MultiUnaryItem(in FieldMetadata binding, double value, UnaryMatchOperation operation) : this(binding, DataType.Double, false, operation, default)
    {
        _doubleValueLeft = value;
    }

    public MultiUnaryItem(in FieldMetadata binding, Slice value, UnaryMatchOperation operation) : this(binding, DataType.Slice, false, operation, default)
    {
        _sliceValueLeft = value;
    }
    
    public MultiUnaryItem(IndexSearcher searcher, in FieldMetadata binding, string leftValue, string rightValue, UnaryMatchOperation leftOperation, UnaryMatchOperation rightOperation)
        : this(binding, DataType.Slice, true, leftOperation, rightOperation)
    {
        _rightIsNull = rightValue == null;
        _leftIsNull = leftValue == null;

        _sliceValueRight = searcher.EncodeAndApplyAnalyzer(binding, rightValue);
        _sliceValueLeft = searcher.EncodeAndApplyAnalyzer(binding, leftValue);
    }

    public MultiUnaryItem(in FieldMetadata binding, long valueLeft, long valueRight, UnaryMatchOperation leftOperation, UnaryMatchOperation rightOperation) : this(binding,
        DataType.Long, true, leftOperation, rightOperation)
    {
        _longValueLeft = valueLeft;
        _longValueRight = valueRight;
    }

    public MultiUnaryItem(in FieldMetadata binding, double valueLeft, double valueRight, UnaryMatchOperation leftOperation, UnaryMatchOperation rightOperation) : this(binding,
        DataType.Double, true, leftOperation, rightOperation)
    {
        _doubleValueLeft = valueLeft;
        _doubleValueRight = valueRight;
    }

    public enum DataType
    {
        Slice,
        Long,
        Double
    }

    private static class GreaterThanMatchComparer
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            return sy.SequenceCompareTo(sx) > 0;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Compare<T>(T sx, T sy) where T : unmanaged
        {
            if (typeof(T) == typeof(long))
                return ((long)(object)sy - (long)(object)sx) > 0;
            if (typeof(T) == typeof(double))
                return ((double)(object)sy - (double)(object)sx) > 0;

            throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
        }
    }

    private static class GreaterThanOrEqualMatchComparer
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            return sy.SequenceCompareTo(sx) >= 0;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Compare<T>(T sx, T sy) where T : unmanaged
        {
            if (typeof(T) == typeof(long))
                return ((long)(object)sy - (long)(object)sx) >= 0;
            if (typeof(T) == typeof(double))
                return ((double)(object)sy - (double)(object)sx) >= 0;

            throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
        }
    }

    private static class LessThanMatchComparer
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            return sy.SequenceCompareTo(sx) < 0;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Compare<T>(T sx, T sy) where T : unmanaged
        {
            if (typeof(T) == typeof(long))
                return ((long)(object)sy - (long)(object)sx) < 0;
            if (typeof(T) == typeof(double))
                return ((double)(object)sy - (double)(object)sx) < 0;

            throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
        }
    }

    private static class LessThanOrEqualMatchComparer
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            return sy.SequenceCompareTo(sx) <= 0;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Compare<T>(T sx, T sy) where T : unmanaged
        {
            if (typeof(T) == typeof(long))
                return ((long)(object)sy - (long)(object)sx) <= 0;
            if (typeof(T) == typeof(double))
                return ((double)(object)sy - (double)(object)sx) <= 0;

            throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
        }
    }

    private static class NotEqualsMatchComparer
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            return sy.SequenceCompareTo(sx) != 0;
        }
                
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Compare<T>(T sx, T sy) where T : unmanaged
        {
            if (typeof(T) == typeof(long))
                return ((long)(object)sy - (long)(object)sx) != 0;
            if (typeof(T) == typeof(double))
                return ((double)(object)sy - (double)(object)sx) != 0;

            throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
        }
    }
    
    private static class EqualsMatchComparer
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            return sy.SequenceCompareTo(sx) == 0;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Compare<T>(T sx, T sy) where T : unmanaged
        {
            if (typeof(T) == typeof(long))
                return ((long)(object)sy - (long)(object)sx) == 0;
            if (typeof(T) == typeof(double))
                return ((double)(object)sy - (double)(object)sx) == 0;

            throw new NotSupportedException($"MatchComparer does not support type {nameof(T)}");
        }
    }
    
    private static class EmptyComparer
    {
        private const string ErrorMessage = $"{nameof(EmptyComparer)} is only use to fill up static pointer. If you see this please report. There is a bug."; 
                
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Compare(ReadOnlySpan<byte> sx, ReadOnlySpan<byte> sy)
        {
            throw new InvalidCastException(ErrorMessage);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Compare<T>(T sx, T sy) where T : unmanaged
        {
            throw new InvalidCastException(ErrorMessage);
        }
    }
}

public struct MultiUnaryMatch<TInner> : IQueryMatch
    where TInner : IQueryMatch
{
    private long _count;
    private QueryCountConfidence _confidence;
    private readonly IndexSearcher _searcher;
    private TInner _inner;
    private readonly MultiUnaryItem[] _comparers;
    private GrowableBuffer<Progressive> _growableBuffer;
    public MultiUnaryMatch(IndexSearcher searcher, TInner inner, in MultiUnaryItem[] items)
    {
        _inner = inner;
        _searcher = searcher;
        _comparers = items;
        _count = _inner.Count;
        _confidence = QueryCountConfidence.Low;
        IsBoosting = false;
    }

    public long Count => _count;
    
    public SkipSortingResult AttemptToSkipSorting()
    {
        return _inner.AttemptToSkipSorting();
    }

    public QueryCountConfidence Confidence => _confidence;
    public bool IsBoosting { get; }

    public unsafe int Fill(Span<long> matches)
    {
        var workingBuffer = matches;
        var matchesIdx = 0;
        ref var inner = ref _inner;
        var read = inner.Fill(workingBuffer);
        
        //When inner is empty we're done.
        if (read == 0)
            return 0;
        
        Page lastPage = default;
        Span<long> comparerFieldsRootPages = _comparers.Length > 128 
            ? new long[_comparers.Length] 
            : stackalloc long[_comparers.Length];
        
        for (int i = 0; i < _comparers.Length; i++)
        {
            long fieldRoot = _searcher.FieldCache.GetLookupRootPage(_comparers[i].Binding.FieldName);
            ref var comparerFieldsRootPage = ref Unsafe.Add(ref MemoryMarshal.GetReference(comparerFieldsRootPages), i);
            comparerFieldsRootPage = fieldRoot;
        }

        using var _ = _searcher.Transaction.LowLevelTransaction.AcquireCompactKey(out var existingKey);

        do
        {
            int currentMatchesIdx = 0;
            for (int docIdx = 0; docIdx < read; ++docIdx)
            {
                _searcher.GetEntryTermsReader(workingBuffer[docIdx], ref lastPage, out var reader, existingKey);
                
                var documentMatched = true;
                
                for (int comparerId = 0; comparerId < _comparers.Length; ++comparerId)
                {
                    ref var comparer = ref Unsafe.Add(ref MemoryMarshal.GetReference(_comparers.AsSpan()), comparerId);
                    ref var currentFieldRootPage = ref Unsafe.Add(ref MemoryMarshal.GetReference(comparerFieldsRootPages), comparerId);
                    
                    // We have two modes of UnaryMatch
                    // Any: that means if any field's term matches our comparer, we've got a match. In this scenario we look for the first matching item.
                    // All: all terms have been matched by our comparer. In this case we'll search for the first non-matching element.
                    var isAccepted = MultiUnaryItem.UnaryMode.All == comparer.Mode;
                    reader.Reset();
                    while (reader.FindNext(currentFieldRootPage))
                    {
                        var cmpResult = comparer.Type switch
                        {
                            MultiUnaryItem.DataType.Slice => comparer.CompareLiteral(reader),
                            MultiUnaryItem.DataType.Long => comparer.CompareNumerical(reader), 
                            MultiUnaryItem.DataType.Double => comparer.CompareNumerical(reader),
                            _ => throw new ArgumentOutOfRangeException(comparer.Type.ToString())
                        };
                        
                        if (comparer.Mode == MultiUnaryItem.UnaryMode.All && cmpResult == false)
                        {
                            isAccepted = false;
                            break;
                        }

                        // Fir
                        if (comparer.Mode == MultiUnaryItem.UnaryMode.Any && cmpResult)
                        {
                            isAccepted = true;
                            break;
                        }
                    }

                    documentMatched &= isAccepted;
                }

                if (documentMatched) // if field(s) doesn't exists that doesn't mean the document is valid for us.
                {
                    currentMatchesIdx++;
                    matches[matchesIdx++] = workingBuffer[docIdx];
                }
            }
            
            _count += currentMatchesIdx;
            workingBuffer = workingBuffer.Slice(currentMatchesIdx);

            if (workingBuffer.Length == 0)
                return matchesIdx;
            
            read = inner.Fill(workingBuffer);
        } 
        while (read > 0);

        if (read == 0)
            _confidence = QueryCountConfidence.High;
        
        return matchesIdx;
    }
    
    public int AndWith(Span<long> buffer, int matches)
    {
        ref var matchBuffer = ref _growableBuffer;
        if (matchBuffer.IsInitialized == false)
        {
            matchBuffer.Init(_searcher.Allocator, _inner.Count);
            while(Fill(matchBuffer.GetSpace()) is var read and > 0)
                matchBuffer.AddUsage(read);
            
            // If results are not natively sorted we do not have any guarantees about order. We've to sort it to perform AND.
            if (_inner.AttemptToSkipSorting() != SkipSortingResult.ResultsNativelySorted)
                Sort.Run(matchBuffer.Results);

            _confidence = QueryCountConfidence.High;
            _count = matchBuffer.Count;
        }
        
        return MergeHelper.And(buffer, buffer.Slice(0, matches), matchBuffer.Results);
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (_inner.IsBoosting)
            _inner.Score(matches, scores, boostFactor);
    }

    public QueryInspectionNode Inspect()
    {
        var parameters = new Dictionary<string, string>()
        {
            {Constants.QueryInspectionNode.IsBoosting, IsBoosting.ToString()},
            {Constants.QueryInspectionNode.Count, _inner.Count.ToString()},
            {Constants.QueryInspectionNode.CountConfidence, QueryCountConfidence.Low.ToString()},
        };

        for (int index = 0; index < _comparers.Length; index++)
        {
            MultiUnaryItem comparer = _comparers[index];
            var prefix = Constants.QueryInspectionNode.Comparer + index + "_";
            parameters.Add(prefix + Constants.QueryInspectionNode.FieldType, comparer.Type.ToString());
            parameters.Add(prefix + Constants.QueryInspectionNode.Operation + (comparer._isBetween ? "Left" : string.Empty), comparer._leftSideOperation.ToString());
            parameters.Add(prefix + (comparer._isBetween ? Constants.QueryInspectionNode.Term : Constants.QueryInspectionNode.LowValue), comparer.LeftAsString());
            
            if (comparer._isBetween)
            {
                parameters.Add(prefix + Constants.QueryInspectionNode.HighValue, comparer.RightAsString());
                parameters.Add(prefix + Constants.QueryInspectionNode.Operation  + (comparer._isBetween ? "Right" : string.Empty), comparer._rightSideOperation.ToString());
            }
        }

        return new QueryInspectionNode($"{nameof(MultiUnaryMatch<TInner>)}",
            children: new List<QueryInspectionNode> { _inner.Inspect() },
            parameters: parameters);
    }
}

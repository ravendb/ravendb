using System;
using System.Runtime.CompilerServices;
using Voron;

namespace Corax.Queries;

public unsafe struct MultiUnaryItem
{
    public int FieldId;
    public DataType Type;
    private Slice SliceValue;
    private long LongValue;
    private double DoubleValue;

    private readonly delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, bool> _byteComparer;
    private readonly delegate*<long, long, bool> _longComparer;
    private readonly delegate*<double, double, bool> _doubleComparer;

    private MultiUnaryItem(int fieldId, DataType dataType, UnaryMatchOperation operation)
    {
        Unsafe.SkipInit(out DoubleValue);
        Unsafe.SkipInit(out LongValue);
        Unsafe.SkipInit(out SliceValue);


        FieldId = fieldId;
        Type = dataType;
        switch (operation)
        {
            case UnaryMatchOperation.LessThan:
                _byteComparer = &LessThanMatchComparer.Compare;
                _longComparer = &LessThanMatchComparer.Compare<long>;
                _doubleComparer = &LessThanMatchComparer.Compare<double>;
                break;
            case UnaryMatchOperation.LessThanOrEqual:
                _byteComparer = &LessThanOrEqualMatchComparer.Compare;
                _longComparer = &LessThanOrEqualMatchComparer.Compare<long>;
                _doubleComparer = &LessThanOrEqualMatchComparer.Compare<double>;
                break;
            case UnaryMatchOperation.GreaterThan:
                _byteComparer = &GreaterThanMatchComparer.Compare;
                _longComparer = &GreaterThanMatchComparer.Compare<long>;
                _doubleComparer = &GreaterThanMatchComparer.Compare<double>;
                break;
            case UnaryMatchOperation.GreaterThanOrEqual:
                _byteComparer = &GreaterThanOrEqualMatchComparer.Compare;
                _longComparer = &GreaterThanOrEqualMatchComparer.Compare<long>;
                _doubleComparer = &GreaterThanOrEqualMatchComparer.Compare<double>;
                break;
            default:
                throw new Exception("Unsupported type of operation");
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool CompareNumerical<T>(T value)
    where T : unmanaged
    {
        if (Type == DataType.Long)
        {
            return _longComparer(LongValue, CoherseValueTypeToLong(value));
        }

        return _doubleComparer(DoubleValue, CoherseValueTypeToDouble(value));
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    public bool CompareLiteral(ReadOnlySpan<byte> value)
    {
        return _byteComparer(SliceValue.AsSpan(), value);
    }

    public MultiUnaryItem(int fieldId, Slice value, UnaryMatchOperation operation) : this(fieldId, DataType.Slice, operation)
    {
        //TODO add analyzer layer
        SliceValue = value;
    }

    public MultiUnaryItem(int fieldId, long value, UnaryMatchOperation operation) : this(fieldId, DataType.Long, operation)
    {
        LongValue = value;
    }

    public MultiUnaryItem(int fieldId, double value, UnaryMatchOperation operation) : this(fieldId, DataType.Double, operation)
    {
        DoubleValue = value;
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
    public QueryCountConfidence Confidence => QueryCountConfidence.Low;
    public bool IsBoosting { get; }

    public int Fill(Span<long> matches)
    {
        var read = _inner.Fill(matches);
        if (read == 0)
            return 0;

        int currentIdx = 0;
        for (int i = 0; i < read; ++i)
        {
            var reader = _searcher.GetReaderFor(matches[i]);
            int comparerId = 0;
            for (; comparerId < _comparers.Length; ++comparerId)
            {
                var comparer = _comparers[comparerId];
                var fieldType = reader.GetFieldType(comparer.FieldId, out _);
                bool isAccepted;
                switch (fieldType)
                {
                    case IndexEntryFieldType.Empty:
                    case IndexEntryFieldType.Null:
                        var fieldName = fieldType == IndexEntryFieldType.Null ? Constants.NullValueSlice : Constants.EmptyStringSlice;
                        if (comparer.Type != MultiUnaryItem.DataType.Slice || comparer.CompareLiteral(fieldName.AsReadOnlySpan()) == false)
                            goto NotMatch;
                            
                        break;
                    
                    case IndexEntryFieldType.TupleList:
                        if (reader.TryReadMany(comparer.FieldId, out var iterator) == false)
                            break;

                        while (iterator.ReadNext())
                        {
                            isAccepted = IsAcceptedForIterator(comparer, in iterator);
                            if (isAccepted == false)
                                goto NotMatch;
                        }

                        break;

                    case IndexEntryFieldType.Tuple:
                        if (reader.Read(comparer.FieldId, out _, out long lVal, out double dVal, out Span<byte> valueInEntry) == false)
                            break;

                        isAccepted = IsAcceptedItem(comparer, valueInEntry, in lVal, in dVal);
                        if (isAccepted == false)
                            goto NotMatch;
                        
                        break;
                    
                    case IndexEntryFieldType.SpatialPoint:
                    case IndexEntryFieldType.SpatialPointList:
                        throw new ArgumentException($"Spatial is not supported inside {nameof(MultiTermMatch)}");

                    case IndexEntryFieldType.TupleListWithNulls:
                    case IndexEntryFieldType.ListWithNulls:
                    case IndexEntryFieldType.List:
                        if (reader.TryReadMany(comparer.FieldId, out iterator) == false)
                            break;

                        while (iterator.ReadNext())
                        {
                            if ((fieldType & IndexEntryFieldType.HasNulls) != 0 && (iterator.IsEmpty || iterator.IsNull))
                            {
                                var fieldValue = iterator.IsNull ? Constants.NullValueSlice : Constants.EmptyStringSlice;
                                if (comparer.Type != MultiUnaryItem.DataType.Slice || comparer.CompareLiteral(fieldValue.AsReadOnlySpan()) == false)
                                    goto NotMatch;
                            }
                            else if ((fieldType & IndexEntryFieldType.Tuple) != 0)
                            {
                                isAccepted = IsAcceptedForIterator(comparer, in iterator);
                                
                                if (isAccepted == false)
                                    goto NotMatch;
                            }
                            else
                            {
                                if (comparer.Type != MultiUnaryItem.DataType.Slice || comparer.CompareLiteral(iterator.Sequence) == false)
                                    goto NotMatch;
                            }
                        }

                        break;
                    case IndexEntryFieldType.Raw:
                    case IndexEntryFieldType.RawList:
                    case IndexEntryFieldType.Invalid:
                        break;
                    default:
                        if (reader.Read(comparer.FieldId, out var value) == false)
                            break;

                        if (comparer.Type == MultiUnaryItem.DataType.Slice)
                        {
                            if (comparer.CompareLiteral(value) == false)
                                goto NotMatch;
                            
                            continue;
                        }
                        
                        goto NotMatch;
                }
            }
            
            NotMatch:
            if (comparerId == _comparers.Length)
            {
                matches[currentIdx++] = matches[i];
                // Its a match
            }
        }

        return currentIdx;
        
        
        bool IsAcceptedItem(MultiUnaryItem comparer, ReadOnlySpan<byte> sequence, in long longValue, in double doubleValue) => comparer.Type switch
        {
            MultiUnaryItem.DataType.Slice => comparer.CompareLiteral(sequence),
            MultiUnaryItem.DataType.Long => comparer.CompareNumerical(longValue),
            _ => comparer.CompareNumerical(doubleValue)
        };

        bool IsAcceptedForIterator(MultiUnaryItem comparer, in IndexEntryFieldIterator iterator) => comparer.Type switch
        {
            MultiUnaryItem.DataType.Slice => comparer.CompareLiteral(iterator.Sequence),
            MultiUnaryItem.DataType.Long => comparer.CompareNumerical(iterator.Long),
            _ => comparer.CompareNumerical(iterator.Double)
        };
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        //This should never be called.
        throw new NotSupportedException();
    }

    public void Score(Span<long> matches, Span<float> scores)
    {
        throw new NotImplementedException();
    }

    public QueryInspectionNode Inspect()
    {
        throw new NotImplementedException();
    }
}

using System;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Queries;
using Voron;
using Range = Corax.Queries.Range;

namespace Corax;

public partial class IndexSearcher
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch BetweenQuery<TValue>(FieldMetadata field, TValue low, TValue high, UnaryMatchOperation leftSide = UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation rightSide = UnaryMatchOperation.LessThanOrEqual, bool isNegated = false) {
        if (typeof(TValue) == typeof(long))
        {
            return (leftSide, rightSide) switch
            {
                // (x, y)
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) => RangeBuilder<Range.Exclusive, Range.Exclusive>(field, (long)(object)low, (long)(object)high, isNegated),

                //<x, y)
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => RangeBuilder<Range.Inclusive, Range.Exclusive>(field, (long)(object)low, (long)(object)high, isNegated),

                //<x, y>
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<Range.Inclusive, Range.Inclusive>(field, (long)(object)low, (long)(object)high, isNegated),

                //(x, y>
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<Range.Exclusive, Range.Inclusive>(field, (long)(object)low, (long)(object)high, isNegated),
                _ => throw new ArgumentOutOfRangeException($"Unknown operation at {nameof(BetweenQuery)}.")
            };
        }

        if (typeof(TValue) == typeof(double))
        {
            return (leftSide, rightSide) switch
            {
                // (x, y)
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) => RangeBuilder<Range.Exclusive, Range.Exclusive>(field, (double)(object)low, (double)(object)high, isNegated),

                //<x, y)
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => RangeBuilder<Range.Inclusive, Range.Exclusive>(field, (double)(object)low, (double)(object)high, isNegated),

                //<x, y>
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<Range.Inclusive, Range.Inclusive>(field, (double)(object)low, (double)(object)high, isNegated),

                //(x, y>
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<Range.Exclusive, Range.Inclusive>(field, (double)(object)low, (double)(object)high, isNegated),
                _ => throw new ArgumentOutOfRangeException($"Unknown operation at {nameof(BetweenQuery)}.")

            };
        }

        if (typeof(TValue) == typeof(string))
        {
            var leftValue = EncodeAndApplyAnalyzer(field, (string)(object)low);
            var rightValue = EncodeAndApplyAnalyzer(field, (string)(object)high);

            return (leftSide, rightSide) switch
            {
                // (x, y)
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) => RangeBuilder<Range.Exclusive, Range.Exclusive>(field,
                    leftValue, rightValue, isNegated),

                //<x, y)
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => RangeBuilder<Range.Inclusive, Range.Exclusive>(field,
                    leftValue, rightValue, isNegated),

                //<x, y>
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<Range.Inclusive, Range.Inclusive>(
                    field, leftValue, rightValue, isNegated),

                //(x, y>
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<Range.Exclusive, Range.Inclusive>(field,
                    leftValue, rightValue, isNegated),

                _ => throw new ArgumentOutOfRangeException($"Unknown operation at {nameof(BetweenQuery)}.")
            };
        }

        throw new ArgumentException($"{typeof(TValue)} is not supported in {nameof(BetweenQuery)}");
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch GreaterThanQuery<TValue>(FieldMetadata field, TValue value, bool isNegated = false)
    {
        return GreatBuilder<Range.Exclusive, Range.Inclusive, TValue>(field, value, isNegated);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch GreatThanOrEqualsQuery<TValue>(FieldMetadata field, TValue value, bool isNegated = false)

    {
        return GreatBuilder<Range.Inclusive, Range.Inclusive, TValue>(field, value, isNegated);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private MultiTermMatch GreatBuilder<TLeftRange, TRightRange, TValue>(FieldMetadata field, TValue value, bool isNegated = false)
        where TLeftRange : struct, Range.Marker
        where TRightRange : struct, Range.Marker
    {
        
        if (typeof(TValue) == typeof(long))
        {
            
            return RangeBuilder<TLeftRange, TRightRange>(field, (long)(object)value, long.MaxValue, isNegated);
        }

        if (typeof(TValue) == typeof(double))
            return RangeBuilder<TLeftRange, TRightRange>(field, (double)(object)value, double.MaxValue, isNegated);
        if (typeof(TValue) == typeof(string))
        {
            var sliceValue = EncodeAndApplyAnalyzer(field, (string)(object)value);
            return RangeBuilder<TLeftRange, TRightRange>(field, sliceValue, Slices.AfterAllKeys,  isNegated);
        }

        throw new ArgumentException("Range queries are supporting strings, longs or doubles only");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch LessThanOrEqualsQuery<TValue>(FieldMetadata field, TValue value, bool isNegated = false)
        => LessBuilder<Range.Inclusive, Range.Inclusive, TValue>(field, value, isNegated);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch LessThanQuery<TValue>(FieldMetadata field, TValue value, bool isNegated = false)
        => LessBuilder<Range.Inclusive, Range.Exclusive, TValue>(field, value, isNegated);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private MultiTermMatch LessBuilder<TLeftRange, TRightRange, TValue>(FieldMetadata field, TValue value,
        bool isNegated = false)
        where TLeftRange : struct, Range.Marker
        where TRightRange : struct, Range.Marker
    {
        if (typeof(TValue) == typeof(long))
            return RangeBuilder<TLeftRange, TRightRange>(field, long.MinValue, (long)(object)value, isNegated);

        if (typeof(TValue) == typeof(double))
            return RangeBuilder<TLeftRange, TRightRange>(field, double.MinValue, (double)(object)value, isNegated);
        
        if (typeof(TValue) == typeof(string))
        {
            var sliceValue = EncodeAndApplyAnalyzer(field, (string)(object)value);
            return RangeBuilder<TLeftRange, TRightRange>(field, Slices.BeforeAllKeys, sliceValue, isNegated);
        }

        throw new ArgumentException("Range queries are supporting strings, longs or doubles only");
    }
    
    private MultiTermMatch RangeBuilder<TLow, THigh>(FieldMetadata field, Slice low, Slice high, bool isNegated)
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        return MultiTermMatch.Create(new MultiTermMatch<TermRangeProvider<TLow, THigh>>(field, _transaction.Allocator, new TermRangeProvider<TLow, THigh>(this, terms, field, low, high)));
    }

    private MultiTermMatch RangeBuilder<TLow, THigh>(FieldMetadata field, Slice fieldLong, long low, long high, bool isNegated)
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        var set = _fieldsTree?.FixedTreeFor(fieldLong, sizeof(long));

        return MultiTermMatch.Create(new MultiTermMatch<TermNumericRangeProvider<TLow, THigh, long>>(field, _transaction.Allocator, new TermNumericRangeProvider<TLow, THigh, long>(this, set, terms, field, low, high)));
    }

    private MultiTermMatch RangeBuilder<TLow, THigh>(FieldMetadata field, double low, double high, bool isNegated)
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        field = field.GetNumericFieldMetadata<double>(Allocator);
        var set = _fieldsTree?.FixedTreeForDouble(field.FieldName, sizeof(long));
            
        return MultiTermMatch.Create(new MultiTermMatch<TermNumericRangeProvider<TLow, THigh, double>>(field, _transaction.Allocator, new TermNumericRangeProvider<TLow, THigh, double>(this, set, terms, field, low, high)));
    }
}

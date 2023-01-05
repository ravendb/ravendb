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
    public MultiTermMatch BetweenQuery<TValue, TScoreFunction>(FieldMetadata field, TValue low, TValue high,
        TScoreFunction scoreFunction = default, UnaryMatchOperation leftSide = UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation rightSide = UnaryMatchOperation.LessThanOrEqual, bool isNegated = false)
        where TScoreFunction : IQueryScoreFunction
    {
        if (typeof(TValue) == typeof(long))
        {
            return (leftSide, rightSide) switch
            {
                // (x, y)
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) => RangeBuilder<TScoreFunction, Range.Exclusive, Range.Exclusive>(field, (long)(object)low, (long)(object)high, scoreFunction, isNegated),

                //<x, y)
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => RangeBuilder<TScoreFunction, Range.Inclusive, Range.Exclusive>(field, (long)(object)low, (long)(object)high, scoreFunction, isNegated),

                //<x, y>
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<TScoreFunction, Range.Inclusive, Range.Inclusive>(field, (long)(object)low, (long)(object)high, scoreFunction, isNegated),

                //(x, y>
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<TScoreFunction, Range.Exclusive, Range.Inclusive>(field, (long)(object)low, (long)(object)high, scoreFunction, isNegated),
                _ => throw new ArgumentOutOfRangeException($"Unknown operation at {nameof(BetweenQuery)}.")
            };
        }

        if (typeof(TValue) == typeof(double))
        {
            return (leftSide, rightSide) switch
            {
                // (x, y)
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) => RangeBuilder<TScoreFunction, Range.Exclusive, Range.Exclusive>(field, (double)(object)low, (double)(object)high, scoreFunction, isNegated),

                //<x, y)
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => RangeBuilder<TScoreFunction, Range.Inclusive, Range.Exclusive>(field, (double)(object)low, (double)(object)high, scoreFunction, isNegated),

                //<x, y>
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<TScoreFunction, Range.Inclusive, Range.Inclusive>(field, (double)(object)low, (double)(object)high, scoreFunction, isNegated),

                //(x, y>
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<TScoreFunction, Range.Exclusive, Range.Inclusive>(field, (double)(object)low, (double)(object)high, scoreFunction, isNegated),
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
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) => RangeBuilder<TScoreFunction, Range.Exclusive, Range.Exclusive>(field,
                    leftValue, rightValue, scoreFunction, isNegated),

                //<x, y)
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => RangeBuilder<TScoreFunction, Range.Inclusive, Range.Exclusive>(field,
                    leftValue, rightValue, scoreFunction, isNegated),

                //<x, y>
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<TScoreFunction, Range.Inclusive, Range.Inclusive>(
                    field, leftValue, rightValue, scoreFunction, isNegated),

                //(x, y>
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<TScoreFunction, Range.Exclusive, Range.Inclusive>(field,
                    leftValue, rightValue, scoreFunction, isNegated),

                _ => throw new ArgumentOutOfRangeException($"Unknown operation at {nameof(BetweenQuery)}.")
            };
        }

        throw new ArgumentException($"{typeof(TValue)} is not supported in {nameof(BetweenQuery)}");
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch GreaterThanQuery<TValue, TScoreFunction>(FieldMetadata field, TValue value, TScoreFunction scoreFunction = default, bool isNegated = false,
        int fieldId = Constants.IndexSearcher.NonAnalyzer)
        where TScoreFunction : IQueryScoreFunction
    {
        return GreatBuilder<Range.Exclusive, Range.Inclusive, TValue, TScoreFunction>(field, value, scoreFunction, isNegated);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch GreatThanOrEqualsQuery<TValue, TScoreFunction>(FieldMetadata field, TValue value, TScoreFunction scoreFunction = default, bool isNegated = false)
        where TScoreFunction : IQueryScoreFunction
    {
        return GreatBuilder<Range.Inclusive, Range.Inclusive, TValue, TScoreFunction>(field, value, scoreFunction, isNegated);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private MultiTermMatch GreatBuilder<TLeftRange, TRightRange, TValue, TScoreFunction>(FieldMetadata field, TValue value, TScoreFunction scoreFunction = default,
        bool isNegated = false)
        where TScoreFunction : IQueryScoreFunction
        where TLeftRange : struct, Range.Marker
        where TRightRange : struct, Range.Marker
    {
        
        if (typeof(TValue) == typeof(long))
        {
            
            return RangeBuilder<TScoreFunction, TLeftRange, TRightRange>(field, (long)(object)value, long.MaxValue, scoreFunction,
                isNegated);
        }

        if (typeof(TValue) == typeof(double))
            return RangeBuilder<TScoreFunction, TLeftRange, TRightRange>(field, (double)(object)value, double.MaxValue, scoreFunction,
                isNegated);
        if (typeof(TValue) == typeof(string))
        {
            var sliceValue = EncodeAndApplyAnalyzer(field, (string)(object)value);
            return RangeBuilder<TScoreFunction, TLeftRange, TRightRange>(field, sliceValue, Slices.AfterAllKeys, scoreFunction, isNegated);
        }

        throw new ArgumentException("Range queries are supporting strings, longs or doubles only");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch LessThanOrEqualsQuery<TValue, TScoreFunction>(FieldMetadata field, TValue value, TScoreFunction scoreFunction = default, bool isNegated = false) where TScoreFunction : IQueryScoreFunction 
        => LessBuilder<Range.Inclusive, Range.Inclusive, TValue, TScoreFunction>(field, value, scoreFunction, isNegated);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch LessThanQuery<TValue, TScoreFunction>(FieldMetadata field, TValue value, TScoreFunction scoreFunction = default, bool isNegated = false) where TScoreFunction : IQueryScoreFunction
        => LessBuilder<Range.Inclusive, Range.Exclusive, TValue, TScoreFunction>(field, value, scoreFunction, isNegated);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private MultiTermMatch LessBuilder<TLeftRange, TRightRange, TValue, TScoreFunction>(FieldMetadata field, TValue value, TScoreFunction scoreFunction = default,
        bool isNegated = false)
        where TScoreFunction : IQueryScoreFunction
        where TLeftRange : struct, Range.Marker
        where TRightRange : struct, Range.Marker
    {
        if (typeof(TValue) == typeof(long))
            return RangeBuilder<TScoreFunction, TLeftRange, TRightRange>(field, long.MinValue, (long)(object)value, scoreFunction, isNegated);

        if (typeof(TValue) == typeof(double))
            return RangeBuilder<TScoreFunction, TLeftRange, TRightRange>(field, double.MinValue, (double)(object)value, scoreFunction, isNegated);
        
        if (typeof(TValue) == typeof(string))
        {
            var sliceValue = EncodeAndApplyAnalyzer(field, (string)(object)value);
            return RangeBuilder<TScoreFunction, TLeftRange, TRightRange>(field, Slices.BeforeAllKeys, sliceValue, scoreFunction, isNegated);
        }

        throw new ArgumentException("Range queries are supporting strings, longs or doubles only");
    }
    
    private MultiTermMatch RangeBuilder<TScoreFunction, TLow, THigh>(FieldMetadata field, Slice low, Slice high, TScoreFunction scoreFunction, bool isNegated)
        where TScoreFunction : IQueryScoreFunction
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        return MultiTermMatch.Create(new MultiTermMatch<TermRangeProvider<TLow, THigh>>(field, _transaction.Allocator, new TermRangeProvider<TLow, THigh>(this, terms, field, low, high)));
    }

    private MultiTermMatch RangeBuilder<TScoreFunction, TLow, THigh>(FieldMetadata field, Slice fieldLong, long low, long high, TScoreFunction scoreFunction, bool isNegated)
        where TScoreFunction : IQueryScoreFunction
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        var set = _fieldsTree?.FixedTreeFor(fieldLong, sizeof(long));

        return MultiTermMatch.Create(new MultiTermMatch<TermNumericRangeProvider<TLow, THigh, long>>(field, _transaction.Allocator, new TermNumericRangeProvider<TLow, THigh, long>(this, set, terms, field, low, high)));
    }

    private MultiTermMatch RangeBuilder<TScoreFunction, TLow, THigh>(FieldMetadata field, double low, double high, TScoreFunction scoreFunction, bool isNegated)
        where TScoreFunction : IQueryScoreFunction
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

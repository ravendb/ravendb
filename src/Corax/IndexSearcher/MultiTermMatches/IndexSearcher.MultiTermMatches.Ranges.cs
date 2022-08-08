using System;
using System.Runtime.CompilerServices;
using Corax.Queries;
using Voron;
using Range = Corax.Queries.Range;

namespace Corax;

public partial class IndexSearcher
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch BetweenQuery<TValue, TScoreFunction>(string field, TValue low, TValue high,
        TScoreFunction scoreFunction = default, UnaryMatchOperation leftSide = UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation rightSide = UnaryMatchOperation.LessThanOrEqual, bool isNegated = false,
        int fieldId = Constants.IndexSearcher.NonAnalyzer)
        where TScoreFunction : IQueryScoreFunction
    {
        var names = GetSliceForRangeQueries(field, low);
        if (typeof(TValue) == typeof(long))
        {
            return (leftSide, rightSide) switch
            {
                // (x, y)
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) => RangeBuilder<TScoreFunction, Range.Exclusive, Range.Exclusive>(names.FieldName,
                    names.NumericTree, (long)(object)low, (long)(object)high, scoreFunction, isNegated),

                //<x, y)
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => RangeBuilder<TScoreFunction, Range.Inclusive, Range.Exclusive>(names.FieldName,
                    names.NumericTree, (long)(object)low, (long)(object)high, scoreFunction, isNegated),

                //<x, y>
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<TScoreFunction, Range.Inclusive, Range.Inclusive>(
                    names.FieldName, names.NumericTree, (long)(object)low, (long)(object)high, scoreFunction, isNegated),

                //(x, y>
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<TScoreFunction, Range.Exclusive, Range.Inclusive>(names.FieldName,
                    names.NumericTree, (long)(object)low, (long)(object)high, scoreFunction, isNegated),
            };
        }

        if (typeof(TValue) == typeof(double))
        {
            return (leftSide, rightSide) switch
            {
                // (x, y)
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) => RangeBuilder<TScoreFunction, Range.Exclusive, Range.Exclusive>(names.FieldName,
                    names.NumericTree, (double)(object)low, (double)(object)high, scoreFunction, isNegated),

                //<x, y)
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => RangeBuilder<TScoreFunction, Range.Inclusive, Range.Exclusive>(names.FieldName,
                    names.NumericTree, (double)(object)low, (double)(object)high, scoreFunction, isNegated),

                //<x, y>
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<TScoreFunction, Range.Inclusive, Range.Inclusive>(
                    names.FieldName, names.NumericTree, (double)(object)low, (double)(object)high, scoreFunction, isNegated),

                //(x, y>
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<TScoreFunction, Range.Exclusive, Range.Inclusive>(names.FieldName,
                    names.NumericTree, (double)(object)low, (double)(object)high, scoreFunction, isNegated),
            };
        }

        if (typeof(TValue) == typeof(string))
        {
            var leftValue = EncodeAndApplyAnalyzer((string)(object)low, fieldId);
            var rightValue = EncodeAndApplyAnalyzer((string)(object)high, fieldId);

            return (leftSide, rightSide) switch
            {
                // (x, y)
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) => RangeBuilder<TScoreFunction, Range.Exclusive, Range.Exclusive>(names.FieldName,
                    leftValue, rightValue, scoreFunction, isNegated),

                //<x, y)
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => RangeBuilder<TScoreFunction, Range.Inclusive, Range.Exclusive>(names.FieldName,
                    leftValue, rightValue, scoreFunction, isNegated),

                //<x, y>
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<TScoreFunction, Range.Inclusive, Range.Inclusive>(
                    names.FieldName, leftValue, rightValue, scoreFunction, isNegated),

                //(x, y>
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<TScoreFunction, Range.Exclusive, Range.Inclusive>(names.FieldName,
                    leftValue, rightValue, scoreFunction, isNegated),
            };
        }

        throw new ArgumentException($"{typeof(TValue)} is not supported in {nameof(BetweenQuery)}");
    }
    //
    // [MethodImpl(MethodImplOptions.AggressiveInlining)]
    // public MultiTermMatch CreateRangeQuery<TValue, TScoreFunction>(string field, TValue value, TScoreFunction scoreFunction = default, bool isNegated = false,
    //     int fieldId = Constants.IndexSearcher.NonAnalyzer)         where TScoreFunction : IQueryScoreFunction
    //
    // {
    //     MultiTermMatch.Create(
    //         MultiTermBoostingMatch<EndsWithTermProvider>.Create(
    //             this, new EndsWithTermProvider(this, _transaction.Allocator, terms, fieldName, fieldId, slicedTerm), scoreFunction))
    // }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch GreaterThanQuery<TValue, TScoreFunction>(string field, TValue value, TScoreFunction scoreFunction = default, bool isNegated = false,
        int fieldId = Constants.IndexSearcher.NonAnalyzer)
        where TScoreFunction : IQueryScoreFunction
    {
        return GreatBuilder<Range.Exclusive, Range.Inclusive, TValue, TScoreFunction>(field, value, scoreFunction, isNegated, fieldId);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch GreatThanOrEqualsQuery<TValue, TScoreFunction>(string field, TValue value, TScoreFunction scoreFunction = default, bool isNegated = false,
        int fieldId = Constants.IndexSearcher.NonAnalyzer)
        where TScoreFunction : IQueryScoreFunction
    {
        return GreatBuilder<Range.Inclusive, Range.Inclusive, TValue, TScoreFunction>(field, value, scoreFunction, isNegated, fieldId);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private MultiTermMatch GreatBuilder<TLeftRange, TRightRange, TValue, TScoreFunction>(string field, TValue value, TScoreFunction scoreFunction = default,
        bool isNegated = false, int fieldId = Constants.IndexSearcher.NonAnalyzer)
        where TScoreFunction : IQueryScoreFunction
        where TLeftRange : struct, Range.Marker
        where TRightRange : struct, Range.Marker
    {
        var names = GetSliceForRangeQueries(field, value);

        if (typeof(TValue) == typeof(long))
            return RangeBuilder<TScoreFunction, TLeftRange, TRightRange>(names.FieldName, names.NumericTree, (long)(object)value, long.MaxValue, scoreFunction,
                isNegated);

        if (typeof(TValue) == typeof(double))
            return RangeBuilder<TScoreFunction, TLeftRange, TRightRange>(names.FieldName, names.NumericTree, (double)(object)value, double.MaxValue, scoreFunction,
                isNegated);
        if (typeof(TValue) == typeof(string))
        {
            var sliceValue = EncodeAndApplyAnalyzer((string)(object)value, fieldId);
            return RangeBuilder<TScoreFunction, TLeftRange, TRightRange>(names.FieldName, sliceValue, Slices.AfterAllKeys, scoreFunction, isNegated);
        }

        throw new ArgumentException("Range queries are supporting strings, longs or doubles only");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch LessThanOrEqualsQuery<TValue, TScoreFunction>(string field, TValue value, TScoreFunction scoreFunction = default, bool isNegated = false, int fieldId = Constants.IndexSearcher.NonAnalyzer) where TScoreFunction : IQueryScoreFunction 
        => LessBuilder<Range.Inclusive, Range.Inclusive, TValue, TScoreFunction>(field, value, scoreFunction, isNegated, fieldId);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch LessThanQuery<TValue, TScoreFunction>(string field, TValue value, TScoreFunction scoreFunction = default, bool isNegated = false,
        int fieldId = Constants.IndexSearcher.NonAnalyzer) where TScoreFunction : IQueryScoreFunction
        => LessBuilder<Range.Inclusive, Range.Exclusive, TValue, TScoreFunction>(field, value, scoreFunction, isNegated, fieldId);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private MultiTermMatch LessBuilder<TLeftRange, TRightRange, TValue, TScoreFunction>(string field, TValue value, TScoreFunction scoreFunction = default,
        bool isNegated = false, int fieldId = Constants.IndexSearcher.NonAnalyzer)
        where TScoreFunction : IQueryScoreFunction
        where TLeftRange : struct, Range.Marker
        where TRightRange : struct, Range.Marker
    {
        var names = GetSliceForRangeQueries(field, value);

        if (typeof(TValue) == typeof(long))
            return RangeBuilder<TScoreFunction, TLeftRange, TRightRange>(names.FieldName, names.NumericTree, long.MinValue, (long)(object)value, scoreFunction,
                isNegated);

        if (typeof(TValue) == typeof(double))
            return RangeBuilder<TScoreFunction, TLeftRange, TRightRange>(names.FieldName, names.NumericTree, double.MinValue, (long)(object)value, scoreFunction,
                isNegated);
        if (typeof(TValue) == typeof(string))
        {
            var sliceValue = EncodeAndApplyAnalyzer((string)(object)value, fieldId);
            return RangeBuilder<TScoreFunction, TLeftRange, TRightRange>(names.FieldName, Slices.BeforeAllKeys, sliceValue, scoreFunction, isNegated);
        }

        throw new ArgumentException("Range queries are supporting strings, longs or doubles only");
    }
    
    private MultiTermMatch RangeBuilder<TScoreFunction, TLow, THigh>(Slice fieldName, Slice low, Slice high, TScoreFunction scoreFunction, bool isNegated)
        where TScoreFunction : IQueryScoreFunction
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
    {
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        var terms = fields?.CompactTreeFor(fieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        return (isNegated, scoreFunction) switch
        {
            (false, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<TermRangeProvider<TLow, THigh>>(_transaction.Allocator,
                new TermRangeProvider<TLow, THigh>(this, terms, fieldName, low, high))),
            (false, ConstantScoreFunction) =>  MultiTermMatch.Create(MultiTermBoostingMatch<TermRangeProvider<TLow, THigh>>.Create<ConstantScoreFunction>(this,
                new TermRangeProvider<TLow, THigh>(this, terms, fieldName, low, high), (ConstantScoreFunction)(object)scoreFunction)),
            _ => throw new NotSupportedException()
        };
    }

    private MultiTermMatch RangeBuilder<TScoreFunction, TLow, THigh>(Slice fieldName, Slice fieldLong, long low, long high, TScoreFunction scoreFunction, bool isNegated)
        where TScoreFunction : IQueryScoreFunction
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
    {
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        var terms = fields?.CompactTreeFor(fieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        var set = fields?.FixedTreeFor(fieldLong, sizeof(long));

        return (isNegated, scoreFunction) switch
        {
            (false, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<TermNumericRangeProvider<TLow, THigh, long>>(_transaction.Allocator,
                new TermNumericRangeProvider<TLow, THigh, long>(this, set, terms, fieldName, low, high))),
            (false, ConstantScoreFunction) => MultiTermMatch.Create(MultiTermBoostingMatch<TermNumericRangeProvider<TLow, THigh, long>>.Create<ConstantScoreFunction>(this,
                new TermNumericRangeProvider<TLow, THigh, long>(this, set, terms, fieldName, low, high), (ConstantScoreFunction)(object)scoreFunction)),
            _ => throw new NotSupportedException()
        };
    }

    private MultiTermMatch RangeBuilder<TScoreFunction, TLow, THigh>(Slice fieldName, Slice fieldLong, double low, double high, TScoreFunction scoreFunction, bool isNegated)
        where TScoreFunction : IQueryScoreFunction
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
    {
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        var terms = fields?.CompactTreeFor(fieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        var set = fields?.FixedTreeForDouble(fieldLong, sizeof(long));

        return (isNegated, scoreFunction) switch
        {
            (false, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<TermNumericRangeProvider<TLow, THigh, double>>(_transaction.Allocator,
                new TermNumericRangeProvider<TLow, THigh, double>(this, set, terms, fieldName, low, high))),
            (false, ConstantScoreFunction) => MultiTermMatch.Create(MultiTermBoostingMatch<TermNumericRangeProvider<TLow, THigh, double>>.Create<ConstantScoreFunction>(this,
                new TermNumericRangeProvider<TLow, THigh, double>(this, set, terms, fieldName, low, high), (ConstantScoreFunction)(object)scoreFunction)),
            _ => throw new NotSupportedException()
        };
    }
}

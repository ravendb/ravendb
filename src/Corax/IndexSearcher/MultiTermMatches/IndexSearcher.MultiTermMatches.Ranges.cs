using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Mappings;
using Corax.Queries;
using Corax.Queries.TermProviders;
using Voron;
using Voron.Data.Lookups;
using static Voron.Data.CompactTrees.CompactTree;
using Range = Corax.Queries.Meta.Range;

namespace Corax;

public partial class IndexSearcher
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch BetweenQuery<TValue>(FieldMetadata field, TValue low, TValue high, UnaryMatchOperation leftSide = UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation rightSide = UnaryMatchOperation.LessThanOrEqual, bool forward = true, bool streamingEnabled = false, long maxNumberOfTerms = long.MaxValue, CancellationToken token = default) {
        if (typeof(TValue) == typeof(long))
        {
            return (leftSide, rightSide) switch
            {
                // (x, y)
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) => RangeBuilder<Range.Exclusive, Range.Exclusive>(field, (long)(object)low, (long)(object)high, forward, streamingEnabled, maxNumberOfTerms, token: token),

                //<x, y)
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => RangeBuilder<Range.Inclusive, Range.Exclusive>(field, (long)(object)low, (long)(object)high, forward, streamingEnabled, maxNumberOfTerms, token: token),

                //<x, y>
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<Range.Inclusive, Range.Inclusive>(field, (long)(object)low, (long)(object)high, forward, streamingEnabled, maxNumberOfTerms, token: token),

                //(x, y>
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<Range.Exclusive, Range.Inclusive>(field, (long)(object)low, (long)(object)high, forward, streamingEnabled, maxNumberOfTerms, token: token),
                _ => throw new ArgumentOutOfRangeException($"Unknown operation at {nameof(BetweenQuery)}.")
            };
        }

        if (typeof(TValue) == typeof(double))
        {
            return (leftSide, rightSide) switch
            {
                // (x, y)
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) => RangeBuilder<Range.Exclusive, Range.Exclusive>(field, (double)(object)low, (double)(object)high,forward, streamingEnabled, maxNumberOfTerms, token: token),

                //<x, y)
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => RangeBuilder<Range.Inclusive, Range.Exclusive>(field, (double)(object)low, (double)(object)high, forward, streamingEnabled, maxNumberOfTerms, token: token),

                //<x, y>
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<Range.Inclusive, Range.Inclusive>(field, (double)(object)low, (double)(object)high, forward, streamingEnabled, maxNumberOfTerms, token: token),

                //(x, y>
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<Range.Exclusive, Range.Inclusive>(field, (double)(object)low, (double)(object)high, forward, streamingEnabled, maxNumberOfTerms, token: token),
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
                    leftValue, rightValue,forward, streamingEnabled, maxNumberOfTerms,token: token),

                //<x, y)
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => RangeBuilder<Range.Inclusive, Range.Exclusive>(field,
                    leftValue, rightValue, forward, streamingEnabled, maxNumberOfTerms,token: token),

                //<x, y>
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<Range.Inclusive, Range.Inclusive>(
                    field, leftValue, rightValue,forward, streamingEnabled, maxNumberOfTerms, token: token),

                //(x, y>
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<Range.Exclusive, Range.Inclusive>(field,
                    leftValue, rightValue,forward, streamingEnabled, maxNumberOfTerms,token: token),

                _ => throw new ArgumentOutOfRangeException($"Unknown operation at {nameof(BetweenQuery)}.")
            };
        }

        throw new ArgumentException($"{typeof(TValue)} is not supported in {nameof(BetweenQuery)}");
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch GreaterThanQuery<TValue>(FieldMetadata field, TValue value, bool forward = true, bool streamingEnabled = false,  long maxNumberOfTerms = long.MaxValue,CancellationToken token = default)
    {
        return GreatBuilder<Range.Exclusive, Range.Inclusive, TValue>(field, value, forward, streamingEnabled, maxNumberOfTerms, token: token);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch GreatThanOrEqualsQuery<TValue>(FieldMetadata field, TValue value, bool forward = true, bool streamingEnabled = false, long maxNumberOfTerms = long.MaxValue, CancellationToken token = default)
    {
        return GreatBuilder<Range.Inclusive, Range.Inclusive, TValue>(field, value, forward, streamingEnabled, maxNumberOfTerms , token: token);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private MultiTermMatch GreatBuilder<TLeftRange, TRightRange, TValue>(FieldMetadata field, TValue value, bool forward = true, bool streamingEnabled = false, long maxNumberOfTerms = long.MaxValue, CancellationToken token = default)
        where TLeftRange : struct, Range.Marker
        where TRightRange : struct, Range.Marker
    {
        if (typeof(TValue) == typeof(long))
        {
            return RangeBuilder<TLeftRange, TRightRange>(field, (long)(object)value, long.MaxValue, forward, streamingEnabled, maxNumberOfTerms,token: token);
        }

        if (typeof(TValue) == typeof(double))
            return RangeBuilder<TLeftRange, TRightRange>(field, (double)(object)value, double.MaxValue, forward, streamingEnabled, maxNumberOfTerms,token: token);
        if (typeof(TValue) == typeof(string))
        {
            var sliceValue = EncodeAndApplyAnalyzer(field, (string)(object)value);
            return RangeBuilder<TLeftRange, TRightRange>(field, sliceValue, Slices.AfterAllKeys,   forward, streamingEnabled, maxNumberOfTerms,token: token);
        }

        throw new ArgumentException("Range queries are supporting strings, longs or doubles only");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch LessThanOrEqualsQuery<TValue>(FieldMetadata field, TValue value, bool forward = true, bool streamingEnabled = false,  long maxNumberOfTerms = long.MaxValue, CancellationToken token = default)
        => LessBuilder<Range.Inclusive, Range.Inclusive, TValue>(field, value, forward, streamingEnabled,maxNumberOfTerms,  token: token);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch LessThanQuery<TValue>(FieldMetadata field, TValue value,bool forward = true, bool streamingEnabled = false,  long maxNumberOfTerms = long.MaxValue, CancellationToken token = default)
        => LessBuilder<Range.Inclusive, Range.Exclusive, TValue>(field, value, forward, streamingEnabled, maxNumberOfTerms, token: token);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private MultiTermMatch LessBuilder<TLeftRange, TRightRange, TValue>(FieldMetadata field, TValue value,
        bool forward, bool streamingEnabled, long maxNumberOfTerms, CancellationToken token)
        where TLeftRange : struct, Range.Marker
        where TRightRange : struct, Range.Marker
    {
        if (typeof(TValue) == typeof(long))
            return RangeBuilder<TLeftRange, TRightRange>(field, long.MinValue, (long)(object)value, forward, streamingEnabled, maxNumberOfTerms, token: token);

        if (typeof(TValue) == typeof(double))
            return RangeBuilder<TLeftRange, TRightRange>(field, double.MinValue, (double)(object)value, forward, streamingEnabled, maxNumberOfTerms, token: token);
        
        if (typeof(TValue) == typeof(string))
        {
            var sliceValue = EncodeAndApplyAnalyzer(field, (string)(object)value);
            return RangeBuilder<TLeftRange, TRightRange>(field, Slices.BeforeAllKeys, sliceValue, forward, streamingEnabled, maxNumberOfTerms, token: token);
        }

        throw new ArgumentException("Range queries are supporting strings, longs or doubles only");
    }
    
    private MultiTermMatch RangeBuilder<TLow, THigh>(FieldMetadata field, Slice low, Slice high,  bool forward, bool streamingEnabled, long maxNumberOfTerms,CancellationToken token)
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);
        
        return forward == true
            ? MultiTermMatch.Create(
                new MultiTermMatch<TermRangeProvider<Lookup<CompactKeyLookup>.ForwardIterator, TLow, THigh>>(this,
                    field, _transaction.Allocator,
                    new TermRangeProvider<Lookup<CompactKeyLookup>.ForwardIterator, TLow, THigh>(this, terms, field, low, high), streamingEnabled, maxNumberOfTerms, token: token))
            : MultiTermMatch.Create(
                new MultiTermMatch<TermRangeProvider<Lookup<CompactKeyLookup>.BackwardIterator, TLow, THigh>>(this,
                    field, _transaction.Allocator,
                    new TermRangeProvider<Lookup<CompactKeyLookup>.BackwardIterator, TLow, THigh>(this, terms, field, low, high), streamingEnabled, maxNumberOfTerms, token: token));
    }

    private MultiTermMatch RangeBuilder<TLow, THigh>(FieldMetadata field, long low, long high, bool forward, bool streamingEnabled, long maxNumberOfTerms, CancellationToken token)
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        field = field.GetNumericFieldMetadata<long>(Allocator);
        var set = _fieldsTree?.LookupFor<Int64LookupKey>(field.FieldName);

        return forward 
            ? MultiTermMatch.Create(new MultiTermMatch<TermNumericRangeProvider<Lookup<Int64LookupKey>.ForwardIterator, TLow, THigh, Int64LookupKey>>(this, field, _transaction.Allocator, new TermNumericRangeProvider<Lookup<Int64LookupKey>.ForwardIterator, TLow, THigh, Int64LookupKey>(this, set, field, low, high), streamingEnabled, maxNumberOfTerms,  token: token)) 
            : MultiTermMatch.Create(new MultiTermMatch<TermNumericRangeProvider<Lookup<Int64LookupKey>.BackwardIterator, TLow, THigh, Int64LookupKey>>(this, field, _transaction.Allocator, new TermNumericRangeProvider<Lookup<Int64LookupKey>.BackwardIterator, TLow, THigh, Int64LookupKey>(this, set, field, low, high), streamingEnabled, maxNumberOfTerms, token: token));
    }

    private MultiTermMatch RangeBuilder<TLow, THigh>(FieldMetadata field, double low, double high, bool forward, bool streamingEnabled, long maxNumberOfTerms, CancellationToken token)
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);
 
        
        field = field.GetNumericFieldMetadata<double>(Allocator);
        var set = _fieldsTree?.LookupFor<DoubleLookupKey>(field.FieldName); 
        return forward
            ? MultiTermMatch.Create(new MultiTermMatch<TermNumericRangeProvider<Lookup<DoubleLookupKey>.ForwardIterator, TLow, THigh, DoubleLookupKey>>(this, field,
                _transaction.Allocator, new TermNumericRangeProvider<Lookup<DoubleLookupKey>.ForwardIterator, TLow, THigh, DoubleLookupKey>(this, set, field, low, high), streamingEnabled, maxNumberOfTerms, token: token))
            : MultiTermMatch.Create(new MultiTermMatch<TermNumericRangeProvider<Lookup<DoubleLookupKey>.BackwardIterator, TLow, THigh, DoubleLookupKey>>(this, field,
                _transaction.Allocator,
                new TermNumericRangeProvider<Lookup<DoubleLookupKey>.BackwardIterator, TLow, THigh, DoubleLookupKey>(this, set, field, low, high), streamingEnabled, maxNumberOfTerms, token: token));
    }
}

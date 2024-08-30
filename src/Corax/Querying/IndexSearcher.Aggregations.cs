using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.TermProviders;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Range = Corax.Querying.Matches.Meta.Range;

namespace Corax.Querying;

public partial class IndexSearcher
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IAggregationProvider TextualAggregation(in FieldMetadata field, bool forward = true, bool streamingEnabled = false, in CancellationToken token = default)
    {
        var compactTree = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (compactTree is null)
            return new EmptyAggregationProvider();
        
        return forward
            ? new ExistsTermProvider<Lookup<CompactTree.CompactKeyLookup>.ForwardIterator>(this, compactTree, field, forAggregation: true)
            : new ExistsTermProvider<Lookup<CompactTree.CompactKeyLookup>.BackwardIterator>(this, compactTree, field, forAggregation: true);
    }

    public IAggregationProvider LowAggregationBuilder<TValue>(in FieldMetadata field, TValue value, UnaryMatchOperation operation, bool forward)
    {
        Debug.Assert(value is double or string, "value is double or string");
        Debug.Assert(operation is UnaryMatchOperation.LessThan or UnaryMatchOperation.LessThanOrEqual);
        
        return value switch
        {
            double d => BetweenAggregation(field, double.MinValue, d, UnaryMatchOperation.GreaterThanOrEqual, rightSide: operation,
                forward),
            string s => BetweenAggregation(field, Slices.BeforeAllKeys, EncodeAndApplyAnalyzer(default, s), UnaryMatchOperation.GreaterThanOrEqual,
                operation, forward),
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, null)
        };
    }

    public IAggregationProvider GreaterAggregationBuilder<TValue>(in FieldMetadata field, TValue value, UnaryMatchOperation operation, bool forward)
    {
        Debug.Assert(operation is UnaryMatchOperation.GreaterThan or UnaryMatchOperation.GreaterThanOrEqual);
        Debug.Assert(value is double or string, "value is double or string");
        
        return value switch
        {
            double d => BetweenAggregation(field, d, double.MaxValue, operation, rightSide: UnaryMatchOperation.LessThanOrEqual,
                forward),
            string s => BetweenAggregation(field, EncodeAndApplyAnalyzer(default, s), Slices.AfterAllKeys, operation,
                UnaryMatchOperation.LessThanOrEqual, forward),
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, null)
        };
    }
    
    public IAggregationProvider BetweenAggregation<TValue>(in FieldMetadata field, TValue low, TValue high,
        UnaryMatchOperation leftSide = UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation rightSide = UnaryMatchOperation.LessThanOrEqual, bool forward = true)
    {
        Debug.Assert(low is double or string or Slice, "value is double or string or Slice");
        
        if (typeof(TValue) == typeof(double))
        {
            return (leftSide, rightSide) switch
            {
                // (x, y)
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) => AggregationRangeBuilder<Range.Exclusive, Range.Exclusive>(field, (double)(object)low,
                    (double)(object)high, forward),

                //<x, y)
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => AggregationRangeBuilder<Range.Inclusive, Range.Exclusive>(field,
                    (double)(object)low,
                    (double)(object)high, forward),

                //<x, y>
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => AggregationRangeBuilder<Range.Inclusive, Range.Inclusive>(field,
                    (double)(object)low, (double)(object)high, forward),

                //(x, y>
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => AggregationRangeBuilder<Range.Exclusive, Range.Inclusive>(field,
                    (double)(object)low,
                    (double)(object)high, forward),
                _ => throw new ArgumentOutOfRangeException($"Unknown operation at {nameof(BetweenQuery)}.")
            };
        }

        if (typeof(TValue) == typeof(string) || typeof(TValue) == typeof(Slice))
        {
            Slice leftValue;
            Slice rightValue;

            if (typeof(string) == typeof(TValue))
            {
                leftValue = EncodeAndApplyAnalyzer(default, (string)(object)low);
                rightValue = EncodeAndApplyAnalyzer(default, (string)(object)high);
            }
            else
            {
                leftValue = (Slice)(object)low;
                rightValue = (Slice)(object)high;
            }

            return (leftSide, rightSide) switch
            {
                // (x, y)
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) => AggregationRangeBuilder<Range.Exclusive, Range.Exclusive>(field,
                    leftValue, rightValue, forward),

                //<x, y)
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => AggregationRangeBuilder<Range.Inclusive, Range.Exclusive>(field,
                    leftValue, rightValue, forward),

                //<x, y>
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => AggregationRangeBuilder<Range.Inclusive, Range.Inclusive>(
                    field, leftValue, rightValue, forward),

                //(x, y>
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => AggregationRangeBuilder<Range.Exclusive, Range.Inclusive>(field,
                    leftValue, rightValue, forward),

                _ => throw new ArgumentOutOfRangeException($"Unknown operation at {nameof(BetweenQuery)}.")
            };
        }

        throw new ArgumentException($"{typeof(TValue)} is not supported in {nameof(BetweenQuery)}");
    }

    private IAggregationProvider AggregationRangeBuilder<TLow, THigh>(in FieldMetadata field, Slice low, Slice high, bool forward)
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return new EmptyAggregationProvider();

        return forward switch
        {
            true => new TermRangeProvider<Lookup<CompactTree.CompactKeyLookup>.ForwardIterator, TLow, THigh>(this, terms, field, low, high),
            false => new TermRangeProvider<Lookup<CompactTree.CompactKeyLookup>.BackwardIterator, TLow, THigh>(this, terms, field, low, high)
        };
    }

    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IAggregationProvider AggregationRangeBuilder<TLow, THigh>(FieldMetadata field, double low, double high, bool forward)
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
        => AggregationRangeBuilder<DoubleLookupKey, double, TLow, THigh>(field, new(low), new(high), forward);


    private IAggregationProvider AggregationRangeBuilder<TLookupKey, TTermType, TLow, THigh>(FieldMetadata field, TLookupKey low, TLookupKey high, bool forward)
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
        where TLookupKey : struct, ILookupKey
    {
        field = field.GetNumericFieldMetadata<TTermType>(Allocator);
        var set = _fieldsTree?.LookupFor<TLookupKey>(field.FieldName);
        if (set is null || set.NumberOfEntries == 0)
            return new EmptyAggregationProvider();

        return forward switch
        {
            true => new TermNumericRangeProvider<Lookup<TLookupKey>.ForwardIterator, TLow, THigh, TLookupKey>(this, set, field, low, high),
            false => new TermNumericRangeProvider<Lookup<TLookupKey>.BackwardIterator, TLow, THigh, TLookupKey>(this, set, field, low, high)
        };
    }
}

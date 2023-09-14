using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Mappings;
using Corax.Queries;
using Corax.Queries.Meta;
using Corax.Utils;
using Voron;

namespace Corax.IndexSearcher;

public partial class IndexSearcher
{
    public UnaryMatch UnaryQuery<TInner, TValueType>(in TInner set, FieldMetadata field, TValueType term, UnaryMatchOperation @operation,
        int take = Constants.IndexSearcher.TakeAll, in CancellationToken token = default)
        where TInner : IQueryMatch
    {
        if (typeof(TValueType) == typeof(string))
        {
            var slice = EncodeAndApplyAnalyzer(field, (string)(object)term);
            return BuildUnaryMatch(in set, field, slice, @operation, take, token);
        }
        else if (typeof(TValueType) == typeof(Slice))
        {
            ApplyAnalyzer(field, field.Analyzer, (Slice)(object)term, out var slice);
            return BuildUnaryMatch(in set, field, slice, @operation, take, token);
        }
        else if (typeof(TValueType) == typeof(TermQueryItem[]))
        {
            return UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldAllIn(set, this, field, term, take: take), token);
        }


        return BuildUnaryMatch(in set, field, term, @operation, take, token);
    }

    public UnaryMatch EqualsNull<TInner>(in TInner set, FieldMetadata field, UnaryMatchOperation @operation, int take = Constants.IndexSearcher.TakeAll, in CancellationToken token = default)
        where TInner : IQueryMatch
    {
        return BuildNullUnaryMatch<TInner, Slice>(in set, field, @operation, take, token);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public UnaryMatch UnaryBetween<TInner, TValueType>(in TInner set, FieldMetadata field, TValueType leftValue, TValueType rightValue, bool isNegated = false, int take = Constants.IndexSearcher.TakeAll, in CancellationToken token = default)
        where TInner : IQueryMatch
    {
        if (typeof(TValueType) == typeof(string))
        {
            var leftValueSlice = EncodeAndApplyAnalyzer(field, (string)(object)leftValue);
            var rightValueSlice = EncodeAndApplyAnalyzer(field, (string)(object)rightValue);

            return BuildBetween(in set, field, leftValueSlice, rightValueSlice, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual, isNegated, take, token);
        }
        else if (typeof(TValueType) == typeof(Slice))
        {
            ApplyAnalyzer(field, field.Analyzer, (Slice)(object)leftValue, out var leftValueSlice);
            ApplyAnalyzer(field, field.Analyzer, (Slice)(object)rightValue, out var rightValueSlice);

            return BuildBetween(in set, field, leftValueSlice, rightValueSlice, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual, isNegated, take, token);
        }

        return BuildBetween(in set, field, leftValue, rightValue, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual, isNegated, take, token);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public UnaryMatch UnaryBetween<TInner, TValueType>(in TInner set, FieldMetadata field, TValueType leftValue, TValueType rightValue, UnaryMatchOperation leftSide, UnaryMatchOperation rightSide, bool isNegated = false,
        int take = Constants.IndexSearcher.TakeAll, in CancellationToken token = default)
        where TInner : IQueryMatch
    {
        if (typeof(TValueType) == typeof(string))
        {
            var leftValueSlice = EncodeAndApplyAnalyzer(field, (string)(object)leftValue);
            var rightValueSlice = EncodeAndApplyAnalyzer(field, (string)(object)rightValue);

            return BuildBetween(in set, field, leftValueSlice, rightValueSlice, leftSide, rightSide, isNegated, take, token);
        }
        else if (typeof(TValueType) == typeof(Slice))
        {
            ApplyAnalyzer(field, field.Analyzer,(Slice)(object)leftValue, out var leftValueSlice);
            ApplyAnalyzer(field, field.Analyzer, (Slice)(object)rightValue, out var rightValueSlice);

            return BuildBetween(in set, field, leftValueSlice, rightValueSlice, leftSide, rightSide, isNegated, take, token);
        }

        return BuildBetween(in set, field, leftValue, rightValue, leftSide, rightSide, isNegated, take);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private UnaryMatch BuildBetween<TInner, TValueType>(in TInner set, FieldMetadata field, TValueType leftValue, TValueType rightValue, UnaryMatchOperation leftSide, UnaryMatchOperation rightSide, bool isNegated = false, int take = Constants.IndexSearcher.TakeAll, in CancellationToken token = default)
        where TInner : IQueryMatch
    {
        return (isNegated, leftSide, rightSide) switch
        {
            // $const <= X <= $const
            (false, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) =>
                UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldBetweenMatch<
                    UnaryMatch<TInner, TValueType>.GreaterThanOrEqualMatchComparer,
                    UnaryMatch<TInner, TValueType>.LessThanOrEqualMatchComparer
                >(set, this, field, leftValue, rightValue, take), token),
            (true, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) =>
                UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldNotBetweenMatch<
                    UnaryMatch<TInner, TValueType>.GreaterThanOrEqualMatchComparer,
                    UnaryMatch<TInner, TValueType>.LessThanOrEqualMatchComparer
                >(set, this, field, leftValue, rightValue, take), token),

            // $const < X <= $const
            (false, UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) =>
                UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldBetweenMatch<
                    UnaryMatch<TInner, TValueType>.GreaterThanMatchComparer,
                    UnaryMatch<TInner, TValueType>.LessThanOrEqualMatchComparer
                >(set, this, field, leftValue, rightValue, take), token),
            (true, UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) =>
                UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldNotBetweenMatch<
                    UnaryMatch<TInner, TValueType>.GreaterThanMatchComparer,
                    UnaryMatch<TInner, TValueType>.LessThanOrEqualMatchComparer
                >(set, this, field, leftValue, rightValue, take), token),

            // $const <= X < $const
            (false, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) =>
                UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldBetweenMatch<
                    UnaryMatch<TInner, TValueType>.GreaterThanOrEqualMatchComparer,
                    UnaryMatch<TInner, TValueType>.LessThanMatchComparer
                >(set, this, field, leftValue, rightValue, take), token),
            (true, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) =>
                UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldNotBetweenMatch<
                    UnaryMatch<TInner, TValueType>.GreaterThanOrEqualMatchComparer,
                    UnaryMatch<TInner, TValueType>.LessThanMatchComparer
                >(set, this, field, leftValue, rightValue, take), token),

            // $const < X < $const
            (false, UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) =>
                UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldBetweenMatch<
                    UnaryMatch<TInner, TValueType>.GreaterThanMatchComparer,
                    UnaryMatch<TInner, TValueType>.LessThanMatchComparer
                >(set, this, field, leftValue, rightValue, take), token),
            (true, UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) =>
                UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldNotBetweenMatch<
                    UnaryMatch<TInner, TValueType>.GreaterThanMatchComparer,
                    UnaryMatch<TInner, TValueType>.LessThanMatchComparer
                >(set, this, field, leftValue, rightValue, take), token),

            _ => throw new InvalidDataException($"Invalid parameter for between match.")
        };
    }

    private UnaryMatch BuildNullUnaryMatch<TInner, TValueType>(in TInner set, FieldMetadata field, UnaryMatchOperation @operation,
    int take = Constants.IndexSearcher.TakeAll, in CancellationToken token = default)
    where TInner : IQueryMatch
    {
        return @operation switch
        {
            UnaryMatchOperation.Equals => UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldIsNull(set, this, field, take: take), token),
            UnaryMatchOperation.NotEquals => UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldIsNotNull(set, this, field, UnaryMatchOperationMode.All, take: take), token),
            _ => throw new ArgumentOutOfRangeException(nameof(operation), @operation, $"Wrong {nameof(UnaryQuery)} was called. Check other overloads.")
        };
    }
    
    private UnaryMatch BuildUnaryMatch<TInner, TValueType>(in TInner set, FieldMetadata field, TValueType term, UnaryMatchOperation @operation,
        int take = Constants.IndexSearcher.TakeAll, in CancellationToken token = default)
        where TInner : IQueryMatch
    {
        return @operation switch
        {
            UnaryMatchOperation.GreaterThan => UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldGreaterThan(set, this, field, term, take: take), token),
            UnaryMatchOperation.GreaterThanOrEqual => UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldGreaterThanOrEqualMatch(set, this, field, term, take: take), token),
            UnaryMatchOperation.LessThan => UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldLessThan(set, this, field, term, take: take), token),
            UnaryMatchOperation.LessThanOrEqual => UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldLessThanOrEqualMatch(set, this, field, term, take: take), token),
            UnaryMatchOperation.Equals => UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldEqualsMatch(set, this, field, term, take: take), token),
            UnaryMatchOperation.NotEquals => UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldNotEqualsMatch(set, this, field, term, UnaryMatchOperationMode.All, take: take), token),
            _ => throw new ArgumentOutOfRangeException(nameof(operation), @operation, $"Wrong {nameof(UnaryQuery)} was called. Check other overloads.")
        };
    }
}

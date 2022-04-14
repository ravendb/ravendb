using System;
using System.Runtime.CompilerServices;
using Corax.Queries;
using Voron;

namespace Corax;

public partial class IndexSearcher
{
    public UnaryMatch UnaryQuery<TInner, TValueType>(in TInner set, int fieldId, TValueType term, UnaryMatchOperation @operation,
        int take = Constants.IndexSearcher.TakeAll)
        where TInner : IQueryMatch
    {
        if (typeof(TValueType) == typeof(string))
        {
            var slice = EncodeAndApplyAnalyzer((string)(object)term, fieldId);
            return BuildUnaryMatch(in set, fieldId, slice, @operation, take);
        }

        return BuildUnaryMatch(in set, fieldId, term, @operation, take);
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public UnaryMatch Between<TInner, TValueType>(in TInner set, int fieldId, TValueType leftValue, TValueType rightValue, bool isNegated = false,
        int take = Constants.IndexSearcher.TakeAll)
        where TInner : IQueryMatch
    {
        if (typeof(TValueType) == typeof(string))
        {
            var leftValueSlice = EncodeAndApplyAnalyzer((string)(object)leftValue, fieldId);
            var rightValueSlice = EncodeAndApplyAnalyzer((string)(object)rightValue, fieldId);

            return BuildBetween(in set, fieldId, leftValueSlice, rightValueSlice, isNegated, take);
        }

        return BuildBetween(in set, fieldId, leftValue, rightValue, isNegated, take);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private UnaryMatch BuildBetween<TInner, TValueType>(in TInner set, int fieldId, TValueType leftValue, TValueType rightValue, bool isNegated = false,
        int take = Constants.IndexSearcher.TakeAll)
        where TInner : IQueryMatch
    {
        return isNegated
            ? UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldNotBetweenMatch(set, this, fieldId, leftValue, rightValue, take))
            : UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldBetweenMatch(set, this, fieldId, leftValue, rightValue, take));
    }

    private UnaryMatch BuildUnaryMatch<TInner, TValueType>(in TInner set, int fieldId, TValueType term, UnaryMatchOperation @operation,
        int take = Constants.IndexSearcher.TakeAll)
        where TInner : IQueryMatch
    {
        return @operation switch
        {
            UnaryMatchOperation.GreaterThan => UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldGreaterThan(set, this, fieldId, term, take: take)),
            UnaryMatchOperation.GreaterThanOrEqual => UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldGreaterThanOrEqualMatch(set, this, fieldId, term, take: take)),
            UnaryMatchOperation.LessThan => UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldLessThan(set, this, fieldId, term, take: take)),
            UnaryMatchOperation.LessThanOrEqual => UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldLessThanOrEqualMatch(set, this, fieldId, term, take: take)),
            UnaryMatchOperation.Equals => UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldEqualsMatch(set, this, fieldId, term, take: take)),
            UnaryMatchOperation.NotEquals => UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldNotEqualsMatch(set, this, fieldId, term, true, take)),
            _ => throw new ArgumentOutOfRangeException(nameof(operation), @operation, $"Wrong {nameof(UnaryQuery)} was called. Check other overloads.")
        };
    }
}

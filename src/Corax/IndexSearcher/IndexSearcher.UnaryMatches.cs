using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Xml.Xsl;
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

            return BuildBetween(in set, fieldId, leftValueSlice, rightValueSlice, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual, isNegated, take);
        }

        return BuildBetween(in set, fieldId, leftValue, rightValue, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual, isNegated, take);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public UnaryMatch Between<TInner, TValueType>(in TInner set, int fieldId, TValueType leftValue, TValueType rightValue, UnaryMatchOperation leftSide, UnaryMatchOperation rightSide, bool isNegated = false,
        int take = Constants.IndexSearcher.TakeAll)
        where TInner : IQueryMatch
    {
        if (typeof(TValueType) == typeof(string))
        {
            var leftValueSlice = EncodeAndApplyAnalyzer((string)(object)leftValue, fieldId);
            var rightValueSlice = EncodeAndApplyAnalyzer((string)(object)rightValue, fieldId);

            return BuildBetween(in set, fieldId, leftValueSlice, rightValueSlice, leftSide, rightSide, isNegated, take);
        }

        return BuildBetween(in set, fieldId, leftValue, rightValue, leftSide, rightSide, isNegated, take);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private UnaryMatch BuildBetween<TInner, TValueType>(in TInner set, int fieldId, TValueType leftValue, TValueType rightValue, UnaryMatchOperation leftSide, UnaryMatchOperation rightSide, bool isNegated = false,
        int take = Constants.IndexSearcher.TakeAll)
        where TInner : IQueryMatch
    {
        return (isNegated, leftSide, rightSide) switch
        {
            // $const <= X <= $const
            (false, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) =>
                UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldBetweenMatch<
                    UnaryMatch<TInner, TValueType>.GreaterThanOrEqualMatchComparer,
                    UnaryMatch<TInner, TValueType>.LessThanOrEqualMatchComparer
                >(set, this, fieldId, leftValue, rightValue, take)),
            (true, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) =>
                UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldNotBetweenMatch<
                    UnaryMatch<TInner, TValueType>.GreaterThanOrEqualMatchComparer,
                    UnaryMatch<TInner, TValueType>.LessThanOrEqualMatchComparer
                >(set, this, fieldId, leftValue, rightValue, take)),

            // $const < X <= $const
            (false, UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) =>
                UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldBetweenMatch<
                    UnaryMatch<TInner, TValueType>.GreaterThanMatchComparer,
                    UnaryMatch<TInner, TValueType>.LessThanOrEqualMatchComparer
                >(set, this, fieldId, leftValue, rightValue, take)),
            (true, UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) =>
                UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldNotBetweenMatch<
                    UnaryMatch<TInner, TValueType>.GreaterThanMatchComparer,
                    UnaryMatch<TInner, TValueType>.LessThanOrEqualMatchComparer
                >(set, this, fieldId, leftValue, rightValue, take)),

            // $const <= X < $const
            (false, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) =>
                UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldBetweenMatch<
                    UnaryMatch<TInner, TValueType>.GreaterThanOrEqualMatchComparer,
                    UnaryMatch<TInner, TValueType>.LessThanMatchComparer
                >(set, this, fieldId, leftValue, rightValue, take)),
            (true, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) =>
                UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldNotBetweenMatch<
                    UnaryMatch<TInner, TValueType>.GreaterThanOrEqualMatchComparer,
                    UnaryMatch<TInner, TValueType>.LessThanMatchComparer
                >(set, this, fieldId, leftValue, rightValue, take)),

            // $const < X < $const
            (false, UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) =>
                UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldBetweenMatch<
                    UnaryMatch<TInner, TValueType>.GreaterThanMatchComparer,
                    UnaryMatch<TInner, TValueType>.LessThanMatchComparer
                >(set, this, fieldId, leftValue, rightValue, take)),
            (true, UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) =>
                UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldNotBetweenMatch<
                    UnaryMatch<TInner, TValueType>.GreaterThanMatchComparer,
                    UnaryMatch<TInner, TValueType>.LessThanMatchComparer
                >(set, this, fieldId, leftValue, rightValue, take)),

            _ => throw new InvalidDataException($"Invalid parameter for between match.")
        };
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

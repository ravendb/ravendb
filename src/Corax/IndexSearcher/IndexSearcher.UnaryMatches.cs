using System;
using System.IO;
using System.Runtime.CompilerServices;
using Corax.Queries;
using Corax.Utils;
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
        else if (typeof(TValueType) == typeof(Slice))
        {
            Slice.From(Allocator, ApplyAnalyzer((Slice)(object)term, fieldId), out var slice);
            return BuildUnaryMatch(in set, fieldId, slice, @operation, take);
        }
        else if (typeof(TValueType) == typeof(TermQueryItem[]))
        {
            return UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldAllIn(set, this, fieldId, term, take: take));
        }


        return BuildUnaryMatch(in set, fieldId, term, @operation, take);
    }

    public UnaryMatch EqualsNull<TInner>(in TInner set, int fieldId, UnaryMatchOperation @operation, int take = Constants.IndexSearcher.TakeAll)
        where TInner : IQueryMatch
    {
        return BuildUnaryMatch<TInner, Slice>(in set, fieldId, @operation, take);
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
        else if (typeof(TValueType) == typeof(Slice))
        {
            Slice.From(Allocator, ApplyAnalyzer((Slice)(object)leftValue, fieldId), out var leftValueSlice);
            Slice.From(Allocator, ApplyAnalyzer((Slice)(object)rightValue, fieldId), out var rightValueSlice);

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
        else if (typeof(TValueType) == typeof(Slice))
        {
            Slice.From(Allocator, ApplyAnalyzer((Slice)(object)leftValue, fieldId), out var leftValueSlice);
            Slice.From(Allocator, ApplyAnalyzer((Slice)(object)rightValue, fieldId), out var rightValueSlice);

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

    private UnaryMatch BuildUnaryMatch<TInner, TValueType>(in TInner set, int fieldId, UnaryMatchOperation @operation,
    int take = Constants.IndexSearcher.TakeAll)
    where TInner : IQueryMatch
    {
        return @operation switch
        {
            UnaryMatchOperation.Equals => UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldIsNull(set, this, fieldId, take: take)),
            UnaryMatchOperation.NotEquals => UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldIsNotNull(set, this, fieldId, UnaryMatchOperationMode.All, take: take)),
            _ => throw new ArgumentOutOfRangeException(nameof(operation), @operation, $"Wrong {nameof(UnaryQuery)} was called. Check other overloads.")
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
            UnaryMatchOperation.NotEquals => UnaryMatch.Create(UnaryMatch<TInner, TValueType>.YieldNotEqualsMatch(set, this, fieldId, term, UnaryMatchOperationMode.All, take: take)),
            _ => throw new ArgumentOutOfRangeException(nameof(operation), @operation, $"Wrong {nameof(UnaryQuery)} was called. Check other overloads.")
        };
    }
}

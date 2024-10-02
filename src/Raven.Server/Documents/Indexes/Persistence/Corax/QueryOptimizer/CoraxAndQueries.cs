using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;

public sealed class CoraxAndQueries : CoraxBooleanQueryBase
{
    private readonly List<CoraxBooleanItem> _queryStack;

    public CoraxAndQueries(IndexSearcher indexSearcher, CoraxQueryBuilder.Parameters parameters, CoraxBooleanItem left, CoraxBooleanItem right) :
        base(indexSearcher, parameters)
    {
        _queryStack = new List<CoraxBooleanItem>() {left, right};
    }

    public bool TryMerge(CoraxAndQueries other)
    {
        if (EqualsScoreFunctions(other) == false)
            return false;

        _queryStack.AddRange(other._queryStack);
        return true;
    }

    public bool TryAnd(IQueryMatch item)
    {
        switch (item)
        {
            case CoraxBooleanQueryBase cbqb:
                throw new InvalidOperationException($"CoraxBooleanQueryBase should be merged via {nameof(TryMerge)} method.");
            case CoraxBooleanItem cbi:
                _queryStack.Add(cbi);
                return true;
            default:
                return false;
        }
    }

    public override IQueryMatch Materialize()
    {
        var stack = CollectionsMarshal.AsSpan(_queryStack);
        var noStreaming = new CoraxQueryBuilder.StreamingOptimization();

        if (ShouldPerformScan(stack, out var queryPosition))
        {
            MultiUnaryItem[] listOfMergedUnaries = new MultiUnaryItem[stack.Length - 1];
            int unaryPos = 0;
            for (var it = 0; it < stack.Length; it++)
            {
                if (it == queryPosition)
                    continue;

                var query = stack[it];
                if (query.Operation is UnaryMatchOperation.Between)
                {
                    listOfMergedUnaries[unaryPos] = (query.Term, query.Term2) switch
                    {
                        (long l, long l2) => new MultiUnaryItem(query.Field, l, l2, query.BetweenLeft, query.BetweenRight),
                        (double d, double d2) => new MultiUnaryItem(query.Field, d, d2, query.BetweenLeft, query.BetweenRight),
                        (string s, string s2) => new MultiUnaryItem(IndexSearcher, query.Field, s, s2, query.BetweenLeft, query.BetweenRight),
                        (long l, double d) => new MultiUnaryItem(query.Field, Convert.ToDouble(l), d, query.BetweenLeft, query.BetweenRight),
                        (double d, long l) => new MultiUnaryItem(query.Field, d, Convert.ToDouble(l), query.BetweenLeft, query.BetweenRight),
                        _ => throw new InvalidOperationException($"UnaryMatchOperation {query.Operation} is not supported for type {query.Term.GetType()}")
                    };
                }
                else
                {
                    listOfMergedUnaries[unaryPos] = query.Term switch
                    {
                        long longTerm => new MultiUnaryItem(query.Field, longTerm, query.Operation),
                        double doubleTerm => new MultiUnaryItem(query.Field, doubleTerm, query.Operation),
                        _ => new MultiUnaryItem(IndexSearcher, query.Field, query.Term as string, query.Operation),
                    };
                }

                unaryPos++;
            }

            return IndexSearcher.CreateMultiUnaryMatch(stack[queryPosition].Materialize(ref noStreaming), listOfMergedUnaries);
        }

        IQueryMatch match = null;
        stack.Sort(PrioritizeSort);
        //stack.Reverse(); // we want to have BIGGEST at the very beginning to avoid filling big match multiple times

        foreach (ref var query in stack)
        {
            var materializedQuery = query.Materialize(ref noStreaming);

            match = match is null
                ? materializedQuery
                : IndexSearcher.And(materializedQuery, match);
        }


        bool ShouldPerformScan(Span<CoraxBooleanItem> queries, out int pos)
        {
            pos = -1;
            if (IsBoosting)
                return false;
            
            var minimumCount = long.MaxValue;
            for (int idX = 0; idX < queries.Length; ++idX)
            {
                ref var query = ref queries[idX];
                if (query.Operation is UnaryMatchOperation.Equals && query.Count < minimumCount)
                {
                    pos = idX;
                    minimumCount = query.Count;
                }
            }


            return minimumCount < 32 * 1024; // 32K items seems ok
        }

        return IsBoosting ? IndexSearcher.Boost(match, Boosting.Value) : match;
    }

    private static int PrioritizeSort(CoraxBooleanItem firstUnaryItem, CoraxBooleanItem secondUnaryItem)
    {
        switch (firstUnaryItem.Operation)
        {
            //After benchmarks we discover it's not better to call termmatch as first item in case when MultiTermMatch has more terms than our termmmatch's posting lists has items;
            case UnaryMatchOperation.Equals when secondUnaryItem.Operation is not (UnaryMatchOperation.NotEquals or UnaryMatchOperation.Equals):
                return firstUnaryItem.Count.CompareTo(secondUnaryItem.Count);
            case UnaryMatchOperation.Equals when secondUnaryItem.Operation != UnaryMatchOperation.Equals:
                return -1;
        }

        if (firstUnaryItem.Operation != UnaryMatchOperation.Equals && secondUnaryItem.Operation == UnaryMatchOperation.Equals)
            return 1;
        if (firstUnaryItem.Operation == UnaryMatchOperation.Between && secondUnaryItem.Operation != UnaryMatchOperation.Between)
            return -1;
        if (firstUnaryItem.Operation != UnaryMatchOperation.Between && secondUnaryItem.Operation == UnaryMatchOperation.Between)
            return 1;

        //This And(MultiTermMatch, MultiTermMatch) we force match with biggest amount of term in it to avoid crawling through
        if (firstUnaryItem.Operation == UnaryMatchOperation.Between && secondUnaryItem.Operation == UnaryMatchOperation.Between)
            return secondUnaryItem.Count.CompareTo(firstUnaryItem.Count);

        return secondUnaryItem.Count.CompareTo(firstUnaryItem.Count);
    }

    public new bool IsBoosting => Boosting.HasValue;
}

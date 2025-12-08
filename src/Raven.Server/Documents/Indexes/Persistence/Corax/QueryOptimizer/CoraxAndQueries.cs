using System;
using System.Runtime.InteropServices;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;

public sealed class CoraxAndQueries : CoraxBooleanQueryBase
{
    private CoraxAndQueries(CoraxQueryBuilder.Parameters parameters) : base(parameters)
    {
    }

    public static CoraxAndQueries And(CoraxQueryBuilder.Parameters parameters, IQueryMatch left, IQueryMatch right)
    {
        {
            if (left is CoraxAndQueries caqLeft && right is CoraxAndQueries caqRight)
            {
                parameters.BuildSteps?.Add($"Trying to merge AND queries.");
                return MergeOrCreateNew(parameters, caqLeft, caqRight);
            }
        }

        {
            if (left is CoraxAndQueries { HasBoosting: false } caqLeft)
                return (CoraxAndQueries)caqLeft.Add(right);
            
            if (right is CoraxAndQueries {HasBoosting: false} caqRight)
                return (CoraxAndQueries)caqRight.Add(left);
        }
        
        return CreateNew(parameters, left, right);
    }

    private static CoraxAndQueries MergeOrCreateNew(CoraxQueryBuilder.Parameters parameters, CoraxAndQueries left, CoraxAndQueries right)
    {
        if (left.EqualsScoreFunctions(right) == false)
        {
            parameters.BuildSteps?.Add($"Cannot merge AND queries because they have different score functions.");
            return CreateNew(parameters, left, right);
        }
        
        return left.Merge(right);
    }

    private CoraxAndQueries Merge(CoraxAndQueries other)
    {
        _parameters.BuildSteps?.Add($"Merging AND queries.");
        if (other.VectorStack != null)
        {
            if (VectorStack == null)
                VectorStack = other.VectorStack;
            else
                VectorStack.AddRange(other.VectorStack);
        }

        if (other.QueryStack != null)
        {
            if (QueryStack == null)
                QueryStack = other.QueryStack;
            else
                QueryStack.AddRange(other.QueryStack);
        }

        if (other.ComplexMatches != null)
        {
            if (ComplexMatches == null)
                ComplexMatches = other.ComplexMatches;
            else
                ComplexMatches.AddRange(other.ComplexMatches);
        }

        return this;
    }

    private static CoraxAndQueries CreateNew(CoraxQueryBuilder.Parameters parameters, IQueryMatch left, IQueryMatch right)
    {
        var caq = new CoraxAndQueries(parameters);
        caq.Add(left);
        caq.Add(right);
        return caq;
    }

    protected override void AddCoraxBooleanItem(CoraxBooleanItem item)
    {
        _parameters.BuildSteps?.Add($"  Adding CoraxBooleanItem to query.");
        QueryStack ??= new();
        QueryStack.Add(item);
    }

    public override IQueryMatch Materialize()
    {
        var indexSearcher = _parameters.IndexSearcher;
        var stack = QueryStack is null ? Span<CoraxBooleanItem>.Empty : CollectionsMarshal.AsSpan(QueryStack);
        var noStreaming = new CoraxQueryBuilder.StreamingOptimization();
        IQueryMatch match = null;
        var shouldScan = ShouldPerformScan(stack, out var queryPosition);
        if (shouldScan)
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
                        (string s, string s2) => new MultiUnaryItem(indexSearcher, query.Field, s, s2, query.BetweenLeft, query.BetweenRight),
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
                        _ => new MultiUnaryItem(indexSearcher, query.Field, query.Term as string, query.Operation),
                    };
                }

                unaryPos++;
            }

            match = indexSearcher.CreateMultiUnaryMatch(stack[queryPosition].Materialize(ref noStreaming), listOfMergedUnaries);
        }

        if (shouldScan == false)
        {
            stack.Sort(PrioritizeSort);
            //stack.Reverse(); // we want to have BIGGEST at the very beginning to avoid filling big match multiple times

            foreach (ref var query in stack)
            {
                var materializedQuery = query.Materialize(ref noStreaming);

                match = match is null
                    ? materializedQuery
                    : indexSearcher.And(materializedQuery, match);
            }
        }

        if (ComplexMatches != null)
        {
            foreach (var complex in ComplexMatches)
                match = match is null ? complex : indexSearcher.And(complex, match);
        }

        if (VectorStack != null)
        {
            //todo consider what to do if we've more than two? for now simplify the path
            foreach (var vector in VectorStack)
                match = vector.Materialize(match);
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

                // RavenDB-22603: NotEquals IS supported by MultiUnaryMatch via UnaryMode.All.
                // Skip it when looking for the Equals anchor, but include it in the scan.
                if (query.Operation is UnaryMatchOperation.NotEquals)
                    continue;

                if (query.Operation is UnaryMatchOperation.Equals && query.Count < minimumCount)
                {
                    pos = idX;
                    minimumCount = query.Count;
                }
            }


            return minimumCount < 32 * 1024; // 32K items seems ok
        }

        return IsBoosting ? indexSearcher.Boost(match, Boosting.Value) : match;
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

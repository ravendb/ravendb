using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using Corax;
using Corax.Queries;
using Corax.Utils;
using Sparrow.Extensions;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;

public class CoraxAndQueries : CoraxBooleanQueryBase
{
    private readonly List<CoraxBooleanItem> _queryStack;

    public CoraxAndQueries(IndexSearcher indexSearcher, MemoizationMatchProvider<AllEntriesMatch> allEntries, CoraxBooleanItem left, CoraxBooleanItem right) : base(indexSearcher)
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
        Debug.Assert(_queryStack.Count > 0);
        
        IQueryMatch baseMatch = null;
        var stack = CollectionsMarshal.AsSpan(_queryStack);
        int reduced = 0;
        
        _queryStack.Sort(PrioritizeSort);
        
        // Build a stack of termmatch at the beginning. 
        // Opt: Maybe we can do it like MultiTermMatch in the future? It should be faster than performing BinaryMatch.
        foreach (var query in stack)
        {
            //Out of TermMatches in our stack
            if (query.Operation is not (UnaryMatchOperation.Equals or UnaryMatchOperation.NotEquals))
                break;

            //We're always do TermMatch (true and NOT (X))
            IQueryMatch second = IndexSearcher.TermQuery(query.Field, query.TermAsString);

            if (query.Operation is UnaryMatchOperation.NotEquals)
            {
                //This could be more expensive than scanning RAW elements. This returns ~(field.NumberOfEntries - term.NumberOfEntries). Can we set threshold around ~10% to perform SCAN maybe? 
                if (baseMatch != null)
                {
                    // Instead of performing AND(TermMatch, AndNot(Exist, Term)) we can translate it into AndNot(baseMatch, Term). This way we avoid additional BinaryMatch
                    baseMatch = IndexSearcher.AndNot(baseMatch, second);
                    _hasBinary = true;
                    goto Reduce;
                }

                //In the first place we've to do (true and NOT))
                baseMatch = IndexSearcher.AndNot<MultiTermMatch, TermMatch>(IndexSearcher.ExistsQuery(query.Field), (TermMatch)second);
                _hasBinary = true;
                goto Reduce;
            }


            // TermMatch:
            // This should be more complex. Should we always perform AND for TermMatch? For example there could be a case when performing RangeQueries in first place will limit our set very well so scanning would be better option.

            
            if (baseMatch == null)
            {
                baseMatch = second;
            }
            else
            {
                baseMatch = IndexSearcher.And(baseMatch, second);
                _hasBinary = true;
            }
            Reduce:
            reduced++;

        }

        stack = stack.Slice(reduced);
        if (stack.Length == 0)
            goto Return;

        //We will perform a scan for the rest. We want to evaluate leftmostClause as our inner match.
        var leftmostClause = stack[0];
        var nextQuery = TransformCoraxBooleanItemIntoQueryMatch(leftmostClause);
        baseMatch = baseMatch is null
                ? nextQuery
                : IndexSearcher.And(baseMatch, nextQuery);
       

        MultiUnaryItem[] listOfMergedUnaries = new MultiUnaryItem[stack.Length - 1];
        for (var index = 1; index < stack.Length; index++)
        {
            var query = stack[index];
            if (query.Operation is UnaryMatchOperation.Between)
            {
                listOfMergedUnaries[index - 1] = (query.Term, query.Term2) switch
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
                listOfMergedUnaries[index - 1] = query.Term switch
                {
                    long longTerm => new MultiUnaryItem(query.Field, longTerm, query.Operation),
                    double doubleTerm => new MultiUnaryItem(query.Field, doubleTerm, query.Operation),
                    _ => new MultiUnaryItem(IndexSearcher, query.Field, query.Term as string, query.Operation),
                };
            }
        }

        if (listOfMergedUnaries.Length > 0)
        {
            baseMatch = IndexSearcher.CreateMultiUnaryMatch(baseMatch ?? IndexSearcher.ExistsQuery(stack[1].Field), listOfMergedUnaries);
        }

        Return:
        return Boosting.HasValue == false
            ? baseMatch
            : IndexSearcher.Boost(baseMatch, Boosting.Value);
    }
    
    private static int PrioritizeSort(CoraxBooleanItem firstUnaryItem, CoraxBooleanItem secondUnaryItem)
    {
        if (firstUnaryItem.Operation == UnaryMatchOperation.Equals && secondUnaryItem.Operation != UnaryMatchOperation.Equals)
            return -1;
        if (firstUnaryItem.Operation != UnaryMatchOperation.Equals && secondUnaryItem.Operation == UnaryMatchOperation.Equals)
            return 1;
        if (firstUnaryItem.Operation == UnaryMatchOperation.Between && secondUnaryItem.Operation != UnaryMatchOperation.Between)
            return -1;
        if (firstUnaryItem.Operation != UnaryMatchOperation.Between && secondUnaryItem.Operation == UnaryMatchOperation.Between)
            return 1;
        if (firstUnaryItem.Operation == UnaryMatchOperation.Between && secondUnaryItem.Operation == UnaryMatchOperation.Between)
            return firstUnaryItem.Count.CompareTo(secondUnaryItem.Count);

        return firstUnaryItem.Count.CompareTo(secondUnaryItem.Count);
    }

    public new bool IsBoosting => Boosting.HasValue;
}

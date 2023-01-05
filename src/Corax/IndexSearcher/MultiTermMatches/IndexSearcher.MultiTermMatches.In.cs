using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Queries;
using Corax.Utils;

namespace Corax;

public partial class IndexSearcher
{
    /// <summary>
    /// Test API only
    /// </summary>
    public MultiTermMatch InQuery(string field, List<string> inTerms) => InQuery(FieldMetadataBuilder(field), inTerms);
    
    public MultiTermMatch InQuery(in FieldMetadata field, List<string> inTerms)
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
        {
            // If either the term or the field does not exist the request will be empty. 
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);
        }

        if (inTerms.Count is > 1 and <= 4)
        {
            var stack = new BinaryMatch[inTerms.Count / 2];
            for (int i = 0; i < inTerms.Count / 2; i++)
                stack[i] = Or(TermQuery(field, inTerms[i * 2], terms), TermQuery(field, inTerms[i * 2 + 1], terms));

            if (inTerms.Count % 2 == 1)
            {
                // We need even values to make the last work. 
                stack[^1] = Or(stack[^1], TermQuery(field, inTerms[^1], terms));
            }

            int currentTerms = stack.Length;
            while (currentTerms > 1)
            {
                int termsToProcess = currentTerms / 2;
                int excessTerms = currentTerms % 2;

                for (int i = 0; i < termsToProcess; i++)
                    stack[i] = Or(stack[i * 2], stack[i * 2 + 1]);

                if (excessTerms != 0)
                    stack[termsToProcess - 1] = Or(stack[termsToProcess - 1], stack[currentTerms - 1]);

                currentTerms = termsToProcess;
            }

            return MultiTermMatch.Create(stack[0]);
        }

        return MultiTermMatch.Create(new MultiTermMatch<InTermProvider>(field, _transaction.Allocator, new InTermProvider(this, field, inTerms)));
    }

    public IQueryMatch AllInQuery(string field, HashSet<string> allInTerms) => AllInQuery(FieldMetadataBuilder(field), allInTerms);
    //Unlike the In operation, this one requires us to check all entries in a given entry.
    //However, building a query with And can quickly lead to a Stackoverflow Exception.
    //In this case, when we get more conditions, we have to quit building the tree and manually check the entries with UnaryMatch.
    public IQueryMatch AllInQuery(FieldMetadata field, HashSet<string> allInTerms)
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);

        if (terms == null)
        {
            // If either the term or the field does not exist the request will be empty. 
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);
        }

        //TODO PERF
        //Since comparing lists is expensive, we will try to reduce the set of likely candidates as much as possible.
        //Therefore, we check the density of elements present in the tree.
        //In the future, this can be optimized by adding some values at which it makes sense to skip And and go directly into checking.
        TermQueryItem[] list = new TermQueryItem[allInTerms.Count];
        var it = 0;
        foreach (var item in allInTerms)
        {
            var itemSlice = EncodeAndApplyAnalyzer(field, item);
            var amount = TermAmount(terms, itemSlice);
            if (amount == 0)
            {
                return MultiTermMatch.CreateEmpty(_transaction.Allocator);
            }

            list[it++] = new TermQueryItem(itemSlice, amount);
        }


        //Sort by density.
        Array.Sort(list, (tuple, valueTuple) => tuple.Density.CompareTo(valueTuple.Density));

        var allInTermsCount = (allInTerms.Count % 16);
        var stack = new BinaryMatch[allInTermsCount / 2];
        for (int i = 0; i < allInTermsCount / 2; i++)
        {
            var term1 = TermQuery(field, terms, list[i * 2].Item.Span);
            var term2 = TermQuery(field, terms, list[i * 2 + 1].Item.Span);
            stack[i] = And(term1, term2);
        }

        if (allInTermsCount % 2 == 1)
        {
            // We need even values to make the last work. 
            var term = TermQuery(field, terms, list[^1].Item.Span);
            stack[^1] = And(stack[^1], term);
        }

        int currentTerms = stack.Length;
        while (currentTerms > 1)
        {
            int termsToProcess = currentTerms / 2;
            int excessTerms = currentTerms % 2;

            for (int i = 0; i < termsToProcess; i++)
                stack[i] = And(stack[i * 2], stack[i * 2 + 1]);

            if (excessTerms != 0)
                stack[termsToProcess - 1] = And(stack[termsToProcess - 1], stack[currentTerms - 1]);

            currentTerms = termsToProcess;
        }


        //Just perform normal And.
        if (allInTerms.Count is > 1 and <= 16)
            return MultiTermMatch.Create(stack[0]);


        //We don't have to check previous items. We have to check if those entries contain the rest of them.
        list = list[16..];

        //BinarySearch requires sorted array.
        Array.Sort(list, ((item, inItem) => item.Item.Span.SequenceCompareTo(inItem.Item.Span)));
        return UnaryQuery(stack[0], field, list, UnaryMatchOperation.AllIn, -1);
    }

    // public MultiTermMatch InQuery<TScoreFunction>(FieldMetadata field, List<string> inTerms, TScoreFunction scoreFunction)
    //     where TScoreFunction : IQueryScoreFunction
    // {
    //     var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
    //     if (terms == null)
    //     {
    //         // If either the term or the field does not exist the request will be empty. 
    //         return MultiTermMatch.CreateEmpty(_transaction.Allocator);
    //     }
    //
    //     if (inTerms.Count is > 1 and <= 4)
    //     {
    //         var stack = new BinaryMatch[inTerms.Count / 2];
    //         for (int i = 0; i < inTerms.Count / 2; i++)
    //         {
    //             var term1 = Boost(TermQuery(field, inTerms[i * 2], terms), scoreFunction);
    //             var term2 = Boost(TermQuery(field, inTerms[i * 2 + 1], terms), scoreFunction);
    //             stack[i] = Or(term1, term2);
    //         }
    //
    //         if (inTerms.Count % 2 == 1)
    //         {
    //             // We need even values to make the last work. 
    //             var term = Boost(TermQuery(field, inTerms[^1], terms), scoreFunction);
    //             stack[^1] = Or(stack[^1], term);
    //         }
    //
    //         int currentTerms = stack.Length;
    //         while (currentTerms > 1)
    //         {
    //             int termsToProcess = currentTerms / 2;
    //             int excessTerms = currentTerms % 2;
    //
    //             for (int i = 0; i < termsToProcess; i++)
    //                 stack[i] = Or(stack[i * 2], stack[i * 2 + 1]);
    //
    //             if (excessTerms != 0)
    //                 stack[termsToProcess - 1] = Or(stack[termsToProcess - 1], stack[currentTerms - 1]);
    //
    //             currentTerms = termsToProcess;
    //         }
    //
    //         return MultiTermMatch.Create(stack[0]);
    //     }
    //
    //     return MultiTermMatch.Create(
    //         MultiTermBoostingMatch<InTermProvider>.Create(
    //             this, new InTermProvider(this, field, inTerms), scoreFunction));
    // }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public AndNotMatch NotInQuery<TInner>(FieldMetadata field, TInner inner, List<string> notInTerms)
        where TInner : IQueryMatch
    {
        return AndNot(inner, MultiTermMatch.Create(new MultiTermMatch<InTermProvider>(field, _transaction.Allocator, new InTermProvider(this, field, notInTerms))));
    }

    // [MethodImpl(MethodImplOptions.AggressiveInlining)]
    // public AndNotMatch NotInQuery<TScoreFunction, TInner>(FieldMetadata field, TInner inner, List<string> notInTerms, TScoreFunction scoreFunction)
    //     where TScoreFunction : IQueryScoreFunction
    //     where TInner : IQueryMatch
    // {
    //     return AndNot(inner, MultiTermMatch.Create(
    //         MultiTermBoostingMatch<InTermProvider>.Create(
    //             this, new InTermProvider(this, field, notInTerms), scoreFunction)));
    // }
}

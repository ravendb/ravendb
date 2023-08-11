using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Queries;
using Corax.Queries.TermProviders;
using Corax.Utils;
using Voron;

namespace Corax;

public partial class IndexSearcher
{
    /// <summary>
    /// Test API only
    /// </summary>
    public MultiTermMatch InQuery(string field, List<string> inTerms) => InQuery(FieldMetadataBuilder(field), inTerms);

    public MultiTermMatch InQuery(in FieldMetadata field, List<string> inTerms) => InQuery<string>(in field, inTerms);
    
    private MultiTermMatch InQuery<TTermType>(in FieldMetadata field, List<TTermType> inTerms)
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
            {
                if (typeof(TTermType) == typeof(string))
                    stack[i] = Or(TermQuery(field, (string)(object)inTerms[i * 2], terms), TermQuery(field, (string)(object)inTerms[i * 2 + 1], terms));
                else 
                    stack[i] = Or(TermQuery(field, (Slice)(object)inTerms[i * 2], terms), TermQuery(field, (Slice)(object)inTerms[i * 2 + 1], terms));
                    
            }

            if (inTerms.Count % 2 == 1)
            {
                // We need even values to make the last work. 
                if (typeof(TTermType) == typeof(string))
                    stack[^1] = Or(stack[^1], TermQuery(field, (string)(object)inTerms[^1], terms));
                else
                    stack[^1] = Or(stack[^1], TermQuery(field, (Slice)(object)inTerms[^1], terms));
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

        return MultiTermMatch.Create(new MultiTermMatch<InTermProvider<TTermType>>(this, field, _transaction.Allocator, new InTermProvider<TTermType>(this, field, inTerms), streamingEnabled: false));
    }

    public IQueryMatch AllInQuery(string field, HashSet<string> allInTerms) => AllInQuery(FieldMetadataBuilder(field), allInTerms);

    public IQueryMatch AllInQuery(in FieldMetadata field, HashSet<string> allInTerms, bool skipEmptyItems = false) =>
        AllInQuery<string>(field, allInTerms, skipEmptyItems);

    public IQueryMatch AllInQuery(in FieldMetadata field, HashSet<Slice> allInTerms, bool skipEmptyItems = false) => AllInQuery<Slice>(field, allInTerms, skipEmptyItems);

    //Unlike the In operation, this one requires us to check all entries in a given entry.
    //However, building a query with And can quickly lead to a Stackoverflow Exception.
    //In this case, when we get more conditions, we have to quit building the tree and manually check the entries with UnaryMatch.
    private IQueryMatch AllInQuery<TTerm>(in FieldMetadata field, HashSet<TTerm> allInTerms, bool skipEmptyItems = false)
    {
        const int MaximumTermMatchesHandledAsTermMatches = 4;
        
        var canUseUnaryMatch = field.HasBoost == false;
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

        var termCount = 0;
        var llt = _transaction.LowLevelTransaction;
        foreach (var item in allInTerms)
        {
            Slice itemSlice = typeof(TTerm) == typeof(string) ? EncodeAndApplyAnalyzer(field, (string)(object)item) : (Slice)(object)item;
            if (itemSlice.Size == 0 && skipEmptyItems)
                continue;

            var amount = NumberOfDocumentsUnderSpecificTerm(terms, itemSlice);
            if (amount == 0)
            {
                return MultiTermMatch.CreateEmpty(_transaction.Allocator);
            }

            var itemKey = llt.AcquireCompactKey();
            itemKey.Set(itemSlice.AsReadOnlySpan());
            list[termCount++] = new TermQueryItem(itemKey, amount);
        }


        //Sort by density descending. Avoid calling read on biggest multiple times.
        Array.Sort(list[..termCount], (tuple, valueTuple) => valueTuple.Density.CompareTo(tuple.Density));
        
        
        
        //UnaryMatch doesn't support boosting, when we wants to calculate ranking we've to build query like And(Term, And(...)).
        var termCountToProceed = canUseUnaryMatch
            ? (termCount % MaximumTermMatchesHandledAsTermMatches)
            : termCount;
        
        var binaryMatchOfTermMatches = new BinaryMatch[termCountToProceed / 2];
        for (int i = 0; i < termCountToProceed / 2; i++)
        {
            var term1 = TermQuery(field, list[i * 2].Item, terms);
            var term2 = TermQuery(field, list[i * 2 + 1].Item, terms);
            binaryMatchOfTermMatches[i] = And(term1, term2);
        }

        if (termCountToProceed % 2 == 1)
        {
            // We need even values to make the last work. 
            var term = TermQuery(field, list[^1].Item, terms);
            binaryMatchOfTermMatches[^1] = And(binaryMatchOfTermMatches[^1], term);
        }

        
        
        int currentTerms = binaryMatchOfTermMatches.Length;
        while (currentTerms > 1)
        {
            int termsToProcess = currentTerms / 2;
            int excessTerms = currentTerms % 2;

            for (int i = 0; i < termsToProcess; i++)
                binaryMatchOfTermMatches[i] = And(binaryMatchOfTermMatches[i * 2], binaryMatchOfTermMatches[i * 2 + 1]);

            if (excessTerms != 0)
                binaryMatchOfTermMatches[termsToProcess - 1] = And(binaryMatchOfTermMatches[termsToProcess - 1], binaryMatchOfTermMatches[currentTerms - 1]);

            currentTerms = termsToProcess;
        }


        //Just perform normal And.
        if (allInTerms.Count is > 1 and <= MaximumTermMatchesHandledAsTermMatches || canUseUnaryMatch == false)
            return MultiTermMatch.Create(binaryMatchOfTermMatches[0]);


        //We don't have to check previous items. We have to check if those entries contain the rest of them.
        list = list[16..];

        // BinarySearch requires sorted array.
        Array.Sort(list, ((item, inItem) => item.Item.Compare(inItem.Item)));
        return UnaryQuery(binaryMatchOfTermMatches[0], field, list, UnaryMatchOperation.AllIn, -1);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public AndNotMatch NotInQuery<TInner>(FieldMetadata field, TInner inner, List<string> notInTerms) where TInner : IQueryMatch
    {
        return AndNot(inner, MultiTermMatch.Create(new MultiTermMatch<InTermProvider<string>>(this, field, _transaction.Allocator, new InTermProvider<string>(this, field, notInTerms), streamingEnabled: false)));
    }
}

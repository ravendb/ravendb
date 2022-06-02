using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Queries;
using Corax.Utils;
using Sparrow;
using Voron;
using Sparrow.Server;
using Voron.Data.CompactTrees;

namespace Corax;

public partial class IndexSearcher
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch StartWithQuery(Slice field, Slice startWith, bool isNegated = false, int fieldId = Constants.IndexSearcher.NonAnalyzer)
    {
        return MultiTermMatchBuilder<NullScoreFunction, StartWithTermProvider>(field, startWith, default, isNegated, fieldId);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch StartWithQuery(string field, string startWith, bool isNegated = false, int fieldId = Constants.IndexSearcher.NonAnalyzer)
    {
        return MultiTermMatchBuilder<NullScoreFunction, StartWithTermProvider>(field, startWith, default, isNegated, fieldId);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch StartWithQuery<TScoreFunction>(string field, string startWith, TScoreFunction scoreFunction, bool isNegated = false,
        int fieldId = Constants.IndexSearcher.NonAnalyzer)
        where TScoreFunction : IQueryScoreFunction
    {
        return MultiTermMatchBuilder<TScoreFunction, StartWithTermProvider>(field, startWith, scoreFunction, isNegated, fieldId);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch StartWithQuery<TScoreFunction>(Slice field, Slice startWith, TScoreFunction scoreFunction, bool isNegated = false,
        int fieldId = Constants.IndexSearcher.NonAnalyzer)
        where TScoreFunction : IQueryScoreFunction
    {
        return MultiTermMatchBuilder<TScoreFunction, StartWithTermProvider>(field, startWith, scoreFunction, isNegated, fieldId);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch EndsWithQuery(string field, string endsWith, bool isNegated = false,
        int fieldId = Constants.IndexSearcher.NonAnalyzer)
    {
        return MultiTermMatchBuilder<NullScoreFunction, EndsWithTermProvider>(field, endsWith, default, isNegated, fieldId);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch EndsWithQuery<TScoreFunction>(string field, string endsWith, TScoreFunction scoreFunction, bool isNegated = false,
        int fieldId = Constants.IndexSearcher.NonAnalyzer)
        where TScoreFunction : IQueryScoreFunction
    {
        return MultiTermMatchBuilder<TScoreFunction, EndsWithTermProvider>(field, endsWith, scoreFunction, isNegated, fieldId);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch EndsWithQuery(Slice field, Slice endsWith, bool isNegated = false,
        int fieldId = Constants.IndexSearcher.NonAnalyzer)
    {
        return MultiTermMatchBuilder<NullScoreFunction, EndsWithTermProvider>(field, endsWith, default, isNegated, fieldId);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch EndsWithQuery<TScoreFunction>(Slice field, Slice endsWith, TScoreFunction scoreFunction, bool isNegated = false,
        int fieldId = Constants.IndexSearcher.NonAnalyzer)
        where TScoreFunction : IQueryScoreFunction
    {
        return MultiTermMatchBuilder<TScoreFunction, EndsWithTermProvider>(field, endsWith, scoreFunction, isNegated, fieldId);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ContainsQuery<TScoreFunction>(string field, string containsTerm, TScoreFunction scoreFunction, bool isNegated = false,
        int fieldId = Constants.IndexSearcher.NonAnalyzer)
        where TScoreFunction : IQueryScoreFunction
    {
        return MultiTermMatchBuilder<TScoreFunction, ContainsTermProvider>(field, containsTerm, scoreFunction, isNegated, fieldId);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ContainsQuery(string field, string containsTerm, bool isNegated = false,
        int fieldId = Constants.IndexSearcher.NonAnalyzer)
    {
        return MultiTermMatchBuilder<NullScoreFunction, ContainsTermProvider>(field, containsTerm, default, isNegated, fieldId);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ContainsQuery<TScoreFunction>(Slice field, Slice containsTerm, TScoreFunction scoreFunction, bool isNegated = false,
        int fieldId = Constants.IndexSearcher.NonAnalyzer)
        where TScoreFunction : IQueryScoreFunction
    {
        return MultiTermMatchBuilder<TScoreFunction, ContainsTermProvider>(field, containsTerm, scoreFunction, isNegated, fieldId);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ContainsQuery(Slice field, Slice containsTerm, bool isNegated = false,
        int fieldId = Constants.IndexSearcher.NonAnalyzer)
    {
        return MultiTermMatchBuilder<NullScoreFunction, ContainsTermProvider>(field, containsTerm, default, isNegated, fieldId);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ExistsQuery(string field)
    {
        return ExistsQuery(field, default(NullScoreFunction));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ExistsQuery<TScoreFunction>(Slice field, TScoreFunction scoreFunction)
        where TScoreFunction : IQueryScoreFunction
    {
        return MultiTermMatchBuilder<TScoreFunction, ExistsTermProvider>(field, default, scoreFunction, false, Constants.IndexSearcher.NonAnalyzer);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ExistsQuery(Slice field)
    {
        return ExistsQuery(field, default(NullScoreFunction));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ExistsQuery<TScoreFunction>(string field, TScoreFunction scoreFunction)
        where TScoreFunction : IQueryScoreFunction
    {
        return MultiTermMatchBuilder<TScoreFunction, ExistsTermProvider>(field, null, scoreFunction, false, Constants.IndexSearcher.NonAnalyzer);
    }

    public MultiTermMatch InQuery(string field, List<string> inTerms, int fieldId = Constants.IndexSearcher.NonAnalyzer)
    {
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        var terms = fields?.CompactTreeFor(field);
        if (terms == null)
        {
            // If either the term or the field does not exist the request will be empty. 
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);
        }

        if (inTerms.Count is > 1 and <= 16)
        {
            var stack = new BinaryMatch[inTerms.Count / 2];
            for (int i = 0; i < inTerms.Count / 2; i++)
                stack[i] = Or(TermQuery(terms, inTerms[i * 2], fieldId), TermQuery(terms, inTerms[i * 2 + 1], fieldId));

            if (inTerms.Count % 2 == 1)
            {
                // We need even values to make the last work. 
                stack[^1] = Or(stack[^1], TermQuery(terms, inTerms[^1], fieldId));
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

        return MultiTermMatch.Create(new MultiTermMatch<InTermProvider>(_transaction.Allocator, new InTermProvider(this, field, inTerms, fieldId)));
    }
    
    //Unlike the In operation, this one requires us to check all entries in a given entry.
    //However, building a query with And can quickly lead to a Stackoverflow Exception.
    //In this case, when we get more conditions, we have to quit building the tree and manually check the entries with UnaryMatch.
    public IQueryMatch AllInQuery(string field, HashSet<string> allInTerms, int fieldId = Constants.IndexSearcher.NonAnalyzer)
    {
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        var terms = fields?.CompactTreeFor(field);

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
            var itemSlice = EncodeAndApplyAnalyzer(item, fieldId);
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
            var term1 = TermQuery(terms, list[i * 2].Item.Span, fieldId);
            var term2 = TermQuery(terms, list[i * 2 + 1].Item.Span, fieldId);
            stack[i] = And(term1, term2);
        }

        if (allInTermsCount % 2 == 1)
        {
            // We need even values to make the last work. 
            var term = TermQuery(terms, list[^1].Item.Span, fieldId);
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
        return UnaryQuery(stack[0], fieldId, list, UnaryMatchOperation.AllIn, -1);
    }

    public MultiTermMatch InQuery<TScoreFunction>(string field, List<string> inTerms, TScoreFunction scoreFunction, int fieldId = Constants.IndexSearcher.NonAnalyzer)
        where TScoreFunction : IQueryScoreFunction
    {
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        var terms = fields?.CompactTreeFor(field);
        if (terms == null)
        {
            // If either the term or the field does not exist the request will be empty. 
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);
        }

        if (inTerms.Count is > 1 and <= 16)
        {
            var stack = new BinaryMatch[inTerms.Count / 2];
            for (int i = 0; i < inTerms.Count / 2; i++)
            {
                var term1 = Boost(TermQuery(terms, inTerms[i * 2], fieldId), scoreFunction);
                var term2 = Boost(TermQuery(terms, inTerms[i * 2 + 1], fieldId), scoreFunction);
                stack[i] = Or(term1, term2);
            }

            if (inTerms.Count % 2 == 1)
            {
                // We need even values to make the last work. 
                var term = Boost(TermQuery(terms, inTerms[^1], fieldId), scoreFunction);
                stack[^1] = Or(stack[^1], term);
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

        return MultiTermMatch.Create(
            MultiTermBoostingMatch<InTermProvider>.Create(
                this, new InTermProvider(this, field, inTerms, fieldId), scoreFunction));
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public AndNotMatch NotInQuery<TInner>(string field, TInner inner, List<string> notInTerms, int fieldId)
        where TInner : IQueryMatch
    {
        return AndNot(inner, MultiTermMatch.Create(new MultiTermMatch<InTermProvider>(_transaction.Allocator, new InTermProvider(this, field, notInTerms, fieldId))));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public AndNotMatch NotInQuery<TScoreFunction, TInner>(string field, TInner inner, List<string> notInTerms, int fieldId, TScoreFunction scoreFunction)
        where TScoreFunction : IQueryScoreFunction
        where TInner : IQueryMatch
    {
        return AndNot(inner, MultiTermMatch.Create(
            MultiTermBoostingMatch<InTermProvider>.Create(
                this, new InTermProvider(this, field, notInTerms, fieldId), scoreFunction)));
    }


    private MultiTermMatch MultiTermMatchBuilder<TScoreFunction, TTermProvider>(Slice fieldName, Slice term, TScoreFunction scoreFunction, bool isNegated, int fieldId)
        where TScoreFunction : IQueryScoreFunction
        where TTermProvider : ITermProvider
    {
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        var terms = fields?.CompactTreeFor(fieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        return MultiTermMatchBuilderBase<TScoreFunction, TTermProvider>(fieldName, terms, term, scoreFunction, isNegated, fieldId);

    }
    
    private MultiTermMatch MultiTermMatchBuilder<TScoreFunction, TTermProvider>(string field, string term, TScoreFunction scoreFunction, bool isNegated, int fieldId)
        where TScoreFunction : IQueryScoreFunction
        where TTermProvider : ITermProvider
    {
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        using var _ = Slice.From(Allocator, field, ByteStringType.Immutable, out var fieldName);

        var terms = fields?.CompactTreeFor(field);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);
        var slicedTerm = EncodeAndApplyAnalyzer(term, fieldId);
        
        return MultiTermMatchBuilderBase<TScoreFunction, TTermProvider>(fieldName, terms, slicedTerm, scoreFunction, isNegated, fieldId);
    }

    private MultiTermMatch MultiTermMatchBuilderBase<TScoreFunction, TTermProvider>(Slice fieldName, CompactTree terms, Slice slicedTerm, TScoreFunction scoreFunction, bool isNegated, int fieldId)
        where TScoreFunction : IQueryScoreFunction
        where TTermProvider : ITermProvider
    {
        if (typeof(TTermProvider) == typeof(StartWithTermProvider))
        {
            return (isNegated, scoreFunction) switch
            {
                (false, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<StartWithTermProvider>(_transaction.Allocator,
                    new StartWithTermProvider(this, _transaction.Allocator, terms, fieldName, fieldId, slicedTerm))),

                (true, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<NotStartWithTermProvider>(_transaction.Allocator,
                    new NotStartWithTermProvider(this, _transaction.Allocator, terms, fieldName, fieldId, slicedTerm))),

                (false, _) => MultiTermMatch.Create(
                    MultiTermBoostingMatch<StartWithTermProvider>.Create(
                        this, new StartWithTermProvider(this, _transaction.Allocator, terms, fieldName, fieldId, slicedTerm), scoreFunction)),

                (true, _) => MultiTermMatch.Create(
                    MultiTermBoostingMatch<NotStartWithTermProvider>.Create(
                        this, new NotStartWithTermProvider(this, _transaction.Allocator, terms, fieldName, fieldId, slicedTerm), scoreFunction))
            };
        }

        if (typeof(TTermProvider) == typeof(EndsWithTermProvider))
        {
            return (isNegated, scoreFunction) switch
            {
                (false, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<EndsWithTermProvider>(_transaction.Allocator,
                    new EndsWithTermProvider(this, _transaction.Allocator, terms, fieldName, fieldId, slicedTerm))),

                (true, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<NotEndsWithTermProvider>(_transaction.Allocator,
                    new NotEndsWithTermProvider(this, _transaction.Allocator, terms, fieldName, fieldId, slicedTerm))),

                (false, _) => MultiTermMatch.Create(
                    MultiTermBoostingMatch<EndsWithTermProvider>.Create(
                        this, new EndsWithTermProvider(this, _transaction.Allocator, terms, fieldName, fieldId, slicedTerm), scoreFunction)),

                (true, _) => MultiTermMatch.Create(
                    MultiTermBoostingMatch<NotEndsWithTermProvider>.Create(
                        this, new NotEndsWithTermProvider(this, _transaction.Allocator, terms, fieldName, fieldId, slicedTerm), scoreFunction))
            };
        }

        if (typeof(TTermProvider) == typeof(ContainsTermProvider))
        {
            return (isNegated, scoreFunction) switch
            {
                (false, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<ContainsTermProvider>(_transaction.Allocator,
                    new ContainsTermProvider(this, _transaction.Allocator, terms, fieldName, fieldId, slicedTerm))),

                (true, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<NotContainsTermProvider>(_transaction.Allocator,
                    new NotContainsTermProvider(this, _transaction.Allocator, terms, fieldName, fieldId, slicedTerm))),

                (false, _) => MultiTermMatch.Create(
                    MultiTermBoostingMatch<ContainsTermProvider>.Create(
                        this, new ContainsTermProvider(this, _transaction.Allocator, terms, fieldName, fieldId, slicedTerm), scoreFunction)),

                (true, _) => MultiTermMatch.Create(
                    MultiTermBoostingMatch<NotContainsTermProvider>.Create(
                        this, new NotContainsTermProvider(this, _transaction.Allocator, terms, fieldName, fieldId, slicedTerm), scoreFunction))
            };
        }

        if (typeof(TTermProvider) == typeof(ExistsTermProvider))
        {
            if (typeof(TScoreFunction) == typeof(NullScoreFunction))
                return MultiTermMatch.Create(new MultiTermMatch<ExistsTermProvider>(_transaction.Allocator,
                    new ExistsTermProvider(this, _transaction.Allocator, terms, fieldName)));

            return MultiTermMatch.Create(
                MultiTermBoostingMatch<ExistsTermProvider>.Create(
                    this, new ExistsTermProvider(this, _transaction.Allocator, terms, fieldName), scoreFunction));
        }

        return MultiTermMatch.CreateEmpty(_transaction.Allocator);
    }
}

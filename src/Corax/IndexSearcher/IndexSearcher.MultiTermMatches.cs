using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Corax.Pipeline;
using Corax.Queries;
using Voron;

namespace Corax;

public partial class IndexSearcher
{
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
    public MultiTermMatch ExistsQuery(string field)
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
        // TODO: The IEnumerable<string> will die eventually, this is for prototyping only. 
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        if (fields is null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        var terms = fields.CompactTreeFor(field);

        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

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

    public MultiTermMatch InQuery<TScoreFunction>(string field, List<string> inTerms, TScoreFunction scoreFunction, int fieldId = Constants.IndexSearcher.NonAnalyzer)
        where TScoreFunction : IQueryScoreFunction
    {
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        var terms = fields.CompactTreeFor(field);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

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

    private MultiTermMatch MultiTermMatchBuilder<TScoreFunction, TTermProvider>(string field, string term, TScoreFunction scoreFunction, bool isNegated,
        int fieldId)
        where TScoreFunction : IQueryScoreFunction
        where TTermProvider : ITermProvider
    {
        // TODO: The IEnumerable<string> will die eventually, this is for prototyping only. 
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        var terms = fields.CompactTreeFor(field);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        var slicedTerm = EncodeAndApplyAnalyzer(term, fieldId);
        if (typeof(TTermProvider) == typeof(StartWithTermProvider))
        {
            return (isNegated, scoreFunction) switch
            {
                (false, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<StartWithTermProvider>(_transaction.Allocator,
                    new StartWithTermProvider(this, _transaction.Allocator, terms, field, fieldId, slicedTerm))),

                (true, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<NotStartWithTermProvider>(_transaction.Allocator,
                    new NotStartWithTermProvider(this, _transaction.Allocator, terms, field, fieldId, slicedTerm))),

                (false, _) => MultiTermMatch.Create(
                    MultiTermBoostingMatch<StartWithTermProvider>.Create(
                        this, new StartWithTermProvider(this, _transaction.Allocator, terms, field, fieldId, slicedTerm), scoreFunction)),

                (true, _) => MultiTermMatch.Create(
                    MultiTermBoostingMatch<NotStartWithTermProvider>.Create(
                        this, new NotStartWithTermProvider(this, _transaction.Allocator, terms, field, fieldId, slicedTerm), scoreFunction))
            };
        }

        if (typeof(TTermProvider) == typeof(EndsWithTermProvider))
        {
            return (isNegated, scoreFunction) switch
            {
                (false, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<EndsWithTermProvider>(_transaction.Allocator,
                    new EndsWithTermProvider(this, _transaction.Allocator, terms, field, fieldId, slicedTerm))),

                (true, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<NotEndsWithTermProvider>(_transaction.Allocator,
                    new NotEndsWithTermProvider(this, _transaction.Allocator, terms, field, fieldId, slicedTerm))),

                (false, _) => MultiTermMatch.Create(
                    MultiTermBoostingMatch<EndsWithTermProvider>.Create(
                        this, new EndsWithTermProvider(this, _transaction.Allocator, terms, field, fieldId, slicedTerm), scoreFunction)),

                (true, _) => MultiTermMatch.Create(
                    MultiTermBoostingMatch<NotEndsWithTermProvider>.Create(
                        this, new NotEndsWithTermProvider(this, _transaction.Allocator, terms, field, fieldId, slicedTerm), scoreFunction))
            };
        }

        if (typeof(TTermProvider) == typeof(ContainsTermProvider))
        {
            return (isNegated, scoreFunction) switch
            {
                (false, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<ContainsTermProvider>(_transaction.Allocator,
                    new ContainsTermProvider(this, _transaction.Allocator, terms, field, fieldId, slicedTerm))),

                (true, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<NotContainsTermProvider>(_transaction.Allocator,
                    new NotContainsTermProvider(this, _transaction.Allocator, terms, field, fieldId, slicedTerm))),

                (false, _) => MultiTermMatch.Create(
                    MultiTermBoostingMatch<ContainsTermProvider>.Create(
                        this, new ContainsTermProvider(this, _transaction.Allocator, terms, field, fieldId, slicedTerm), scoreFunction)),

                (true, _) => MultiTermMatch.Create(
                    MultiTermBoostingMatch<NotContainsTermProvider>.Create(
                        this, new NotContainsTermProvider(this, _transaction.Allocator, terms, field, fieldId, slicedTerm), scoreFunction))
            };
        }

        if (typeof(TTermProvider) == typeof(ExistsTermProvider))
        {
            if (typeof(TScoreFunction) == typeof(NullScoreFunction))
                return MultiTermMatch.Create(new MultiTermMatch<ExistsTermProvider>(_transaction.Allocator,
                    new ExistsTermProvider(this, _transaction.Allocator, terms, field)));

            return MultiTermMatch.Create(
                MultiTermBoostingMatch<ExistsTermProvider>.Create(
                    this, new ExistsTermProvider(this, _transaction.Allocator, terms, field), scoreFunction));
        }

        return MultiTermMatch.CreateEmpty(_transaction.Allocator);
    }
}

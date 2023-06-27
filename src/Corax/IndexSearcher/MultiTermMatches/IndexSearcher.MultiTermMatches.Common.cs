using Corax.Mappings;
using Corax.Queries;
using Voron;
using Voron.Data.CompactTrees;
using static Voron.Data.CompactTrees.CompactTree;
using Voron.Data.Lookups;

namespace Corax;

public partial class IndexSearcher
{
    private MultiTermMatch MultiTermMatchBuilder<TTermProvider>(FieldMetadata field, Slice term,  bool isNegated)
        where TTermProvider : ITermProvider
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        CompactKey termKey;
        if (term.Size != 0)
        {
            termKey = _fieldsTree.Llt.AcquireCompactKey();
            termKey.Set(term.AsReadOnlySpan());
        }
        else
        {
            termKey = null;
        }
        
        return MultiTermMatchBuilderBase<TTermProvider>(field, terms, termKey, isNegated);
    }

    private MultiTermMatch MultiTermMatchBuilder<TTermProvider>(FieldMetadata field, string term,  bool isNegated)
        where TTermProvider : ITermProvider
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        var slicedTerm = EncodeAndApplyAnalyzer(field, term);

        var termKey = _fieldsTree.Llt.AcquireCompactKey();
        termKey.Set(slicedTerm.AsReadOnlySpan());
        return MultiTermMatchBuilderBase<TTermProvider>(field, terms, termKey, isNegated);
    }

    private MultiTermMatch MultiTermMatchBuilderBase<TTermProvider>(FieldMetadata field, CompactTree termTree, CompactKey term, bool isNegated)
        where TTermProvider : ITermProvider
    {
        if (typeof(TTermProvider) == typeof(StartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
        {
            return (isNegated) switch
            {
                (false) => MultiTermMatch.Create(
                    new MultiTermMatch<StartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(
                        field, _transaction.Allocator,
                        new StartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term))),

                (true) => MultiTermMatch.Create(
                    new MultiTermMatch<NotStartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(
                        field, _transaction.Allocator,
                        new NotStartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term))),
            };
        }

        if (typeof(TTermProvider) == typeof(StartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
        {
            return (isNegated) switch
            {
                (false) => MultiTermMatch.Create(
                    new MultiTermMatch<StartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(
                        field, _transaction.Allocator,
                        new StartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term))),

                (true) => MultiTermMatch.Create(
                    new MultiTermMatch<NotStartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(
                        field, _transaction.Allocator,
                        new NotStartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term))),
            };
        }

        if (typeof(TTermProvider) == typeof(EndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
        {
            return (isNegated) switch
            {
                (false) => MultiTermMatch.Create(new MultiTermMatch<EndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, _transaction.Allocator,
                    new EndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term))),

                (true) => MultiTermMatch.Create(new MultiTermMatch<NotEndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, _transaction.Allocator,
                    new NotEndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term)))
            };
        }

        if (typeof(TTermProvider) == typeof(EndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
        {
            return (isNegated) switch
            {
                (false) => MultiTermMatch.Create(new MultiTermMatch<EndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, _transaction.Allocator,
                    new EndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term))),

                (true) => MultiTermMatch.Create(new MultiTermMatch<NotEndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, _transaction.Allocator,
                    new NotEndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term)))
            };
        }

        if (typeof(TTermProvider) == typeof(ContainsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
        {
            return (isNegated) switch
            {
                (false) => MultiTermMatch.Create(new MultiTermMatch<ContainsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, _transaction.Allocator,
                    new ContainsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term))),

                (true) => MultiTermMatch.Create(new MultiTermMatch<NotContainsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, _transaction.Allocator,
                    new NotContainsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term))),
            };
        }

        if (typeof(TTermProvider) == typeof(ContainsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
        {
            return (isNegated) switch
            {
                (false) => MultiTermMatch.Create(new MultiTermMatch<ContainsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, _transaction.Allocator,
                    new ContainsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term))),

                (true) => MultiTermMatch.Create(new MultiTermMatch<NotContainsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, _transaction.Allocator,
                    new NotContainsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term))),
            };
        }

        if (typeof(TTermProvider) == typeof(ExistsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
        {
            return MultiTermMatch.Create(
                new MultiTermMatch<ExistsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(
                    field, _transaction.Allocator, 
                    new ExistsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field)));
        }

        if (typeof(TTermProvider) == typeof(ExistsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
        {
            return MultiTermMatch.Create(
                new MultiTermMatch<ExistsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(
                    field, _transaction.Allocator, 
                    new ExistsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field)));
        }

        return MultiTermMatch.CreateEmpty(_transaction.Allocator);
    }
}

using Corax.Mappings;
using Corax.Queries;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax;

public partial class IndexSearcher
{
    private MultiTermMatch MultiTermMatchBuilder<TTermProvider>(FieldMetadata field, Slice term,  bool isNegated)
        where TTermProvider : ITermProvider
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        return MultiTermMatchBuilderBase<TTermProvider>(field, terms, term, isNegated);
    }

    private MultiTermMatch MultiTermMatchBuilder<TTermProvider>(FieldMetadata field, string term,  bool isNegated)
        where TTermProvider : ITermProvider
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        var slicedTerm = EncodeAndApplyAnalyzer(field, term);
        return MultiTermMatchBuilderBase<TTermProvider>(field, terms, slicedTerm, isNegated);
    }

    private MultiTermMatch MultiTermMatchBuilderBase<TTermProvider>(FieldMetadata field, CompactTree termTree, Slice term, bool isNegated)
        where TTermProvider : ITermProvider
    {
        if (typeof(TTermProvider) == typeof(StartWithTermProvider))
        {
            return (isNegated) switch
            {
                (false) => MultiTermMatch.Create(new MultiTermMatch<StartWithTermProvider>(field, _transaction.Allocator,
                    new StartWithTermProvider(this, termTree, field, term))),

                (true) => MultiTermMatch.Create(new MultiTermMatch<NotStartWithTermProvider>(field, _transaction.Allocator,
                    new NotStartWithTermProvider(this, _transaction.Allocator, termTree, field, term))),
            };
        }

        if (typeof(TTermProvider) == typeof(EndsWithTermProvider))
        {
            return (isNegated) switch
            {
                (false) => MultiTermMatch.Create(new MultiTermMatch<EndsWithTermProvider>(field, _transaction.Allocator,
                    new EndsWithTermProvider(this, termTree, field, term))),

                (true) => MultiTermMatch.Create(new MultiTermMatch<NotEndsWithTermProvider>(field, _transaction.Allocator,
                    new NotEndsWithTermProvider(this, termTree, field, term)))
            };
        }

        if (typeof(TTermProvider) == typeof(ContainsTermProvider))
        {
            return (isNegated) switch
            {
                (false) => MultiTermMatch.Create(new MultiTermMatch<ContainsTermProvider>(field, _transaction.Allocator,
                    new ContainsTermProvider(this, termTree, field, term))),

                (true) => MultiTermMatch.Create(new MultiTermMatch<NotContainsTermProvider>(field, _transaction.Allocator,
                    new NotContainsTermProvider(this, termTree, field, term))),
            };
        }

        if (typeof(TTermProvider) == typeof(ExistsTermProvider))
        {
            return MultiTermMatch.Create(new MultiTermMatch<ExistsTermProvider>(field, _transaction.Allocator, new ExistsTermProvider(this, termTree, field)));
        }

        return MultiTermMatch.CreateEmpty(_transaction.Allocator);
    }
}

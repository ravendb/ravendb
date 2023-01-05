using Corax.Mappings;
using Corax.Queries;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax;

public partial class IndexSearcher
{
    private MultiTermMatch MultiTermMatchBuilder<TScoreFunction, TTermProvider>(FieldMetadata field, Slice term, TScoreFunction scoreFunction, bool isNegated)
        where TScoreFunction : IQueryScoreFunction
        where TTermProvider : ITermProvider
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        return MultiTermMatchBuilderBase<TScoreFunction, TTermProvider>(field, terms, term, scoreFunction, isNegated);
    }

    private MultiTermMatch MultiTermMatchBuilder<TScoreFunction, TTermProvider>(FieldMetadata field, string term, TScoreFunction scoreFunction, bool isNegated)
        where TScoreFunction : IQueryScoreFunction
        where TTermProvider : ITermProvider
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        var slicedTerm = EncodeAndApplyAnalyzer(field, term);
        return MultiTermMatchBuilderBase<TScoreFunction, TTermProvider>(field, terms, slicedTerm, scoreFunction, isNegated);
    }

    private MultiTermMatch MultiTermMatchBuilderBase<TScoreFunction, TTermProvider>(FieldMetadata field, CompactTree termTree, Slice term, TScoreFunction scoreFunction,
        bool isNegated)
        where TScoreFunction : IQueryScoreFunction
        where TTermProvider : ITermProvider
    {
        if (typeof(TTermProvider) == typeof(StartWithTermProvider))
        {
            return (isNegated, scoreFunction) switch
            {
                (false, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<StartWithTermProvider>(_transaction.Allocator,
                    new StartWithTermProvider(this, termTree, field, term))),

                (true, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<NotStartWithTermProvider>(_transaction.Allocator,
                    new NotStartWithTermProvider(this, _transaction.Allocator, termTree, field, term))),

                (false, _) => MultiTermMatch.Create(
                    MultiTermBoostingMatch<StartWithTermProvider>.Create(
                        this, new StartWithTermProvider(this, termTree, field, term), scoreFunction)),

                (true, _) => MultiTermMatch.Create(
                    MultiTermBoostingMatch<NotStartWithTermProvider>.Create(
                        this, new NotStartWithTermProvider(this, _transaction.Allocator, termTree, field, term), scoreFunction))
            };
        }

        if (typeof(TTermProvider) == typeof(EndsWithTermProvider))
        {
            return (isNegated, scoreFunction) switch
            {
                (false, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<EndsWithTermProvider>(_transaction.Allocator,
                    new EndsWithTermProvider(this, termTree, field, term))),

                (true, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<NotEndsWithTermProvider>(_transaction.Allocator,
                    new NotEndsWithTermProvider(this, termTree, field, term))),

                (false, _) => MultiTermMatch.Create(
                    MultiTermBoostingMatch<EndsWithTermProvider>.Create(
                        this, new EndsWithTermProvider(this, termTree, field, term), scoreFunction)),

                (true, _) => MultiTermMatch.Create(
                    MultiTermBoostingMatch<NotEndsWithTermProvider>.Create(
                        this, new NotEndsWithTermProvider(this, termTree, field, term), scoreFunction))
            };
        }

        if (typeof(TTermProvider) == typeof(ContainsTermProvider))
        {
            return (isNegated, scoreFunction) switch
            {
                (false, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<ContainsTermProvider>(_transaction.Allocator,
                    new ContainsTermProvider(this, termTree, field, term))),

                (true, NullScoreFunction) => MultiTermMatch.Create(new MultiTermMatch<NotContainsTermProvider>(_transaction.Allocator,
                    new NotContainsTermProvider(this, termTree, field, term))),

                (false, _) => MultiTermMatch.Create(
                    MultiTermBoostingMatch<ContainsTermProvider>.Create(
                        this, new ContainsTermProvider(this, termTree, field, term), scoreFunction)),

                (true, _) => MultiTermMatch.Create(
                    MultiTermBoostingMatch<NotContainsTermProvider>.Create(
                        this, new NotContainsTermProvider(this, termTree, field, term), scoreFunction))
            };
        }

        if (typeof(TTermProvider) == typeof(ExistsTermProvider))
        {
            if (typeof(TScoreFunction) == typeof(NullScoreFunction))
                return MultiTermMatch.Create(new MultiTermMatch<ExistsTermProvider>(_transaction.Allocator,
                    new ExistsTermProvider(this, termTree, field)));

            return MultiTermMatch.Create(
                MultiTermBoostingMatch<ExistsTermProvider>.Create(
                    this, new ExistsTermProvider(this, termTree, field), scoreFunction));
        }

        return MultiTermMatch.CreateEmpty(_transaction.Allocator);
    }
}

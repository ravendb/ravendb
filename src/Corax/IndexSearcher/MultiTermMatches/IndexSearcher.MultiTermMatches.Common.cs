using Corax.Queries;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax;

public partial class IndexSearcher
{
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

    private MultiTermMatch MultiTermMatchBuilderBase<TScoreFunction, TTermProvider>(Slice fieldName, CompactTree terms, Slice slicedTerm, TScoreFunction scoreFunction,
        bool isNegated, int fieldId)
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

using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Corax.Queries;
using Sparrow.Server;
using Voron;

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

    public MultiTermMatch RegexQuery<TScoreFunction>(string field, TScoreFunction scoreFunction, Regex regex)
        where TScoreFunction : IQueryScoreFunction
    {
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        var terms = fields?.CompactTreeFor(field);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        return MultiTermMatch.Create(new MultiTermMatch<RegexTermProvider>(_transaction.Allocator,
            new RegexTermProvider(this, terms, field, regex)));
    }
}

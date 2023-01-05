using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Corax.Mappings;
using Corax.Queries;
using Voron;

namespace Corax;

public partial class IndexSearcher
{
    /// <summary>
    /// Test API only
    /// </summary>
    public MultiTermMatch StartWithQuery(string field, string startWith, bool isNegated = false) => StartWithQuery(FieldMetadataBuilder(field), EncodeAndApplyAnalyzer(default, startWith));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch StartWithQuery(FieldMetadata field, Slice startWith, bool isNegated = false)
    {
        return MultiTermMatchBuilder<NullScoreFunction, StartWithTermProvider>(field, startWith, default, isNegated);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch StartWithQuery(FieldMetadata field, string startWith, bool isNegated = false)
    {
        return MultiTermMatchBuilder<NullScoreFunction, StartWithTermProvider>(field, startWith, default, isNegated);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch StartWithQuery<TScoreFunction>(FieldMetadata field, string startWith, TScoreFunction scoreFunction, bool isNegated = false)
        where TScoreFunction : IQueryScoreFunction
    {
        return MultiTermMatchBuilder<TScoreFunction, StartWithTermProvider>(field, startWith, scoreFunction, isNegated);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch StartWithQuery<TScoreFunction>(FieldMetadata field, Slice startWith, TScoreFunction scoreFunction, bool isNegated = false)
        where TScoreFunction : IQueryScoreFunction
    {
        return MultiTermMatchBuilder<TScoreFunction, StartWithTermProvider>(field, startWith, scoreFunction, isNegated);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch EndsWithQuery(FieldMetadata field, string endsWith, bool isNegated = false)
    {
        return MultiTermMatchBuilder<NullScoreFunction, EndsWithTermProvider>(field, endsWith, default, isNegated);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch EndsWithQuery<TScoreFunction>(FieldMetadata field, string endsWith, TScoreFunction scoreFunction, bool isNegated = false,
        int fieldId = Constants.IndexSearcher.NonAnalyzer)
        where TScoreFunction : IQueryScoreFunction
    {
        return MultiTermMatchBuilder<TScoreFunction, EndsWithTermProvider>(field, endsWith, scoreFunction, isNegated);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch EndsWithQuery(FieldMetadata field, Slice endsWith, bool isNegated = false)
    {
        return MultiTermMatchBuilder<NullScoreFunction, EndsWithTermProvider>(field, endsWith, default, isNegated);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch EndsWithQuery<TScoreFunction>(FieldMetadata field, Slice endsWith, TScoreFunction scoreFunction, bool isNegated = false)
        where TScoreFunction : IQueryScoreFunction
    {
        return MultiTermMatchBuilder<TScoreFunction, EndsWithTermProvider>(field, endsWith, scoreFunction, isNegated);
    }

    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ContainsQuery<TScoreFunction>(FieldMetadata field, string containsTerm, TScoreFunction scoreFunction, bool isNegated = false)
        where TScoreFunction : IQueryScoreFunction
    {
        return MultiTermMatchBuilder<TScoreFunction, ContainsTermProvider>(field, containsTerm, scoreFunction, isNegated);
    }

    public MultiTermMatch ContainsQuery(FieldMetadata field, string containsTerm, bool isNegated = false) => ContainsQuery(field, EncodeAndApplyAnalyzer(field, containsTerm));
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ContainsQuery(FieldMetadata field, Slice containsTerm, bool isNegated = false)
    {
        return MultiTermMatchBuilder<NullScoreFunction, ContainsTermProvider>(field, containsTerm, default, isNegated);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ContainsQuery<TScoreFunction>(FieldMetadata field, Slice containsTerm, TScoreFunction scoreFunction, bool isNegated = false)
        where TScoreFunction : IQueryScoreFunction
    {
        return MultiTermMatchBuilder<TScoreFunction, ContainsTermProvider>(field, containsTerm, scoreFunction, isNegated);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ExistsQuery(FieldMetadata field)
    {
        return ExistsQuery(field, default(NullScoreFunction));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ExistsQuery<TScoreFunction>(FieldMetadata field, TScoreFunction scoreFunction)
        where TScoreFunction : IQueryScoreFunction
    {
        return MultiTermMatchBuilder<TScoreFunction, ExistsTermProvider>(field, default(Slice), scoreFunction, false);
    }

    public MultiTermMatch RegexQuery<TScoreFunction>(FieldMetadata field, TScoreFunction scoreFunction, Regex regex)
        where TScoreFunction : IQueryScoreFunction
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        return MultiTermMatch.Create(new MultiTermMatch<RegexTermProvider>(field, _transaction.Allocator,
            new RegexTermProvider(this, terms, field, regex)));
    }
}

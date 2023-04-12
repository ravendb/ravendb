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
    public MultiTermMatch StartWithQuery(string field, string startWith, bool isNegated = false, bool hasBoost = false) => StartWithQuery(FieldMetadataBuilder(field, hasBoost: hasBoost), EncodeAndApplyAnalyzer(default, startWith));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch StartWithQuery(FieldMetadata field, string startWith, bool isNegated = false)
    {
        return MultiTermMatchBuilder<StartWithTermProvider>(field, startWith, isNegated);
    }
    
    public MultiTermMatch StartWithQuery(FieldMetadata field, Slice startWith, bool isNegated = false)
    {
        return MultiTermMatchBuilder<StartWithTermProvider>(field, startWith, isNegated);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch EndsWithQuery(FieldMetadata field, string endsWith, bool isNegated = false)
    {
        return MultiTermMatchBuilder<EndsWithTermProvider>(field, endsWith, isNegated);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch EndsWithQuery(FieldMetadata field, Slice endsWith, bool isNegated = false)
    {
        return MultiTermMatchBuilder<EndsWithTermProvider>(field, endsWith, isNegated);
    }
    
    public MultiTermMatch ContainsQuery(FieldMetadata field, string containsTerm, bool isNegated = false) => ContainsQuery(field, EncodeAndApplyAnalyzer(field, containsTerm), isNegated);
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ContainsQuery(FieldMetadata field, Slice containsTerm, bool isNegated = false)
    {
        return MultiTermMatchBuilder<ContainsTermProvider>(field, containsTerm, isNegated);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ExistsQuery(FieldMetadata field)
    {
        return MultiTermMatchBuilder<ExistsTermProvider>(field, default(Slice), false);
    }

    public MultiTermMatch RegexQuery(FieldMetadata field, Regex regex)
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        return MultiTermMatch.Create(new MultiTermMatch<RegexTermProvider>(field, _transaction.Allocator,
            new RegexTermProvider(this, terms, field, regex)));
    }
}

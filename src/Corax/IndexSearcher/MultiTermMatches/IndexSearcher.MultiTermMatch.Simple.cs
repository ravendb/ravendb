using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Corax.Mappings;
using Corax.Queries;
using Corax.Queries.TermProviders;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using static Voron.Data.CompactTrees.CompactTree;

namespace Corax;

public partial class IndexSearcher
{
    /// <summary>
    /// Test API only
    /// </summary>
    public MultiTermMatch StartWithQuery(string field, string startWith, bool isNegated = false, bool hasBoost = false, bool forward = true) => StartWithQuery(FieldMetadataBuilder(field, hasBoost: hasBoost), EncodeAndApplyAnalyzer(default, startWith), isNegated, forward);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch StartWithQuery(FieldMetadata field, string startWith, bool isNegated = false, bool forward = true, bool streamingEnabled = false)
    {
        return (forward, isNegated) switch
        {
            (true, false) => MultiTermMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, startWith, streamingEnabled),
            (false, false) => MultiTermMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, startWith, streamingEnabled),
            (true, true) => MultiTermMatchBuilder<NotStartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, startWith, streamingEnabled),
            (false, true) => MultiTermMatchBuilder<NotStartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, startWith, streamingEnabled)
        };
    }
    
    public MultiTermMatch StartWithQuery(FieldMetadata field, Slice startWith, bool isNegated = false, bool forward = true, bool streamingEnabled = false)
    {
        return (forward, isNegated) switch
        {
            (true, false) => MultiTermMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, startWith, streamingEnabled),
            (false, false) => MultiTermMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, startWith, streamingEnabled),
            (true, true) => MultiTermMatchBuilder<NotStartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, startWith, streamingEnabled),
            (false, true) => MultiTermMatchBuilder<NotStartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, startWith, streamingEnabled)
        };
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch EndsWithQuery(FieldMetadata field, string endsWith, bool isNegated = false, bool forward = true, bool streamingEnabled = false)
    {
        return (forward, isNegated) switch
        {
            (true, false) => MultiTermMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, endsWith, streamingEnabled),
            (false, false) => MultiTermMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, endsWith, streamingEnabled),
            (true, true) => MultiTermMatchBuilder<NotEndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, endsWith, streamingEnabled),
            (false, true) => MultiTermMatchBuilder<NotEndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, endsWith, streamingEnabled)
        };
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch EndsWithQuery(FieldMetadata field, Slice endsWith, bool isNegated = false, bool forward = true, bool streamingEnabled = false)
    {
        return (forward, isNegated) switch
        {
            (true, false) => MultiTermMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, endsWith, streamingEnabled),
            (false, false) => MultiTermMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, endsWith, streamingEnabled),
            (true, true) => MultiTermMatchBuilder<NotEndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, endsWith, streamingEnabled),
            (false, true) => MultiTermMatchBuilder<NotEndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, endsWith, streamingEnabled)
        };
    }
    
    public MultiTermMatch ContainsQuery(FieldMetadata field, string containsTerm, bool isNegated = false, bool forward = true) => ContainsQuery(field, EncodeAndApplyAnalyzer(field, containsTerm), isNegated, forward);
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ContainsQuery(FieldMetadata field, Slice containsTerm, bool isNegated = false, bool forward = true)
    {
        return (forward, isNegated) switch
        {
            (true, false) => MultiTermMatchBuilder<ContainsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, containsTerm),
            (false, false) => MultiTermMatchBuilder<ContainsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, containsTerm),
            (true, true) => MultiTermMatchBuilder<NotContainsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, containsTerm),
            (false, true) => MultiTermMatchBuilder<NotContainsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, containsTerm)
        };
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ExistsQuery(FieldMetadata field, bool forward = true, bool streamingEnabled = false)
    {
        return forward 
            ? MultiTermMatchBuilder<ExistsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, default(Slice), streamingEnabled: streamingEnabled) 
            : MultiTermMatchBuilder<ExistsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, default(Slice), streamingEnabled: streamingEnabled);
    }

    public MultiTermMatch RegexQuery(FieldMetadata field, Regex regex, bool forward = true, bool streamingEnabled = false)
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        return forward
            ? MultiTermMatch.Create(
                new MultiTermMatch<RegexTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(this,
                    field, _transaction.Allocator,
                    new RegexTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, terms, field, regex), streamingEnabled
                ))
            : MultiTermMatch.Create(
                new MultiTermMatch<RegexTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(this,
                    field, _transaction.Allocator,
                    new RegexTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, terms, field, regex), streamingEnabled
                ));
    }
}

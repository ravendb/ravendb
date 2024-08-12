using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.TermProviders;
using Voron;
using Voron.Data.Lookups;
using static Voron.Data.CompactTrees.CompactTree;

namespace Corax.Querying;

public partial class IndexSearcher
{
    /// <summary>
    /// Test API only
    /// </summary>
    public MultiTermMatch StartWithQuery(string field, string startWith, bool isNegated = false, bool hasBoost = false, bool forward = true) => StartWithQuery(FieldMetadataBuilder(field, hasBoost: hasBoost), EncodeAndApplyAnalyzer(default, startWith), isNegated, forward);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch StartWithQuery(in FieldMetadata field, string startWith, bool isNegated = false, bool forward = true, bool streamingEnabled = false, in CancellationToken token = default)
    {
        return (forward, isNegated) switch
        {
            (true, false) => MultiTermMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, startWith, streamingEnabled, token),
            (false, false) => MultiTermMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, startWith, streamingEnabled, token),
            (true, true) => MultiTermMatchBuilder<NotStartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, startWith, streamingEnabled, token),
            (false, true) => MultiTermMatchBuilder<NotStartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, startWith, streamingEnabled, token)
        };
    }
    
    public MultiTermMatch StartWithQuery(in FieldMetadata field, Slice startWith, bool isNegated = false, bool forward = true, bool streamingEnabled = false, bool validatePostfixLen = false,in CancellationToken token = default)
    {
        return (forward, isNegated) switch
        {
            (true, false) => MultiTermMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, startWith, streamingEnabled, validatePostfixLen, token),
            (false, false) => MultiTermMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, startWith, streamingEnabled, validatePostfixLen, token),
            (true, true) => MultiTermMatchBuilder<NotStartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, startWith, streamingEnabled, validatePostfixLen, token),
            (false, true) => MultiTermMatchBuilder<NotStartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, startWith, streamingEnabled, validatePostfixLen, token)
        };
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch EndsWithQuery(in FieldMetadata field, string endsWith, bool isNegated = false, bool forward = true, bool streamingEnabled = false, in CancellationToken token = default)
    {
        return (forward, isNegated) switch
        {
            (true, false) => MultiTermMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, endsWith, streamingEnabled, token),
            (false, false) => MultiTermMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, endsWith, streamingEnabled, token),
            (true, true) => MultiTermMatchBuilder<NotEndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, endsWith, streamingEnabled, token),
            (false, true) => MultiTermMatchBuilder<NotEndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, endsWith, streamingEnabled, token)
        };
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch EndsWithQuery(in FieldMetadata field, Slice endsWith, bool isNegated = false, bool forward = true, bool streamingEnabled = false, in CancellationToken token = default)
    {
        return (forward, isNegated) switch
        {
            (true, false) => MultiTermMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, endsWith, streamingEnabled, validatePostfixLen: false, token: token),
            (false, false) => MultiTermMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, endsWith, streamingEnabled,  validatePostfixLen: false, token:token),
            (true, true) => MultiTermMatchBuilder<NotEndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, endsWith, streamingEnabled,  validatePostfixLen: false, token:token),
            (false, true) => MultiTermMatchBuilder<NotEndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, endsWith, streamingEnabled,  validatePostfixLen: false, token:token)
        };
    }
    
    public MultiTermMatch ContainsQuery(in FieldMetadata field, string containsTerm, bool isNegated = false, bool forward = true, in CancellationToken token = default) => ContainsQuery(field, (Slice)EncodeAndApplyAnalyzer(field, containsTerm), isNegated, forward, token);
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ContainsQuery(in FieldMetadata field, Slice containsTerm, bool isNegated = false, bool forward = true, in CancellationToken token = default)
    {
        return (forward, isNegated) switch
        {
            (true, false) => MultiTermMatchBuilder<ContainsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, containsTerm, token: token),
            (false, false) => MultiTermMatchBuilder<ContainsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, containsTerm, token: token),
            (true, true) => MultiTermMatchBuilder<NotContainsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, containsTerm, token: token),
            (false, true) => MultiTermMatchBuilder<NotContainsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, containsTerm, token: token)
        };
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ExistsQuery(in FieldMetadata field, bool forward = true, bool streamingEnabled = false, in CancellationToken token = default)
    {
        return forward 
            ? MultiTermMatchBuilder<ExistsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, default(Slice), streamingEnabled: streamingEnabled, token: token) 
            : MultiTermMatchBuilder<ExistsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, default(Slice), streamingEnabled: streamingEnabled, token: token);
    }

    
    public MultiTermMatch RegexQuery(in FieldMetadata field, Regex regex, bool forward = true, bool streamingEnabled = false, in CancellationToken token = default)
    {
        if (_fieldsTree == null || _fieldsTree.TryGetCompactTreeFor(field.FieldName, out var terms) == false)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        return forward
            ? MultiTermMatch.Create(
                new MultiTermMatch<RegexTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(this,
                    field, _transaction.Allocator,
                    new RegexTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, terms, field, regex), streamingEnabled, token: token
                ))
            : MultiTermMatch.Create(
                new MultiTermMatch<RegexTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(this,
                    field, _transaction.Allocator,
                    new RegexTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, terms, field, regex), streamingEnabled, token: token
                ));
    }
}

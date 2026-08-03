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
    public MultiTermMatch StartWithQuery(string field, string startWith, bool hasBoost = false, bool forward = true) => StartWithQuery(FieldMetadataBuilder(field, hasBoost: hasBoost), EncodeAndApplyAnalyzer(default, startWith), forward);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch StartWithQuery(in FieldMetadata field, string startWith, bool forward = true, bool streamingEnabled = false, in CancellationToken token = default)
    {
        return forward
            ? MultiTermMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, startWith, streamingEnabled, token)
            : MultiTermMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, startWith, streamingEnabled, token);
    }

    public MultiTermMatch StartWithQuery(in FieldMetadata field, Slice startWith, bool forward = true, bool streamingEnabled = false, bool validatePostfixLen = false,in CancellationToken token = default)
    {
        return forward
            ? MultiTermMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, startWith, streamingEnabled, validatePostfixLen, token)
            : MultiTermMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, startWith, streamingEnabled, validatePostfixLen, token);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch EndsWithQuery(in FieldMetadata field, string endsWith, bool forward = true, bool streamingEnabled = false, in CancellationToken token = default)
    {
        return forward
            ? MultiTermMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, endsWith, streamingEnabled, token)
            : MultiTermMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, endsWith, streamingEnabled, token);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch EndsWithQuery(in FieldMetadata field, Slice endsWith, bool forward = true, bool streamingEnabled = false, in CancellationToken token = default)
    {
        return forward
            ? MultiTermMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, endsWith, streamingEnabled, validatePostfixLen: false, token: token)
            : MultiTermMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, endsWith, streamingEnabled, validatePostfixLen: false, token: token);
    }
    
    public MultiTermMatch ContainsQuery(in FieldMetadata field, string containsTerm, bool forward = true, in CancellationToken token = default) => ContainsQuery(field, (Slice)EncodeAndApplyAnalyzer(field, containsTerm), forward, token);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public MultiTermMatch ContainsQuery(in FieldMetadata field, Slice containsTerm, bool forward = true, in CancellationToken token = default)
    {
        return forward
            ? MultiTermMatchBuilder<ContainsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, containsTerm, token: token)
            : MultiTermMatchBuilder<ContainsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, containsTerm, token: token);
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
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
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

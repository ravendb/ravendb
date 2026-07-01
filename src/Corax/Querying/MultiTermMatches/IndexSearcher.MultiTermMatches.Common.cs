using System;
using System.Diagnostics;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.TermsProviders;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using static Voron.Data.CompactTrees.CompactTree;

namespace Corax.Querying;

public partial class IndexSearcher
{
    private IQueryMatch TermsProviderMatchBuilder<TTermsProvider>(in FieldMetadata field, Slice term, bool validatePostfixLen = false, in CancellationToken token = default)
        where TTermsProvider : struct, ITermsProvider
    {
        var terms = GetTermsFor(field.FieldName);
        if (terms == null)
            return EmptyQueryMatch.Instance;

        CompactKey termKey;
        if (term.Size != 0)
        {
            termKey = _fieldsTree.Llt.AcquireCompactKey();
            termKey.Set(term.AsReadOnlySpan());
            termKey.ChangeDictionary(terms.DictionaryId);
        }
        else
        {
            termKey = null;
        }

        CompactKey seekTerm = BuildBackwardStartsWithSeekLimit<TTermsProvider>(terms, term.AsReadOnlySpan());
        ITermsProvider provider = GetMultiTermMatchProvider<TTermsProvider>(field, terms, termKey, seekTerm, validatePostfixLen, token);
        return new TermsProviderMatch(provider, _transaction.LowLevelTransaction, _transaction.Allocator, token);
    }

    private IQueryMatch TermsProviderMatchBuilder<TTermsProvider>(in FieldMetadata field, string term, CancellationToken token)
        where TTermsProvider : struct, ITermsProvider
    {
        var terms = GetTermsFor(field.FieldName);
        if (terms == null)
            return EmptyQueryMatch.Instance;

        var slicedTerm = EncodeAndApplyAnalyzer(field, term);
        var termKey = _fieldsTree.Llt.AcquireCompactKey();
        termKey.Set(slicedTerm.AsReadOnlySpan());

        CompactKey seekTerm = BuildBackwardStartsWithSeekLimit<TTermsProvider>(terms, slicedTerm.AsReadOnlySpan());
        ITermsProvider provider = GetMultiTermMatchProvider<TTermsProvider>(field, terms, termKey, seekTerm, validatePostfixLen: false, token: token);
        return new TermsProviderMatch(provider, _transaction.LowLevelTransaction, _transaction.Allocator, token);
    }

    // A backward StartsWith scan must seek to the END of the prefix block and walk down. The seek upper bound is
    // successor(prefix) (the first key past the block); the backward iterator then positions at the last in-prefix
    // key (the provider's _firstRun skip discards the overshoot). Returns null when the prefix has no finite
    // successor (empty / all-0xFF) — the block then runs to the tree end and the provider starts via a Reset. For
    // every other provider (forward StartsWith ignores the limit; Not/Ends/Contains/Exists don't take one) returns
    // null, preserving the previous seekTerm: null behavior.
    private CompactKey BuildBackwardStartsWithSeekLimit<TTermsProvider>(CompactTree terms, ReadOnlySpan<byte> prefix)
        where TTermsProvider : struct, ITermsProvider
    {
        if (typeof(TTermsProvider) != typeof(StartsWithTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>) || prefix.Length == 0)
            return null;

        using var _ = _transaction.Allocator.Allocate(prefix.Length, out Span<byte> successor);
        int len = TryWritePrefixSuccessor(prefix, successor);
        if (len == 0)
            return null; // no finite successor → backward scan starts at the tree end (provider Resets)

        var seekKey = _fieldsTree.Llt.AcquireCompactKey();
        seekKey.Set(successor.Slice(0, len));
        seekKey.ChangeDictionary(terms.DictionaryId);
        return seekKey;
    }

    private TTermsProvider GetMultiTermMatchProvider<TTermsProvider>(in FieldMetadata field, CompactTree termTree, CompactKey term, CompactKey seekTerm, bool validatePostfixLen, CancellationToken token)
        where TTermsProvider : struct, ITermsProvider
    {
        if (typeof(TTermsProvider) == typeof(StartsWithTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermsProvider)(object)new StartsWithTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term, seekTerm, validatePostfixLen, token);

        if (typeof(TTermsProvider) == typeof(StartsWithTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermsProvider)(object)new StartsWithTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term, seekTerm, validatePostfixLen, token);

        if (typeof(TTermsProvider) == typeof(NotStartsWithTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermsProvider)(object)new NotStartsWithTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term, validatePostfixLen, token);

        if (typeof(TTermsProvider) == typeof(NotStartsWithTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermsProvider)(object)new NotStartsWithTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term, validatePostfixLen, token);

        Debug.Assert(validatePostfixLen == false, "Not supported for the rest of this");

        if (typeof(TTermsProvider) == typeof(EndsWithTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermsProvider)(object)new EndsWithTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term);

        if (typeof(TTermsProvider) == typeof(EndsWithTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermsProvider)(object)new EndsWithTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term);

        if (typeof(TTermsProvider) == typeof(NotEndsWithTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermsProvider)(object)new NotEndsWithTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term);

        if (typeof(TTermsProvider) == typeof(NotEndsWithTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermsProvider)(object)new NotEndsWithTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term);

        if (typeof(TTermsProvider) == typeof(ContainsTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermsProvider)(object)new ContainsTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term);

        if (typeof(TTermsProvider) == typeof(ContainsTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermsProvider)(object)new ContainsTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term);

        if (typeof(TTermsProvider) == typeof(NotContainsTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermsProvider)(object)new NotContainsTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term);

        if (typeof(TTermsProvider) == typeof(NotContainsTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermsProvider)(object)new NotContainsTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term);

        if (typeof(TTermsProvider) == typeof(ExistsTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermsProvider)(object)new ExistsTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field);

        if (typeof(TTermsProvider) == typeof(ExistsTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermsProvider)(object)new ExistsTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field);

        throw new NotSupportedException($"{nameof(TTermsProvider)}: {typeof(TTermsProvider)} is not supported. ");
    }
}

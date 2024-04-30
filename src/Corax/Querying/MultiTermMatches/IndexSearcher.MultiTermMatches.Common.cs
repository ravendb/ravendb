using System;
using System.Diagnostics;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.TermProviders;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using static Voron.Data.CompactTrees.CompactTree;

namespace Corax.Querying;

public partial class IndexSearcher
{
    private MultiTermMatch MultiTermMatchBuilder<TTermProvider>(in FieldMetadata field, Slice term, bool streamingEnabled = false, bool validatePostfixLen = false, in CancellationToken token = default)
        where TTermProvider : struct, ITermProvider
    {
        if (_fieldsTree == null || _fieldsTree.TryGetCompactTreeFor(field.FieldName, out var terms) == false)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);
        
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
        
        CompactKey seekKey = null;
        if (TryRewriteTermWhenPerformingBackwardStreaming<TTermProvider>(streamingEnabled, term, out var seekTerm))
        {
            seekKey = _fieldsTree.Llt.AcquireCompactKey();
            seekKey.Set(seekTerm.AsReadOnlySpan());
            seekKey.ChangeDictionary(terms.DictionaryId);
        }
        
        return MultiTermMatch.Create(new MultiTermMatch<TTermProvider>(this, field, _transaction.Allocator, 
            GetMultiTermMatchProvider<TTermProvider>(field, terms, termKey, seekKey, validatePostfixLen, token), streamingEnabled: streamingEnabled, token: token));
    }

    private MultiTermMatch MultiTermMatchBuilder<TTermProvider>(in FieldMetadata field, string term, bool streamingEnabled, CancellationToken token)
        where TTermProvider : struct, ITermProvider
    {
        if (_fieldsTree == null || _fieldsTree.TryGetCompactTreeFor(field.FieldName, out var terms) == false)
            return MultiTermMatch.CreateEmpty(_transaction.Allocator);

        var slicedTerm = EncodeAndApplyAnalyzer(field, term);
        var termKey = _fieldsTree.Llt.AcquireCompactKey();
        termKey.Set(slicedTerm.AsReadOnlySpan());

        CompactKey seekKey = null;
        if (TryRewriteTermWhenPerformingBackwardStreaming<TTermProvider>(streamingEnabled, slicedTerm, out var seekTerm))
        {
            seekKey = _fieldsTree.Llt.AcquireCompactKey();
            seekKey.Set(seekTerm.AsReadOnlySpan());
        }

        return MultiTermMatch.Create(new MultiTermMatch<TTermProvider>(this, field, _transaction.Allocator, 
            GetMultiTermMatchProvider<TTermProvider>(field, terms, termKey, seekKey,  validatePostfixLen: false, token: token), 
            streamingEnabled, token: token));
    }

    private bool TryRewriteTermWhenPerformingBackwardStreaming<TTermProvider>(bool streamingEnabled, Slice termSlice, out Slice termForSeek)
        where TTermProvider : struct, ITermProvider
    {
        var shouldRewrite = typeof(TTermProvider) == typeof(StartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>);

        if (streamingEnabled == false || shouldRewrite == false || termSlice.Size == 0)
        {
            termForSeek = default;
            return false;
        }

        var originalTerm = termSlice.AsSpan();

        if (originalTerm[^1] < byte.MaxValue)
        {
            Slice.From(Allocator, termSlice.AsSpan(), out termForSeek);
            //When we have eg startsWith("ab") we have to seek into "ac"
            termForSeek.AsSpan()[^1]++;
            return true;
        }

        if (originalTerm.Length >= 2)
        {
            //Lets scan
            int idX = originalTerm.Length - 2;
            for (; idX >= 0; idX--)
            {
                if (originalTerm[idX] < byte.MaxValue)
                    break;
            }

            if (idX == 0 && originalTerm[idX] == byte.MaxValue)
                goto AfterAllKeys;

            using (Slice.From(Allocator, originalTerm, out Slice temporarySlice))
            {
                temporarySlice[idX]++;
                temporarySlice[idX + 1] = 1;

                //We accept leaking here since it's will be released after query execution.
                Slice.From(Allocator, temporarySlice.AsSpan().Slice(idX + 1), out termForSeek);
                return true;
            }
        }

        AfterAllKeys:
        //Super rare case when we have prefix [255][255] prefix that means we can go to the end of tree, isn't?
        //[255] chain, we can go to the end of the tree then ;-)
        termForSeek = Slices.AfterAllKeys;
        return true;
    }

    private TTermProvider GetMultiTermMatchProvider<TTermProvider>(in FieldMetadata field, CompactTree termTree, CompactKey term, CompactKey seekTerm, bool validatePostfixLen, CancellationToken token)
        where TTermProvider : struct, ITermProvider
    {
        if (typeof(TTermProvider) == typeof(StartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermProvider)(object)new StartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term, seekTerm, validatePostfixLen, token);
        
        if (typeof(TTermProvider) == typeof(StartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermProvider)(object)new StartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term, seekTerm, validatePostfixLen, token);
        
        if (typeof(TTermProvider) == typeof(NotStartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermProvider)(object)new NotStartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term, validatePostfixLen, token);
        
        if (typeof(TTermProvider) == typeof(NotStartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermProvider)(object)new NotStartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term, validatePostfixLen, token);
        
        Debug.Assert(validatePostfixLen == false, "Not supported for the rest of this");
        
        if (typeof(TTermProvider) == typeof(EndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermProvider)(object)new EndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term);
        
        if (typeof(TTermProvider) == typeof(EndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermProvider)(object)new EndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term);
        
        if (typeof(TTermProvider) == typeof(NotEndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermProvider)(object)new NotEndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term);
        
        if (typeof(TTermProvider) == typeof(NotEndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermProvider)(object)new NotEndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term);
        
        if (typeof(TTermProvider) == typeof(ContainsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermProvider)(object)new ContainsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term);
        
        if (typeof(TTermProvider) == typeof(ContainsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermProvider)(object)new ContainsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term);
        
        if (typeof(TTermProvider) == typeof(NotContainsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermProvider)(object)new NotContainsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term);
        
        if (typeof(TTermProvider) == typeof(NotContainsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermProvider)(object)new NotContainsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term);
        
        if (typeof(TTermProvider) == typeof(ExistsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermProvider)(object)new ExistsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field);
        
        if (typeof(TTermProvider) == typeof(ExistsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermProvider)(object)new ExistsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field);

        throw new NotSupportedException($"{nameof(TTermProvider)}: {typeof(TTermProvider)} is not supported. ");
    }
}

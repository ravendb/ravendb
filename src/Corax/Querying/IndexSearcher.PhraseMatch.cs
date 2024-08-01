using System;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Querying;

public partial class IndexSearcher
{
    public IQueryMatch PhraseQuery<TInner>(TInner inner, in FieldMetadata field, ReadOnlySpan<Slice> terms)
        where TInner : IQueryMatch
    {
        var compactTree = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (compactTree == null)
            return default;

        Allocator.Allocate(terms.Length * sizeof(long), out var sequenceBuffer);
        Span<long> sequence = sequenceBuffer.ToSpan<long>();
        
        var termsVectorFieldName = field.GetPhraseQueryContainerName(Allocator);
        var vectorRootPage = GetRootPageByFieldName(termsVectorFieldName);
        var rootPage = GetRootPageByFieldName(field.FieldName);

        using var _ = _fieldsTree.Llt.AcquireCompactKey(out var termKey);
        
        for (var i = 0; i < terms.Length; ++i)
        {
            termKey.Set(terms[i]);

            // When the term doesn't exist, that means no document matches our query (phrase query is performing "AND" between them).
            if (compactTree.TryGetTermContainerId(termKey, out var termContainerId) == false)
                return TermMatch.CreateEmpty(this, Allocator);

            sequence[i] = termContainerId;
        }
        
        return new PhraseMatch<TInner>(this, inner, sequenceBuffer, vectorRootPage, rootPage: rootPage);
    }
}

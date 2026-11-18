using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Corax.Mappings;
using Corax.Pipeline;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Sparrow.Compression;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Querying;

public partial class IndexSearcher
{
    public IQueryMatch PhraseQuery<TInner>(TInner inner, in FieldMetadata field, ReadOnlySpan<Slice> terms)
        where TInner : IQueryMatch
    {
        var compactTree = GetTermsFor(field.FieldName);
        if (compactTree is null)
            return EmptyQueryMatch.Instance;

        Allocator.Allocate(terms.Length * sizeof(long), out var sequenceBuffer);
        Span<long> sequence = sequenceBuffer.ToSpan<long>();
        
        var termsVectorFieldName = field.GetPhraseQueryContainerName(Allocator);

        if (TryGetRootPageByFieldName(termsVectorFieldName, out var vectorRootPage) == false || TryGetRootPageByFieldName(field.FieldName, out var rootPage) == false)
            return EmptyQueryMatch.Instance;

        using var _ = _fieldsTree.Llt.AcquireCompactKey(out var termKey);
        for (var i = 0; i < terms.Length; ++i)
        {
            termKey.Set(terms[i]);

            // When the term doesn't exist, that means no document matches our query (phrase query is performing "AND" between them).
            if (compactTree.TryGetTermContainerId(termKey, out var termContainerId) == false)
                return EmptyQueryMatch.Instance;

            sequence[i] = termContainerId;
        }
        
        return new PhraseMatch<TInner>(field, this, inner, sequenceBuffer, vectorRootPage, rootPage: rootPage);
    }
}

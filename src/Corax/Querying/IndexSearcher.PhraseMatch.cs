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
using static Voron.Global.Constants;

namespace Corax.Querying;

public partial class IndexSearcher
{
    public IQueryMatch PhraseQuery<TInner>(TInner inner, in FieldMetadata field, ReadOnlySpan<Slice> terms)
        where TInner : IQueryMatch
    {
        if (_fieldsTree == null || _fieldsTree.TryGetCompactTreeFor(field.FieldName, out var compactTree) == false)
            return default;

        Allocator.Allocate(terms.Length * sizeof(long), out var sequenceBuffer);
        Span<long> sequence = sequenceBuffer.ToSpan<long>();
        
        var termsVectorFieldName = field.GetPhraseQueryContainerName(Allocator);
        var vectorRootPage = GetRootPageByFieldName(termsVectorFieldName);
        var rootPage = GetRootPageByFieldName(field.FieldName);

        for (var i = 0; i < terms.Length; ++i)
        {
            var term = terms[i];
            CompactKey termKey = _fieldsTree.Llt.AcquireCompactKey();
            termKey.Set(term);

            // When the term doesn't exist, that means no document matches our query (phrase query is performing "AND" between them).
            if (compactTree.TryGetTermContainerId(termKey, out var termContainerId) == false)
                return TermMatch.CreateEmpty(this, Allocator);

            sequence[i] = termContainerId;
        }
        
        return new PhraseMatch<TInner>(field, this, inner, sequenceBuffer, vectorRootPage, rootPage: rootPage);
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Corax.Mappings;
using Corax.Pipeline;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Querying;

public partial class IndexSearcher
{
    public PhraseMatch<TInner> PhraseMatch<TInner>(TInner inner, in FieldMetadata field, ReadOnlySpan<Slice> terms)
        where TInner : IQueryMatch
    {
        var compactTree = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (compactTree == null)
            return default;

        var currentTermId = 0;
        Allocator.Allocate(terms.Length * sizeof(long), out var sequenceBuffer);
        
        var sequenceToFind = MemoryMarshal.Cast<byte, long>(sequenceBuffer.ToSpan());
        var pagesToField = GetIndexedFieldNamesByRootPage();
        var fieldName = field.GetPhraseQueryContainerName(Allocator);
        var rootPage = pagesToField.Where(x => x.Value == fieldName.ToString()).FirstOrDefault().Key; //todo perf

        for (var i = 0; i < terms.Length; ++i)
        {
            var term = terms[i];
            CompactKey termKey = _fieldsTree.Llt.AcquireCompactKey();
            termKey.Set(term);

            var exists = compactTree.TryGetTermContainerId(termKey, out var termContainerId);
            //if term doesn't exists we can already finish since such sequence doesn't exists. right?

            sequenceToFind[currentTermId++] = termContainerId;
        }
        
        return new PhraseMatch<TInner>(field, this, inner, sequenceBuffer, currentTermId, rootPage);
    }

    public PhraseMatch<TInner> PhraseMatch<TInner>(TInner inner, in FieldMetadata field, ReadOnlySpan<byte> terms, ReadOnlySpan<Token> tokens)
        where TInner : IQueryMatch
    {
        var compactTree = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (compactTree == null)
            return default;

        var currentTermId = 0;
        Allocator.Allocate(tokens.Length * sizeof(long), out var sequenceBuffer);
        
        var sequenceToFind = MemoryMarshal.Cast<byte, long>(sequenceBuffer.ToSpan());
        var pagesToField = GetIndexedFieldNamesByRootPage();
        var fieldName = field.GetPhraseQueryContainerName(Allocator);
        var rootPage = pagesToField.Where(x => x.Value == fieldName.ToString()).FirstOrDefault().Key; //todo perf

        for (var tokenId = 0; tokenId < tokens.Length; ++tokenId)
        {
            var token = tokens[tokenId];
            if (token.Length == 0) continue;
            
            var term = terms.Slice(token.Offset, (int)token.Length);
            
            
            CompactKey termKey = _fieldsTree.Llt.AcquireCompactKey();
            termKey.Set(term);

            var exists = compactTree.TryGetTermContainerId(termKey, out var termContainerId);
            //if term doesn't exists we can already finish since such sequence doesn't exists. right?

            sequenceToFind[currentTermId++] = termContainerId;
        }
        
        return new PhraseMatch<TInner>(field, this, inner, sequenceBuffer, currentTermId, rootPage);
    }
}

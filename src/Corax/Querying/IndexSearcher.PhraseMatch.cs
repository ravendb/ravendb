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
        var compactTree = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (compactTree == null)
            return default;

        Allocator.Allocate(terms.Length * sizeof(long), out var sequenceBuffer);
        var sequencePosition = 0;
        var pagesToField = GetIndexedFieldNamesByRootPage();
        var fieldName = field.GetPhraseQueryContainerName(Allocator);
        var rootPage = pagesToField.FirstOrDefault(x => SliceComparer.CompareInline(x.Value, fieldName) == 0).Key;

        for (var i = 0; i < terms.Length; ++i)
        {
            var term = terms[i];
            CompactKey termKey = _fieldsTree.Llt.AcquireCompactKey();
            termKey.Set(term);

            if (compactTree.TryGetTermContainerId(termKey, out var termContainerId) == false)
                return TermMatch.CreateEmpty(this, Allocator);

            sequencePosition += ZigZagEncoding.Encode(sequenceBuffer.ToSpan(), termContainerId, sequencePosition);
        }
        
        return new PhraseMatch<TInner>(field, this, inner, sequenceBuffer, sequencePosition, rootPage);
    }

    public IQueryMatch PhraseQuery<TInner>(TInner inner, in FieldMetadata field, ReadOnlySpan<byte> terms, ReadOnlySpan<Token> tokens)
        where TInner : IQueryMatch
    {
        var compactTree = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (compactTree == null)
            return default;

        Allocator.Allocate(tokens.Length * sizeof(long), out var sequenceBuffer);
        var sequencePosition = 0;
        
        var pagesToField = GetIndexedFieldNamesByRootPage();
        var fieldName = field.GetPhraseQueryContainerName(Allocator);
        var rootPage = pagesToField.FirstOrDefault(x => SliceComparer.CompareInline(x.Value, fieldName) == 0).Key;

        for (var tokenId = 0; tokenId < tokens.Length; ++tokenId)
        {
            var token = tokens[tokenId];
            if (token.Length == 0) continue;
            
            var term = terms.Slice(token.Offset, (int)token.Length);
            
            
            CompactKey termKey = _fieldsTree.Llt.AcquireCompactKey();
            termKey.Set(term);

            if (compactTree.TryGetTermContainerId(termKey, out var termContainerId) == false)
                return TermMatch.CreateEmpty(this, Allocator);

            sequencePosition += ZigZagEncoding.Encode(sequenceBuffer.ToSpan(), termContainerId, sequencePosition);
        }
        
        return new PhraseMatch<TInner>(field, this, inner, sequenceBuffer, sequencePosition, rootPage);
    }
    
}

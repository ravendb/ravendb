using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Sparrow.Server;
using Voron;

namespace Corax.Querying.Matches;

public struct PhraseMatch<TInner> : IQueryMatch
    where TInner : IQueryMatch
{
    
    private TInner _inner;
    private readonly FieldMetadata _fieldMetadata;
    private IndexSearcher _indexSearcher;
    private ByteString _subsequence;
    private readonly int _subsequenceLength;
    private readonly long _rootPage;


    public PhraseMatch(in FieldMetadata fieldMetadata, IndexSearcher indexSearcher, TInner inner, ByteString subsequence, int subsequenceLength, long rootPage)
    {
        _fieldMetadata = fieldMetadata;
        _indexSearcher = indexSearcher;
        _inner = inner;
        _subsequence = subsequence;
        _subsequenceLength = subsequenceLength;
        _rootPage = rootPage;

        Debug.Assert(_subsequence.Length % sizeof(long) == 0, "this._subsequence.Length % sizeof(long) == 0");
    }

    public long Count => _inner.Count;
    public SkipSortingResult AttemptToSkipSorting()
    {
        //Filter only, not changing order.
        return _inner.AttemptToSkipSorting();
    }

    public QueryCountConfidence Confidence => QueryCountConfidence.Normal;
    public bool IsBoosting => _inner.IsBoosting;
    public int Fill(Span<long> matches)
    {
        ref var match = ref _inner;
        var results = match.Fill(matches);
        int currentId = 0;
        Page p = default;
        var sequenceToFind = _subsequence.ToSpan().Slice(0, _subsequenceLength);
        
        for (var processingId = 0; processingId < results; ++processingId)
        {
            var entryTermsReader = _indexSearcher.GetEntryTermsReader(matches[processingId], ref p);
            var result = entryTermsReader.FindNextStored(_rootPage);
            if (result == false) continue;
            
            Debug.Assert(entryTermsReader.IsList, "entryTermsReader.IsList");
            
            var storedValue = entryTermsReader.StoredField.Value.ToSpan();
            var isMatch = storedValue.IndexOf(sequenceToFind);
            if (isMatch >= 0)
                matches[currentId++] = matches[processingId];
        }

        return currentId;
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        var results = _inner.AndWith(buffer, matches);
        int currentId = 0;
        Page p = default;
        var sequenceToFind = _subsequence.ToSpan().Slice(0, _subsequenceLength);

        for (var processingId = 0; processingId < results; ++processingId)
        {
            var entryTermsReader = _indexSearcher.GetEntryTermsReader(buffer[processingId], ref p);
            var result = entryTermsReader.FindNextStored(_rootPage);
            if (result == false) continue;
            
            Debug.Assert(entryTermsReader.IsList, "entryTermsReader.IsList");
            var storedValue = entryTermsReader.StoredField.Value.ToSpan();
            var isMatch = storedValue.IndexOf(sequenceToFind);
            if (isMatch >= 0)
                buffer[currentId++] = buffer[processingId];
        }
        
        return currentId;
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        _inner.Score(matches, scores, boostFactor);
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode(nameof(PhraseMatch<TInner>),
            parameters: new Dictionary<string, string>()
            {
                { nameof(IsBoosting), IsBoosting.ToString() },
                { nameof(Count), $"{Count} [{Confidence}]" }
            },
            children: [_inner.Inspect()]);
    }
}

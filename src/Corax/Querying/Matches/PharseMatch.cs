using System;
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
    private readonly int _termCount;
    private readonly long _rootPage;


    public PhraseMatch(in FieldMetadata fieldMetadata, IndexSearcher indexSearcher, TInner inner, ByteString subsequence, int termCount, long rootPage)
    {
        _fieldMetadata = fieldMetadata;
        _indexSearcher = indexSearcher;
        _inner = inner;
        _subsequence = subsequence;
        _termCount = termCount;
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
        var subsequence = MemoryMarshal.Cast<byte, long>(_subsequence.ToSpan().Slice(0, sizeof(long) * _termCount));
        int currentId = 0;
        Page p = default;
        for (var processingId = 0; processingId < results; ++processingId)
        {
            var entryTermsReader = _indexSearcher.GetEntryTermsReader(matches[processingId], ref p);
            var result = entryTermsReader.FindNextStored(_rootPage);
            
            Debug.Assert(result, "Document has to have stored field! This is a bug.");
            Debug.Assert(entryTermsReader.IsList, "entryTermsReader.IsList");
            
            var list = entryTermsReader.StoredField.Value;
            var storedList = MemoryMarshal.Cast<byte, long>(list.ToSpan());

            for (var i = 0; i < storedList.Length; ++i)
            {
                var indexOf = storedList.Slice(i).IndexOf(subsequence[0]);
                if (indexOf < 0) break;
                if (indexOf + _termCount > storedList.Length) break;

                if (storedList.Slice(indexOf, _termCount).SequenceEqual(subsequence) == false)
                {
                    i = indexOf + _termCount - 1;
                }
                else
                {
                    matches[currentId++] = matches[processingId];
                    break;
                }
            }
        }

        return currentId;
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        var results = _inner.AndWith(buffer, matches);
        var subsequence = MemoryMarshal.Cast<byte, long>(_subsequence.ToSpan().Slice(0, sizeof(long) * _termCount));

        int currentId = 0;
        Page p = default;
        for (var processingId = 0; processingId < results; ++processingId)
        {
            var entryTermsReader = _indexSearcher.GetEntryTermsReader(buffer[processingId], ref p);
            var result = entryTermsReader.FindNextStored(_rootPage);
            
            Debug.Assert(result, "Document has to have stored field! This is a bug.");
            Debug.Assert(entryTermsReader.IsList, "entryTermsReader.IsList");
            
            var list = entryTermsReader.StoredField.Value;
            var storedList = MemoryMarshal.Cast<byte, long>(list.ToSpan());

            for (var i = 0; i < storedList.Length; ++i)
            {
                var indexOf = storedList.Slice(i).IndexOf(subsequence[0]);
                if (indexOf < 0) break;
                if (indexOf + _termCount > storedList.Length) break;

                if (storedList.Slice(indexOf, _termCount).SequenceEqual(subsequence) == false)
                {
                    i = indexOf + _termCount - 1;
                }
                else
                {
                    buffer[currentId++] = buffer[processingId];
                    break;
                }
            }
        }

        return currentId;
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        _inner.Score(matches, scores, boostFactor);
    }

    public QueryInspectionNode Inspect()
    {
        return _inner.Inspect(); //todo
    }
}

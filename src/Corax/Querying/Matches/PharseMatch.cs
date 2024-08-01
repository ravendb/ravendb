using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Sparrow.Binary;
using Sparrow.Compression;
using Sparrow.Server;
using Voron;

namespace Corax.Querying.Matches;

public struct PhraseMatch<TInner> : IQueryMatch
    where TInner : IQueryMatch
{
    private IDisposable _memoryHandler;
    private ByteString _memory;
    private int MemorySize => _memory.Length / ( sizeof(int) + sizeof(long));
    
    
    private TInner _inner;
    private readonly IndexSearcher _indexSearcher;
    private readonly ByteString _subsequence;
    private readonly long _vectorRootPage;
    private readonly long _rootPage;


    public PhraseMatch(IndexSearcher indexSearcher, TInner inner, ByteString subsequence, long vectorRootPage, long rootPage)
    {
        _indexSearcher = indexSearcher;
        _inner = inner;
        _subsequence = subsequence;
        _vectorRootPage = vectorRootPage;
        _rootPage = rootPage;
        Debug.Assert(_subsequence.Length % sizeof(long) == 0, "this._subsequence.Length % sizeof(long) == 0");

        _memoryHandler = null;
        _memory = default;
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
        return ScanDocumentsTermsEntries(matches.Slice(0, results));
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        var results = _inner.AndWith(buffer, matches);
        return ScanDocumentsTermsEntries(buffer.Slice(0, results));
    }
    
    private int ScanDocumentsTermsEntries(Span<long> matches)
    {
        int currentId = 0;
        Page p = default;
        var sequenceToFind = _subsequence.ToSpan<long>();
        
        Span<long> buffer = _memoryHandler is null 
            ? stackalloc long[128] 
            :  MemoryMarshal.Cast<byte, long>(_memory.ToSpan().Slice(0, sizeof(long) * MemorySize));

        Span<int> indexes = _memoryHandler is null 
          ? stackalloc int[128]
          : MemoryMarshal.Cast<byte, int>(_memory.ToSpan().Slice(MemorySize * sizeof(long)));

        using var _ = _indexSearcher.Transaction.LowLevelTransaction.AcquireCompactKey(out var existingKey);

        for (var processingId = 0; processingId < matches.Length; ++processingId)
        {
            _indexSearcher.GetEntryTermsReader(matches[processingId], ref p, out var entryTermsReader, existingKey);
            if (entryTermsReader.FindNextStored(_vectorRootPage) == false)
                continue;
            
            //This is value from storage, is not changed since we're seeking to non-stored-value
            var storedValue = entryTermsReader.StoredField.Value.ToSpan();
            entryTermsReader.Reset();

            int position = 0;
            int currentTerm = 0;
            long termId = -1;
            while (position < storedValue.Length)
            {
                if (currentTerm >= indexes.Length)
                    UnlikelyGrowBuffer(ref buffer, ref indexes);
                
                var documentPosition = ZigZagEncoding.Decode<int>(storedValue, out var len, position);
                position += len;
                var isRepetition = (documentPosition & 0b1) == 0b1;
                var originalIndex = documentPosition;
                if (isRepetition == false)
                {
                    var found = entryTermsReader.FindNext(_rootPage);
                    Debug.Assert(found, "We expected next term stored for document, this is a bug");
                    termId = entryTermsReader.TermId;
                }

                indexes[currentTerm] = originalIndex;
                buffer[currentTerm++] = termId;
            }
            
            if (currentTerm == 0 || sequenceToFind.Length > currentTerm) 
                continue;

            //In our reader, we have two "arrays". The first is a term list that contains terms present in documents, sorted by term IDs.
            //We also have our TermVector list, which represents the order of terms inside a document.
            //The position of an element in the term vector is linked with the term at the same position. (However, the term list is a unique list, so we are using the lowest bit in TermsVector that indicates this is a repetition of the previous term, allowing us to reproduce currentTerms to have EXACTLY the same length as the TermVector list).
            //The value inside the term vector is the initial position in the document.
            //When we sort the dictionary indexes using indexes.Sort(terms), we retrieve the initial sentences (analyzed, without stop-words).
            var currentTerms = buffer.Slice(0, currentTerm);
            indexes.Slice(0, currentTerm).Sort(currentTerms);
            
            var isMatch = currentTerms.IndexOf(sequenceToFind);
            if (isMatch >= 0)
                matches[currentId++] = matches[processingId];
        }

        return currentId;
    }
    
    private void UnlikelyGrowBuffer(ref Span<long> buffer, ref Span<int> indexes)
    {
        var length = Bits.PowerOf2(buffer.Length + 1);
        var newDisposable = _indexSearcher.Allocator.Allocate(length * (sizeof(long) + sizeof(int)), out ByteString memory);
        Span<long> newBuffer = MemoryMarshal.Cast<byte, long>(memory.ToSpan().Slice(0, sizeof(long) * length));
        Span<int> newIndexes = MemoryMarshal.Cast<byte, int>(memory.ToSpan().Slice(length * sizeof(long)));
        buffer.CopyTo(newBuffer);
        indexes.CopyTo(newIndexes);
        buffer = newBuffer;
        indexes = newIndexes;
                    
        _memoryHandler?.Dispose();
        _memoryHandler = newDisposable;
        _memory = memory;
    }
    
    internal string RenderOriginalSentence(long documentId)
    {
        var p = default(Page);
        
        _indexSearcher.GetEntryTermsReader(documentId, ref p, out var entryTermsReader);
        
        if (entryTermsReader.FindNextStored(_vectorRootPage) == false)
            return "NO PHRASE QUERY";
        
        var storedValue = entryTermsReader.StoredField.Value.ToSpan();
        List<int> indexes = new();
        List<string> terms = new();
        
        int position = 0;
        string termId = string.Empty;
        while (position < storedValue.Length)
        {
            var documentPosition = ZigZagEncoding.Decode<int>(storedValue, out var len, position);
            position += len;
            var isRepetition = (documentPosition & 0b1) == 0b1;
            var originalIndex = documentPosition;
            if (isRepetition == false)
            {
                var found = entryTermsReader.FindNext(_rootPage);
                Debug.Assert(found, "We expected next term stored for document, this is a bug");
                termId = Encoding.UTF8.GetString(entryTermsReader.Current.Decoded());
            }

            indexes.Add(originalIndex);
            terms.Add(termId);
        }

        var indexesAsSpan = CollectionsMarshal.AsSpan(indexes);
        var termsAsSpan = CollectionsMarshal.AsSpan(terms);
        
        Debug.Assert(indexesAsSpan.Length == termsAsSpan.Length, "indexes.Length == terms.Length");
        
        indexesAsSpan.Sort(termsAsSpan);
        
        return string.Join(" ", termsAsSpan.ToArray());
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

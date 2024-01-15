using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
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
    private int MemorySize => _memory.Length / (sizeof(int) + sizeof(long));
    
    
    private TInner _inner;
    private readonly FieldMetadata _fieldMetadata;
    private readonly IndexSearcher _indexSearcher;
    private readonly ByteString _subsequence;
    private readonly long _vectorRootPage;
    private readonly long _rootPage;


    public PhraseMatch(in FieldMetadata fieldMetadata, IndexSearcher indexSearcher, TInner inner, ByteString subsequence, long vectorRootPage, long rootPage)
    {
        _fieldMetadata = fieldMetadata;
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
        
        for (var processingId = 0; processingId < matches.Length; ++processingId)
        {
            var entryTermsReader = _indexSearcher.GetEntryTermsReader(matches[processingId], ref p);
            if (entryTermsReader.FindNextStored(_vectorRootPage) == false)
                continue;

            //This is value from storage, is not changed since we're seeking to non-stored-value
            var storedValue = entryTermsReader.StoredField.Value.ToSpan();

            int position = 0;
            entryTermsReader.Reset();
            var currentTerm = 0;
            while (entryTermsReader.FindNext(_rootPage))
            {
                if (currentTerm >= buffer.Length)
                    UnlikelyGrowBuffer(ref buffer, ref indexes);
                
                
                buffer[currentTerm] = entryTermsReader.TermId;
                indexes[currentTerm] = ZigZagEncoding.Decode<int>(storedValue, out var len, position);
                position += len;
                currentTerm += 1;
            }

            if (currentTerm == 0 || sequenceToFind.Length > currentTerm) 
                continue;

            var currentIndexes = indexes.Slice(0, currentTerm);
            var currentTerms = buffer.Slice(0, currentTerm);
            currentIndexes.Sort(currentTerms);
            
            Debug.Assert(entryTermsReader.IsList, "entryTermsReader.IsList");
            
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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Sparrow;
using Sparrow.Server;
using Voron.Impl;
using Constants = Voron.Global.Constants;

namespace Voron.Data.PostingLists;

public readonly unsafe struct PostingListLeafPage
{
    private readonly Page _page;
    public PostingListLeafPageHeader* Header => (PostingListLeafPageHeader*)_page.Pointer;

    public int SpaceUsed => Header->Floor + (Constants.Storage.PageSize - Header->Ceiling);

    public Span<byte> SpanFor(int idx)
    {
        return SpanFor(Header, idx);
    }

    public int RunFor(int idx, out byte* ptr)
    {
        var header = Header;
        if (idx >= header->NumberOfCompressedRuns)
            throw new ArgumentOutOfRangeException(nameof(idx));

        var runs = (CompressedHeader*)((byte*)header + PageHeader.SizeOf);
        ptr = _page.Pointer + runs[idx].Position;
        return runs[idx].Length;
    }
    
    private static Span<byte> SpanFor(PostingListLeafPageHeader* header, int idx)
    {
        if (idx >= header->NumberOfCompressedRuns)
            throw new ArgumentOutOfRangeException(nameof(idx));

        var runs = (CompressedHeader*)((byte*)header + PageHeader.SizeOf);

        return new Span<byte>((byte*)header + runs[idx].Position, runs[idx].Length);
    }

    public CompressedHeader* Runs => (CompressedHeader*)((byte*)Header + PageHeader.SizeOf);

    public struct CompressedHeader
    {
        public ushort Position;
        public ushort Length;

        public override string ToString()
        {
            return $"{nameof(Position)}: {Position}, {nameof(Length)}: {Length}";
        }
    }

    public PostingListLeafPage(Page page)
    {
        _page = page;
    }

    public static void InitLeaf(PostingListLeafPageHeader* header)
    {
        header->Flags = PageFlags.Single | PageFlags.Other;
        header->PostingListFlags = ExtendedPageType.PostingListLeaf;
        header->NumberOfCompressedRuns = 0;
        header->Ceiling = Constants.Storage.PageSize;
        header->NumberOfEntries = 0;
    }
    

    public static bool TryAdd(PostingListLeafPageHeader* header, Span<byte> compressed)
    {
        int floor = header->Floor + sizeof(CompressedHeader);
        int ceilingAfterNewVal = header->Ceiling - compressed.Length;
        if (floor > ceilingAfterNewVal)
            return false;
        
        int countOfItems = PForDecoder.ReadCount(compressed);

        header->Ceiling = (ushort)ceilingAfterNewVal;
        var buf = new Span<byte>(header, Constants.Storage.PageSize);
        compressed.CopyTo(buf[ceilingAfterNewVal..]);
        CompressedHeader* newMetadata = (CompressedHeader*)((byte*)header + PageHeader.SizeOf) + header->NumberOfCompressedRuns;
        header->NumberOfCompressedRuns++;
        header->NumberOfEntries += countOfItems;
        newMetadata->Length = (ushort)compressed.Length;
        newMetadata->Position = (ushort)ceilingAfterNewVal;
        return true;
    }

    public Iterator GetIterator() => new Iterator(this);

    [SkipLocalsInit]
    public struct Iterator
    {
        private readonly PostingListLeafPage _parent;
        private int _idx;
        private bool _hasDecoder;
        private PForDecoder.DecoderState _decoderState;
        private const int MoveNextBufferSize = 32;
        public int BufferIndex, BufferLength;

        public fixed long Buffer[PForEncoder.BufferLen];
        
        public Iterator(PostingListLeafPage parent)
        {
            _parent = parent;
            _hasDecoder = _parent.Header->NumberOfCompressedRuns > 0;
            _decoderState = _hasDecoder ? new PForDecoder.DecoderState(_parent.Runs[0].Length) : default;
            BufferIndex = 0;
            BufferLength = 0;
            _idx = 0;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext(out long l)
        {
            while (_hasDecoder)
            {
                if (BufferIndex < BufferLength)
                {
                    l = Buffer[BufferIndex++]; 
                    return true;
                }

                MoveNextUnlikely();
            }
            l = default;
            return false;
        }

        public void Skip(long val)
        {
            for (; _idx < _parent.Header->NumberOfCompressedRuns; _idx++)
            {
                Span<byte> compressed = _parent.SpanFor(_idx);
                long lastValue = PForDecoder.ReadLast(compressed);
                if (val > lastValue)
                    continue;

                _decoderState = new PForDecoder.DecoderState(compressed.Length);
                while (true)
                {
                    UncompressValues();
                    Debug.Assert(BufferLength > 0); 
                    if (BufferLength == 0 || // for safety's sake, should never actually happen
                        // keep reading from the compressed segment until we find a value that is
                        // bigger than the value we are searching for
                        val <= Buffer[BufferLength-1])
                        break;
                }

                // now find its position in the current buffer
                fixed (long* scratchPtr = Buffer)
                {
                    var loc = new Span<long>(scratchPtr, BufferLength).BinarySearch(val);
                    if (loc < 0)
                        loc = ~loc;
                    BufferIndex = loc;
                }
                return;
            }

            _hasDecoder = false;
        }
        
        public int TryFill(Span<long> matches, long pruneGreaterThanOptimization)
        {
            if (BufferIndex >= BufferLength)
                return 0;

            fixed (long* buffer = Buffer)
            {
                var bufferedMatches = new Span<long>(buffer + BufferIndex, BufferLength - BufferIndex);
                var countOfItems = Math.Min(matches.Length, bufferedMatches.Length);
                if (bufferedMatches[^1] >= pruneGreaterThanOptimization)
                {
                    countOfItems = bufferedMatches.BinarySearch(pruneGreaterThanOptimization);
                    if (countOfItems < 0)
                        countOfItems = ~countOfItems;
                }
                bufferedMatches[..countOfItems].CopyTo(matches);
                BufferIndex += countOfItems;
                return countOfItems;

            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Fill(Span<long> matches, out int numberOfMatches, out bool hasPrunedResults, long pruneGreaterThanOptimization = long.MaxValue)
        {
            Debug.Assert(BufferIndex == BufferLength);

            numberOfMatches = 0;
            int compressIndex = BufferIndex;
            int compressLength = BufferLength;

            while (true)
            {
                if (compressIndex >= compressLength)
                {
                    UncompressValues();
                    if (_hasDecoder == false)
                    {
                        hasPrunedResults = false;
                        break;
                    }

                    compressIndex = BufferIndex;
                    compressLength = BufferLength;
                }

                long v = Buffer[compressIndex++];
                matches[numberOfMatches++] = v;
                if (v > pruneGreaterThanOptimization)
                {
                    hasPrunedResults = true;
                    break;
                }

                if (numberOfMatches < matches.Length)
                    continue;

                hasPrunedResults = false;
                break;
            }

            BufferIndex = compressIndex;
            BufferLength = compressLength;
        }

        private void MoveNextUnlikely()
        {
            Debug.Assert(_hasDecoder);

            UncompressValues();
        }

        private void UncompressValues()
        {
            fixed (long* scratchPtr = Buffer)
            {
                while (_hasDecoder)
                {
                    BufferIndex = 0;
                    int runLen = _parent.RunFor(_idx, out var runPtr);
                    BufferLength = PForDecoder.Decode(ref _decoderState, runPtr, runLen, scratchPtr, PForEncoder.BufferLen);
                    if (BufferLength > 0)
                        return;
                    _idx++;
                    if (_idx >= _parent.Header->NumberOfCompressedRuns)
                        break;
                    _decoderState = new PForDecoder.DecoderState(_parent.Runs[_idx].Length);
                }

                _hasDecoder = false;
            }
        }
    }

    /// <summary>
    /// Additions and removals are *sorted* by the caller
    /// maxValidValue is the limit for the *next* page, so we won't consume entries from there
    /// </summary>
    public List<ExtraSegmentDetails> Update(LowLevelTransaction tx, ref long* additions, ref int additionsCount, ref long* removals, ref int removalsCount, long maxValidValue)
    {
        var updater = new Updater(this, additions, additionsCount, removals, removalsCount, tx, maxValidValue);
        
        updater.Update();
        additions += updater.AdditionsIdx;
        additionsCount -= updater.AdditionsIdx;
        removals += updater.RemovalsIdx;
        removalsCount -= updater.RemovalsIdx;
        return updater.Extras;
    }

    [SkipLocalsInit]
    private ref struct Updater
    {
        private const int MaximumCompressedSizeBytes = 2048;
        private const int DesirableMaxCompressedBits = (MaximumCompressedSizeBytes / 2) * 8; // about 1 KB
        private const long InvalidValue = long.MaxValue;

        private readonly PostingListLeafPage _parent;
        private readonly LowLevelTransaction _tx;
        private readonly long* _additions;
        private readonly int _additionsCount;
        private readonly long* _removals;
        private readonly int _removalsCount;
        private long _maxValidValue;
        private fixed long _uncompressed[PForEncoder.BufferLen];
        private fixed uint _scratch[PForEncoder.BufferLen];
        public int AdditionsIdx;
        public int RemovalsIdx;

        public List<ExtraSegmentDetails> Extras;
        private PostingListLeafPageHeader* Header => _parent.Header;
        
        private bool _hasDecoder;
        private PForDecoder.DecoderState _decoderState;
        private long _compressedCurrent;
        private long _additionCurrent;
        private long _removalCurrent;
        private int _uncompressedIdx;
        private int _uncompressedLen;
        private int _runIdx;
        
        public Updater(PostingListLeafPage parent, long* additions, int additionsCount, long* removals, int removalsCount, LowLevelTransaction tx, long maxValidValue)
        {
            _parent = parent;
            _compressedCurrent = InvalidValue;
            _additionCurrent = InvalidValue;
            _removalCurrent = InvalidValue;
            AdditionsIdx = 0;
            RemovalsIdx = 0;
            _uncompressedIdx = 0;
            _uncompressedLen = 0;
            _hasDecoder = false;
            _decoderState = default;
            _additions = additions;
            _additionsCount = additionsCount;
            _removals = removals;
            _removalsCount = removalsCount;
            _tx = tx;
            _maxValidValue = maxValidValue;
            Extras = null;
            _runIdx = 0;
        }


        public void Update()
        {
            if (_additionsCount == 0 && _removalsCount == 0)
                return; // nothing to do

            using var _ = _tx.Allocator.Allocate(Constants.Storage.PageSize, out ByteString tmp);
            var newPagePtr = tmp.Ptr;
            
            PostingListLeafPageHeader* newHeader = (PostingListLeafPageHeader*)newPagePtr;
            Memory.Copy(newPagePtr, Header, PageHeader.SizeOf);
            InitLeaf(newHeader);

            MergeCompressedAdditionsAndRemovals(newHeader);

            // clear the middle of the page
            Memory.Set(newPagePtr + newHeader->Floor, 0, newHeader->Ceiling - newHeader->Floor);
            Memory.Copy(_parent.Header, newPagePtr, Constants.Storage.PageSize);
        }

        private void MergeCompressedAdditionsAndRemovals(PostingListLeafPageHeader* newHeader)
        {
            using var __ = _tx.Allocator.Allocate(MaximumCompressedSizeBytes, out Span<byte> tmp);
            fixed (uint* pScratch = _scratch)
            {
                FindFirstRelevantRunAndCopyPreviousOnes(newHeader);
                var encoder = new PForEncoder(tmp, pScratch);
                while (true)
                {
                    if (_compressedCurrent == InvalidValue && _hasDecoder)
                    {
                        if (_uncompressedIdx < _uncompressedLen)
                        {
                            _compressedCurrent = _uncompressed[_uncompressedIdx++];
                        }
                        else
                        {
                            TryFillUncompressed();
                            continue;
                        }

                        Debug.Assert(_compressedCurrent != InvalidValue);
                    }

                    if (_hasDecoder == false && Extras != null)
                    {
                        // we are now writing to *another* page, but we consumed all the
                        // compressed entries on this page, so we should return back to the caller

                        // we may have checked-out addition/removal, and need to "return" them so we'll
                        // process them in the next page
                        if (_additionCurrent != InvalidValue)
                            AdditionsIdx--;
                        if (_removalCurrent != InvalidValue)
                            RemovalsIdx--;
                        
                        FlushEncoder(ref encoder, tmp, newHeader);
                        return;
                    }

                    if (_additionCurrent == InvalidValue)
                    {
                        if (AdditionsIdx < _additionsCount && _additions[AdditionsIdx] < _maxValidValue)
                        {
                            _additionCurrent = _additions[AdditionsIdx++] ;
                            Debug.Assert(_additionCurrent != InvalidValue);
                        }
                    }

                    if (_removalCurrent == InvalidValue)
                    {
                        if (RemovalsIdx < _removalsCount && _removals[RemovalsIdx] < _maxValidValue)
                        {
                            _removalCurrent = _removals[RemovalsIdx++];
                            Debug.Assert(_removalCurrent != InvalidValue);
                        }
                    }

                    if (_compressedCurrent == InvalidValue && _additionCurrent == InvalidValue)
                    {
                        // nothing to add, but may need to skip removals
                        for (; RemovalsIdx < _removalsCount  && _removals[RemovalsIdx] < _maxValidValue; RemovalsIdx++)
                        {
                            if (_removals[RemovalsIdx] <= _maxValidValue)
                                break;
                        }

                        if (encoder.NumberOfAdditions > 0)
                        {
                            FlushEncoder(ref encoder, tmp, newHeader);
                        }

                        return;
                    }

                    long current;
                    if (_additionCurrent < _compressedCurrent || _compressedCurrent == InvalidValue)
                    {
                        current = _additionCurrent;
                        _additionCurrent = InvalidValue;
                    }
                    else if (_additionCurrent == _compressedCurrent)
                    {
                        current = _additionCurrent;
                        _additionCurrent = InvalidValue;
                        _compressedCurrent = InvalidValue;
                    }
                    else // additionCurrent > compressedCurrent
                    {
                        current = _compressedCurrent;
                        _compressedCurrent = InvalidValue;
                    }

                    if (_removalCurrent < current)
                    {
                        // removal of item not in additions / compressed
                        _removalCurrent = InvalidValue;
                    }
                    else if (_removalCurrent == current)
                    {
                        _removalCurrent = InvalidValue;
                        continue; // skip the entry
                    }

                    if (encoder.TryAdd(current) == false)
                        throw new InvalidOperationException("This should not be possible, we max at ~1KB, and have ~2KB available");
                    if (DesirableMaxCompressedBits > encoder.ConsumedBits)
                        continue;

                    FlushEncoder(ref encoder, tmp, newHeader);
                    encoder = new PForEncoder(tmp, pScratch);
                }
            }
        }

        private void FlushEncoder(ref PForEncoder encoder, Span<byte> tmp, PostingListLeafPageHeader* newHeader)
        {
            if (encoder.NumberOfAdditions == 0)
                return;
            
            if (encoder.TryCloseWithSuffix() == false)
                throw new InvalidOperationException("This should not be possible, we max at ~1KB, and have ~2KB available");

            Span<byte> compressed = tmp[..encoder.SizeInBytes];
            if(Extras != null ||// if we previously run out of space, we need to continue to do so in the future 
               TryAdd(newHeader, compressed) == false)
            {
                AddToExtras(ref encoder, compressed);
            }
        }

        private void AddToExtras(ref PForEncoder encoder, Span<byte> compressed)
        {
            Extras ??= new List<ExtraSegmentDetails>();

            var scope = _tx.Allocator.Allocate(compressed.Length, out Memory<byte> buffer);
            compressed.CopyTo(buffer.Span);
            Extras.Add(new ExtraSegmentDetails
            {
                Compressed = buffer, 
                Scope = scope, 
                LastValue = encoder.Last,
                FirstValue = encoder.First,
                NumberOfEntries = encoder.NumberOfAdditions
            });
        }

        private void TryFillUncompressed()
        {
            CompressedHeader* runs = _parent.Runs;
            fixed (long* pUncompressed = _uncompressed)
            {
                _uncompressedLen = PForDecoder.Decode(ref _decoderState, (byte*)Header + runs[_runIdx].Position, runs[_runIdx].Length,
                    pUncompressed, PForEncoder.BufferLen);
            }

            _uncompressedIdx = 0;
            if (_uncompressedLen != 0) 
                return;
            
            _runIdx++;
            if (_runIdx < Header->NumberOfCompressedRuns)
            {
                _decoderState = new PForDecoder.DecoderState(runs[_runIdx].Length);
            }
            else
            {
                _hasDecoder = false;
            }
        }

        private void FindFirstRelevantRunAndCopyPreviousOnes(PostingListLeafPageHeader* newHeader)
        {
            long first = long.MaxValue;
            if (_additionsCount > 0 && _additions[0] <= _maxValidValue)
            {
                first = _additions[0];
            }

            if (_removalsCount > 0 && _removals[0] <= _maxValidValue)
            {
                first = Math.Min(_removals[0], first);
            }

            _runIdx = 0;
            for (; _runIdx < Header->NumberOfCompressedRuns; _runIdx++)
            {
                var compressed = _parent.SpanFor(_runIdx);
                if (first > PForDecoder.ReadLast(compressed))
                {
                    if (TryAdd(newHeader, compressed) == false)
                        throw new InvalidOperationException("Initial copying failed, should never happen");
                    continue;
                }

                _hasDecoder = true;
                _decoderState = new PForDecoder.DecoderState(compressed.Length);
                TryFillUncompressed();
                Debug.Assert(_uncompressedLen > 0);
                break;
            }
        }
    }

    public class ExtraSegmentDetails
    {
        public IDisposable Scope;
        public Memory<byte> Compressed;
        public long LastValue;
        public long FirstValue;
        public int NumberOfEntries;
    }

    public static void Merge(LowLevelTransaction llt, PostingListLeafPageHeader* dest, PostingListLeafPageHeader* first, PostingListLeafPageHeader* second)
    {
        using (llt.Allocator.Allocate(Constants.Storage.PageSize, out ByteString tmp))
        {
            var tmpPtr = tmp.Ptr;
            PostingListLeafPageHeader* newHeader = (PostingListLeafPageHeader*)tmpPtr;
            newHeader->PageNumber = dest->PageNumber;
            InitLeaf(newHeader);
            for (int i = 0; i < first->NumberOfCompressedRuns; i++)
            {
                if (TryAdd(newHeader, SpanFor(first, i)) == false)
                {
                    throw new InvalidOperationException("Not enough space in new page, should never happen, caller handled that");
                }
            } 
            for (int i = 0; i < second->NumberOfCompressedRuns; i++)
            {
                if (TryAdd(newHeader, SpanFor(second, i)) == false)
                {
                    throw new InvalidOperationException("Not enough space in new page, should never happen, caller handled that");
                }
            }
            tmp.CopyTo((byte*)dest);
        }
    }

    [Conditional("DEBUG")]
    private void VerifyOtherLastValueBeforeMyFirstOne(PostingListLeafPage other)
    {
        if (other.Header->NumberOfCompressedRuns == 0)
            return;

        Iterator iterator = GetIterator();
        if (iterator.MoveNext(out var first) == false)
            return;

        long lastValueOfLastEntry = PForDecoder.ReadLast(SpanFor(other.Header->NumberOfCompressedRuns - 1));
        Debug.Assert(lastValueOfLastEntry > first);
    }
    
    public List<long> GetDebugOutput()
    {
        var list = new List<long>();
        var it = GetIterator();
        while (it.MoveNext(out long cur))
        {
            list.Add(cur);
        }
        return list;
    }

    public (long First, long Last) GetRange()
    {
        if (Header->NumberOfCompressedRuns == 0)
            return (-1, -1);
        GetIterator().MoveNext(out var first);
        long last = PForDecoder.ReadLast(SpanFor(Header->NumberOfCompressedRuns-1));

        return (first, last);
    }
}

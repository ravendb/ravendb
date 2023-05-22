using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Sparrow;
using Sparrow.Server;
using Voron.Impl;
using Voron.Util.Simd;
using Constants = Voron.Global.Constants;

namespace Voron.Data.PostingLists;

public readonly unsafe struct PostingListLeafPage
{
    private const long InvalidValue = long.MaxValue;
    
    private readonly Page _page;
    public PostingListLeafPageHeader* Header => (PostingListLeafPageHeader*)_page.Pointer;

    public int SpaceUsed => Header->SizeUsed;

    public PostingListLeafPage(Page page)
    {
        _page = page;
    }

    public static void InitLeaf(PostingListLeafPageHeader* header)
    {
        header->Flags = PageFlags.Single | PageFlags.Other;
        header->PostingListFlags = ExtendedPageType.PostingListLeaf;
        header->SizeUsed = 0;
        header->NumberOfEntries = 0;
    }
    
    /// <summary>
    /// Additions and removals are *sorted* by the caller
    /// maxValidValue is the limit for the *next* page, so we won't consume entries from there
    /// </summary>
    public Span<long> Update(LowLevelTransaction tx, NativeIntegersList tempList, ref long* additions, ref int additionsCount,
        ref long* removals, ref int removalsCount, long maxValidValue)
    {
        var maxAdditionsLimit = new Span<long>(additions, additionsCount).BinarySearch(maxValidValue);
        if (maxAdditionsLimit < 0)
            maxAdditionsLimit = ~maxAdditionsLimit;
        
        var maxRemovalsLimit = new Span<long>(removals, removalsCount).BinarySearch(maxValidValue);
        if (maxRemovalsLimit < 0)
            maxRemovalsLimit = ~maxRemovalsLimit;
      
        if (maxRemovalsLimit == 0 && maxAdditionsLimit == 0)
            return Span<long>.Empty; // nothing to do
        
        tempList.Clear();

        if (Header->NumberOfEntries == 0)
        {
            var written = AppendToNewPage(additions, maxAdditionsLimit);
            removals += maxRemovalsLimit;
            removalsCount -= maxRemovalsLimit;
            additions += written;
            additionsCount -= written;
            return Span<long>.Empty;
        }

        var tmpPageScope = tx.Allocator.Allocate(Constants.Storage.PageSize, out ByteString tmp);
        var newPagePtr = tmp.Ptr;
            
        PostingListLeafPageHeader* newHeader = (PostingListLeafPageHeader*)newPagePtr;
        Memory.Copy(newPagePtr, Header, PageHeader.SizeOf);
        InitLeaf(newHeader);

        var existingList = new NativeIntegersList(tx.Allocator, (Header->NumberOfEntries+255)/256 * 256);
        existingList.Count = ReadAllEntries(existingList.RawItems, existingList.Capacity);

        Debug.Assert(existingList.Count == Header->NumberOfEntries);

        int existingIndex = int.MaxValue,additionsIdx = 0, removalsIdx = 0;

        if (additionsCount != 0)
        {
            existingIndex = existingList.Items.BinarySearch(*additions);
            if (existingIndex < 0)
            {
                existingIndex = ~existingIndex;
            }
        }

        if (removalsCount != 0)
        {
            var removalIndex = existingList.Items.BinarySearch(*removals);
            if (removalIndex < 0)
            {
                removalIndex = ~removalIndex;
            }

            existingIndex = Math.Min(removalIndex, existingIndex);
        }

        long existingCurrent = InvalidValue, additionCurrent = InvalidValue, removalCurrent = InvalidValue;
        while (existingIndex < existingList.Count)
        {
            if (existingCurrent == InvalidValue)
            {
                existingCurrent = existingList.RawItems[existingIndex++];
            }

            if (additionsIdx < additionsCount && additionCurrent == InvalidValue) 
                additionCurrent = additions[additionsIdx++];

            if (removalsIdx < removalsCount && removalCurrent == InvalidValue) 
                removalCurrent = removals[removalsIdx++];

            AddItemToList();
        }

        int entriesCount = 0;
        int sizeUsed = 0;
        Span<long> remainder = Span<long>.Empty;
        if (tempList.Count > 0)
        {
            (entriesCount, sizeUsed) = SimdBitPacker<SortedDifferentials>.Encode(tempList.RawItems, tempList.Count, newPagePtr + PageHeader.SizeOf,
                Constants.Storage.PageSize - PageHeader.SizeOf);
            if (entriesCount < tempList.Count)
            {
                remainder = tempList.Items[entriesCount..];
            }
        }
        // we still have items to add, let's add them as the next item here
        if (entriesCount == tempList.Count && additionsIdx < additionsCount)
        {
            (int secondCount,  int sizeUsedSecond) = SimdBitPacker<SortedDifferentials>.Encode(additions + additionsIdx, additionsCount - additionsIdx,
                newPagePtr + PageHeader.SizeOf + sizeUsed,
                Constants.Storage.PageSize - PageHeader.SizeOf - sizeUsed);
            additionsIdx += secondCount;
            entriesCount += secondCount;
            sizeUsed += sizeUsedSecond;
        }

        Debug.Assert(sizeUsed < Constants.Storage.PageSize);
        newHeader->SizeUsed = (ushort)sizeUsed;
        newHeader->NumberOfEntries = entriesCount;
        // clear the parts we aren't using
        Memory.Set(newPagePtr + PageHeader.SizeOf + sizeUsed, 0, Constants.Storage.PageSize - (PageHeader.SizeOf + sizeUsed));
        Memory.Copy(Header, newPagePtr, Constants.Storage.PageSize);
            
        existingList.Dispose();
        tmpPageScope.Dispose();
        
        additions += additionsIdx;
        additionsCount -= additionsIdx;
        removals += removalsIdx;
        removalsCount -= removalsIdx;

        return remainder;
        
        void AddItemToList()
        {
            long current;
            if (additionCurrent < existingCurrent || existingCurrent == InvalidValue)
            {
                current = additionCurrent;
                additionCurrent = InvalidValue;
            }
            else if (additionCurrent == existingCurrent)
            {
                current = additionCurrent;
                additionCurrent = InvalidValue;
                existingCurrent = InvalidValue;
            }
            else // existingCurrent > existingCurrent
            {
                current = existingCurrent;
                existingCurrent = InvalidValue;
            }

            if (removalCurrent < current)
            {
                removalCurrent = InvalidValue;
            }
            else if (removalCurrent == current)
            {
                removalCurrent = InvalidValue;
            }
            else
            {
                tempList.Add(current);
            }
        }
    }
    
      /// <summary>
    /// Additions and removals are *sorted* by the caller
    /// maxValidValue is the limit for the *next* page, so we won't consume entries from there
    /// </summary>
    public int AppendToNewPage(long* additions, int additionsCount)
    {
        PostingListLeafPageHeader* newHeader = (PostingListLeafPageHeader*)_page.Pointer;
        InitLeaf(newHeader);

        (int entriesCount, int sizeUsed) = SimdBitPacker<SortedDifferentials>.Encode(additions, additionsCount, _page.DataPointer,
            Constants.Storage.PageSize - PageHeader.SizeOf);

        Debug.Assert(sizeUsed < Constants.Storage.PageSize);
        newHeader->SizeUsed = (ushort)sizeUsed;
        newHeader->NumberOfEntries = entriesCount;
        // clear the parts we aren't using
        Memory.Set(_page.DataPointer+ sizeUsed, 0, Constants.Storage.PageSize - (PageHeader.SizeOf + sizeUsed));

        return entriesCount;
    }

    public struct Iterator
    {
        private SimdBitPacker<SortedDifferentials>.Reader _reader;

        public Iterator(byte* start, int sizeUsed)
        {
            _reader = new SimdBitPacker<SortedDifferentials>.Reader(start, sizeUsed);
        }

        public int Fill(Span<long> matches, out bool hasPrunedResults, long pruneGreaterThanOptimization)
        {
            int totalRead = 0;
            hasPrunedResults = false;
            fixed (long* m = matches)
            {
                while (totalRead < matches.Length)
                {
                    var r = _reader.Fill(m + totalRead, matches.Length - totalRead);
                    if (r == 0)
                        break;

                    totalRead += r;
                    
                    if (pruneGreaterThanOptimization > m[totalRead - 1])
                    {
                        hasPrunedResults = true;
                        break;
                    }
                }

                return totalRead;
            }
        }
        

        /// <summary>
        ///  This will find the *range* in which there are values
        ///  that are greater or equal to from, not the exact match
        /// </summary>
        public bool SkipHint(long from)
        {
            if (from == long.MinValue)
                return true; // nothing to do here
            
            var buffer = stackalloc long[256];
            while (true)
            {
                var previous = _reader;
                var read = _reader.Fill(buffer, 256);
                if (read == 0)
                {
                    return false; // not found
                }

                if (from > buffer[read - 1]) 
                    continue;
                
                // we setup the *next* call to read this again
                _reader = previous;
                return true;
            }
        }
    }

    private int ReadAllEntries(long* existing, int count)
    {
        int existingCount = 0;
        var reader = new SimdBitPacker<SortedDifferentials>.Reader(_page.DataPointer, Header->SizeUsed);
        while (true) 
        {
            var read = reader.Fill(existing + existingCount, count - existingCount);
            if (read == 0)
                break;
            existing += read;
        }

        return existingCount;
    }

    public List<long> GetDebugOutput()
    {
        var buf = new long[(Header->NumberOfEntries + 255)/256 * 256];
        fixed (long* f = buf)
        {
            ReadAllEntries(f, buf.Length);
        }
        return buf.Take(Header->NumberOfEntries).ToList();
    }

    public Iterator GetIterator()
    {
        return new Iterator(_page.DataPointer, Header->SizeUsed);
    }

    public static void Merge(ByteStringContext allocator,
        PostingListLeafPageHeader* dest, PostingListLeafPageHeader* first, PostingListLeafPageHeader* second)
    {
        var scope = allocator.Allocate(Constants.Storage.PageSize, out ByteString tmp);
        var tmpPtr = tmp.Ptr;
        var newHeader = (PostingListLeafPageHeader*)tmpPtr;
        InitLeaf(newHeader);
        
        Memory.Copy((byte*)dest + PageHeader.SizeOf,
            (byte*)first + PageHeader.SizeOf,
            first->SizeUsed
            );
        dest->SizeUsed += first->SizeUsed;
        dest->NumberOfEntries += first->NumberOfEntries;
        
        Memory.Copy((byte*)dest + PageHeader.SizeOf + dest->SizeUsed,
            (byte*)second + PageHeader.SizeOf,
            second->SizeUsed
        );
        dest->SizeUsed += second->SizeUsed;
        dest->NumberOfEntries += second->NumberOfEntries;

        Memory.Set((byte*)dest + PageHeader.SizeOf + dest->SizeUsed, 0,
            Constants.Storage.PageSize - (PageHeader.SizeOf + dest->SizeUsed));
        
        tmp.CopyTo((byte*)dest);
        
        scope.Dispose();
    }
}

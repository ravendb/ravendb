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
    public int Update(LowLevelTransaction tx, NativeIntegersList tempList, ref long* additions, ref int additionsCount,
        ref long* removals, ref int removalsCount, long maxValidValue)
    {
        var maxAdditionsLimit = new Span<long>(additions, additionsCount).BinarySearch(maxValidValue);
        if (maxAdditionsLimit < 0)
            maxAdditionsLimit = ~maxAdditionsLimit;
        
        var maxRemovalsLimit = new Span<long>(removals, removalsCount).BinarySearch(maxValidValue);
        if (maxRemovalsLimit < 0)
            maxRemovalsLimit = ~maxRemovalsLimit;
      
        if (maxRemovalsLimit == 0 && maxAdditionsLimit == 0)
            return 0; // nothing to do
            
        tempList.Clear();

        var tmpPageScope = tx.Allocator.Allocate(Constants.Storage.PageSize, out ByteString tmp);
        var newPagePtr = tmp.Ptr;
            
        PostingListLeafPageHeader* newHeader = (PostingListLeafPageHeader*)newPagePtr;
        Memory.Copy(newPagePtr, Header, PageHeader.SizeOf);
        InitLeaf(newHeader);

        var existingBufferScope = tx.Allocator.Allocate(sizeof(long) * Header->NumberOfEntries, out tmp);
        long* existing = (long*)tmp.Ptr;
        int existingCount = ReadAllEntries(existing);

        Debug.Assert(existingCount == Header->NumberOfEntries);

        int existingIndex = 0, additionsIdx = 0, removalsIdx = 0;
        long existingCurrent = -1, additionCurrent = -1, removalCurrent = -1;
        while (existingIndex < existingCount)
        {
            if (existingCurrent == -1)
            {
                existingCurrent = existing[existingIndex++];
                if (existingIndex == existingCount)
                {
                    AddItemToList();
                    break;
                }
            }

            if (additionsIdx < additionsCount && additionCurrent == -1) 
                additionCurrent = additions[additionsIdx++];

            if (removalsIdx < removalsCount && removalCurrent == -1) 
                removalCurrent = removals[removalsIdx++];

            AddItemToList();
        }

        (int entriesCount, int sizeUsed) = SimdBitPacker<SortedDifferentials>.Encode(tempList.RawItems, tempList.Count, newPagePtr + PageHeader.SizeOf,
            Constants.Storage.PageSize - PageHeader.SizeOf);
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
        Memory.Set(newPagePtr + PageHeader.SizeOf + sizeUsed, 0, Constants.Storage.PageSize - PageHeader.SizeOf + sizeUsed);
        Memory.Copy(Header, newPagePtr, Constants.Storage.PageSize);
            
        existingBufferScope.Dispose();
        tmpPageScope.Dispose();
        
        additions += additionsIdx;
        additionsCount -= additionsIdx;
        removals += removalsIdx;
        removalsCount -= removalsIdx;
        return tempList.Count - entriesCount;
        
        void AddItemToList()
        {
            long current;
            if (additionCurrent < existingCurrent || existingCurrent == -1)
            {
                current = additionCurrent;
                additionCurrent = -1;
            }
            else if (additionCurrent == existingCurrent)
            {
                current = additionCurrent;
                additionCurrent = -1;
                existingCurrent = -1;
            }
            else // existingCurrent > existingCurrent
            {
                current = existingCurrent;
                existingCurrent = -1;
            }

            if (removalCurrent < current)
            {
                removalCurrent = -1;
            }
            else if (removalCurrent == current)
            {
                removalCurrent = -1;
            }
            else
            {
                tempList.Add(current);
            }
        }
    }

    public struct Iterator
    {
        private readonly byte* _endOfData;
        private SimdBitPacker<SortedDifferentials>.Reader _reader;

        public Iterator(byte* start, int sizeUsed)
        {
            _reader = new SimdBitPacker<SortedDifferentials>.Reader
            {
                Offset = start
            };
            _endOfData = start + sizeUsed;
        }

        public int Fill(Span<long> matches, out bool hasPrunedResults, long pruneGreaterThanOptimization)
        {
            int read = 0;
            hasPrunedResults = false;
            fixed (long* m = matches)
            {
                while (read < matches.Length)
                {
                    var r = _reader.Fill(m + read, matches.Length - read);
                    if (r == 0)
                    {
                        if (_reader.Offset == _endOfData)
                            break;
                        _reader.MoveToNextHeader();
                        continue;
                    }

                    read += read;
                    
                    if (pruneGreaterThanOptimization > m[read - 1])
                    {
                        hasPrunedResults = true;
                        break;
                    }
                }

                return read;
            }
        }
    }

    private int ReadAllEntries(long* existing)
    {
        int existingCount = 0;
        var offset = _page.DataPointer;
        var endOfData = offset + Header->SizeUsed;
        var reader = new SimdBitPacker<SortedDifferentials>.Reader { Offset = offset };
        while (reader.Offset < endOfData) // typically should have up to two of them...
        {
            reader.MoveToNextHeader();
            var read = reader.Fill(existing + existingCount, Header->NumberOfEntries);
            offset += read;
            existingCount += read;
        }

        return existingCount;
    }

    public List<long> GetDebugOutput()
    {
        var buf = new long[Header->NumberOfEntries];
        fixed (long* f = buf)
        {
            ReadAllEntries(f);
        }
        return buf.ToList();
    }

    public Iterator GetIterator()
    {
        return new Iterator(_page.DataPointer, Header->SizeUsed);
    }
}

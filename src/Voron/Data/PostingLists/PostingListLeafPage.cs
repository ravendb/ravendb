using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Voron.Impl;
using Voron.Util.PFor;
using Voron.Util.Simd;
using Constants = Voron.Global.Constants;

namespace Voron.Data.PostingLists;

public readonly unsafe struct PostingListLeafPage
{
    public const int MinimumSizeOfBuffer = 256;

    public static int GetNextValidBufferSize(int size)
    {
        return (size + 255) / 256 * 256;
    }
    
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
    public void Update(LowLevelTransaction tx, FastPForEncoder encoder, ref NativeIntegersList tempList, ref long* additions, ref int additionsCount,
        ref long* removals, ref int removalsCount, long maxValidValue)
    {
        var maxAdditionsLimit = new Span<long>(additions, additionsCount).BinarySearch(maxValidValue);
        if (maxAdditionsLimit < 0)
            maxAdditionsLimit = ~maxAdditionsLimit;
        
        var maxRemovalsLimit = new Span<long>(removals, removalsCount).BinarySearch(maxValidValue);
        if (maxRemovalsLimit < 0)
            maxRemovalsLimit = ~maxRemovalsLimit;
      
        if (maxRemovalsLimit == 0 && maxAdditionsLimit == 0)
            return; // nothing to do
        
        tempList.Clear();

        if (Header->NumberOfEntries == 0)
        {
            encoder.Encode(additions, maxAdditionsLimit);
            
            var written = AppendToNewPage(encoder);
            removals += maxRemovalsLimit;
            removalsCount -= maxRemovalsLimit;
            additions += written;
            additionsCount -= written;
            return;
        }

        var tmpPageScope = tx.Allocator.Allocate(Constants.Storage.PageSize, out ByteString tmp);
        var newPagePtr = tmp.Ptr;
            
        PostingListLeafPageHeader* newHeader = (PostingListLeafPageHeader*)newPagePtr;
        Memory.Copy(newPagePtr, Header, PageHeader.SizeOf);
        InitLeaf(newHeader);

        var existingList = new NativeIntegersList(tx.Allocator, (Header->NumberOfEntries+255)/256 * 256);
        existingList.Count = ReadAllEntries(tx.Allocator,existingList.RawItems, existingList.Capacity);

        Debug.Assert(existingList.Count == Header->NumberOfEntries);

        int existingIndex = 0, additionsIdx = 0, removalsIdx = 0;

        long existingCurrent = InvalidValue, additionCurrent = InvalidValue, removalCurrent = InvalidValue;
        while (true)
        {
            if (existingIndex < existingList.Count && existingCurrent == InvalidValue) 
                existingCurrent = existingList.RawItems[existingIndex++];

            if (additionsIdx < maxAdditionsLimit && additionCurrent == InvalidValue) 
                additionCurrent = additions[additionsIdx++];

            if (removalsIdx < maxRemovalsLimit && removalCurrent == InvalidValue) 
                removalCurrent = removals[removalsIdx++];

            if (additionCurrent == InvalidValue && existingCurrent == InvalidValue)
                break;
            
            AddItemToList(ref tempList);
        }

        int entriesCount = 0;
        int sizeUsed = 0;
        
        if (tempList.Count > 0)
        {
            encoder.Encode(tempList.RawItems, tempList.Count);
            (entriesCount, sizeUsed) = encoder.Write(newPagePtr + PageHeader.SizeOf, Constants.Storage.PageSize - PageHeader.SizeOf);
            Debug.Assert(entriesCount > 0);
        }

        Debug.Assert(sizeUsed <= Constants.Storage.PageSize - PageHeader.SizeOf);
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
        
        void AddItemToList(ref NativeIntegersList list)
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
                list.Add(current);
            }
        }
    }
    
     /// <summary>
    /// Additions and removals are *sorted* by the caller
    /// maxValidValue is the limit for the *next* page, so we won't consume entries from there
    /// </summary>
    public int AppendToNewPage(FastPForEncoder encoder)
    {
        PostingListLeafPageHeader* newHeader = (PostingListLeafPageHeader*)_page.Pointer;
        InitLeaf(newHeader);

        (int entriesCount, int sizeUsed) = encoder.Write(_page.DataPointer, Constants.Storage.PageSize - PageHeader.SizeOf);

        Debug.Assert(entriesCount > 0);
        Debug.Assert(sizeUsed < Constants.Storage.PageSize);
        newHeader->SizeUsed = (ushort)sizeUsed;
        newHeader->NumberOfEntries = entriesCount;
        // clear the parts we aren't using
        Memory.Set(_page.DataPointer+ sizeUsed, 0, Constants.Storage.PageSize - (PageHeader.SizeOf + sizeUsed));

        return entriesCount;
    }

    public struct Iterator
    {
        private FastPForBufferedReader _reader;

        public Iterator(ByteStringContext allocator ,byte* start, int sizeUsed)
        {
            _reader = new FastPForBufferedReader(allocator, start, sizeUsed);
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
                    
                    if (m[totalRead - 1] >= pruneGreaterThanOptimization)
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
            
            var buffer = stackalloc long[MinimumSizeOfBuffer];
            // we *copy* the reader to do the search without modifying 
            //  the original position
            var innerReader = _reader.Decoder;
            while (true)
            {
                var read = innerReader.Read(buffer, MinimumSizeOfBuffer);
                if (read == 0)
                {
                    return false; // not found
                }

                if (from > buffer[read - 1]) 
                    continue;
                
                return true;
            }
        }
    }

    private int ReadAllEntries(ByteStringContext context, long* existing, int count)
    {
        int existingCount = 0;
        var reader = new FastPForBufferedReader(context, _page.DataPointer, Header->SizeUsed);
        while (true) 
        {
            var read = reader.Fill(existing + existingCount, count - existingCount);
            existingCount += read;
            if (read == 0)
            {
                reader.Dispose();
                return existingCount;
            }
        }
    }

    public List<long> GetDebugOutput()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        var buf = new long[(Header->NumberOfEntries + 255)/256 * 256];
        fixed (long* f = buf)
        {
            ReadAllEntries(bsc, f, buf.Length);
        }
        return buf.Take(Header->NumberOfEntries).ToList();
    }

    public Iterator GetIterator(ByteStringContext allocator)
    {
        return new Iterator(allocator, _page.DataPointer, Header->SizeUsed);
    }

    public static void Merge(
        ByteStringContext allocator, ref FastPForDecoder fastPForDecoder,
        PostingListLeafPageHeader* dest, PostingListLeafPageHeader* first, PostingListLeafPageHeader* second)
    {
        var scope = allocator.Allocate(Constants.Storage.PageSize, out ByteString tmp);
        var tmpPtr = tmp.Ptr;
        var newHeader = (PostingListLeafPageHeader*)tmpPtr;
        newHeader->PageNumber = dest->PageNumber;
        InitLeaf(newHeader);

        // using +256 here to ensure that we always have at least 256 available in the buffer
        var mergedList = new NativeIntegersList(allocator, first->NumberOfEntries + second->NumberOfEntries + 256);

        if (first->SizeUsed > 0)
        {
            fastPForDecoder.Init((byte*)first + PageHeader.SizeOf, first->SizeUsed);
            mergedList.Count += fastPForDecoder.Read(mergedList.RawItems, mergedList.Capacity);
        }

        if (second->SizeUsed > 0)
        {
            fastPForDecoder.Init((byte*)second + PageHeader.SizeOf, second->SizeUsed);
            mergedList.Count += fastPForDecoder.Read(mergedList.RawItems + first->NumberOfEntries, mergedList.Capacity - first->NumberOfEntries);
        }

        var encoder = new FastPForEncoder(allocator);
        int reqSize = 0;
        if (mergedList.Count > 0)
        {
            reqSize = encoder.Encode(mergedList.RawItems, mergedList.Count);
            Debug.Assert(reqSize < Constants.Storage.PageSize - PageHeader.SizeOf);
            encoder.Write(tmpPtr + PageHeader.SizeOf, Constants.Storage.PageSize - PageHeader.SizeOf);
        }
        mergedList.Dispose();

        newHeader->SizeUsed = (ushort)reqSize;
        newHeader->NumberOfEntries =  mergedList.Count;

        Memory.Set((byte*)dest + PageHeader.SizeOf + dest->SizeUsed, 0,
            Constants.Storage.PageSize - (PageHeader.SizeOf + dest->SizeUsed));
        
        tmp.CopyTo((byte*)dest);
        
        scope.Dispose();
    }
}

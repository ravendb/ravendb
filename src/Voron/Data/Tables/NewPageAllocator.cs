using System;
using System.Collections.Generic;
using Sparrow;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.Tables
{
    /// <summary>
    /// This class is responsible for allocating pages for tables for the 
    /// indexes. This is important so we'll have high degree of locality 
    /// for those indexes, instead of having to search throughout the data
    /// file.
    /// 
    /// This is done by storing the information by preallocating
    /// data in batches of NumberOfPagesInSection pages at a time and
    /// trying to allocate the value near to the parent page as we can get.
    /// 
    /// Note that overflow pages are always allocated externally (they shouldn't
    /// happen in indexes, and complicate the code significantly).
    /// 
    /// All the indexes for a table are using the same allocator, so they will reside 
    /// nearby, the idea is that this should give the OS a hint that this is a hot
    /// portion of memory, and it will be loaded early.
    /// 
    /// Global indexes have a dedicated global allocator
    /// </summary>
    public class NewPageAllocator
    {
        private readonly LowLevelTransaction _llt;
        private readonly Tree _parentTree;
        internal static readonly Slice AllocationStorage;
        private const byte BitmapSize = sizeof(long)*4;
        private const int NumberOfPagesInSection = BitmapSize*8;
        public const string AllocationStorageName = "Allocation-Storage";

        static NewPageAllocator()
        {
            Slice.From(StorageEnvironment.LabelsContext, AllocationStorageName, ByteStringType.Immutable,
                out AllocationStorage);
        }


        public NewPageAllocator(LowLevelTransaction llt, Tree parentTree)
        {
            _llt = llt;
            _parentTree = parentTree;
        }

        public void Create()
        {
            var fixedSizeTree = _parentTree.FixedTreeFor(AllocationStorage, valSize: BitmapSize);
            if (fixedSizeTree.NumberOfEntries != 0)
                return;

            AllocateMoreSpace(fixedSizeTree);
        }

        private unsafe Page AllocateMoreSpace(FixedSizeTree fst)
        {
            var allocatePage = _llt.AllocatePage(NumberOfPagesInSection);
            _llt.BreakLargeAllocationToSeparatePages(allocatePage.PageNumber);

            var initialPageNumber = allocatePage.PageNumber;

            bool isNew;
            var ptr = fst.DirectAdd(initialPageNumber, out isNew);
            if (isNew == false)
                ThrowInvalidExistingBuffer();
            Memory.Set(ptr, 0, BitmapSize); // mark all pages as free 
            return allocatePage;
        }

        private static void ThrowInvalidExistingBuffer()
        {
            throw new InvalidOperationException("Invalid attempt to create a new buffer, but it was already there");
        }

        public unsafe Page AllocateSinglePage(long nearbyPage)
        {
            var fst = _parentTree.FixedTreeFor(AllocationStorage, valSize: BitmapSize);
            using (var it = fst.Iterate())
            {
                Page page;
                if (it.Seek(nearbyPage)) // found a value >= from the nearby page
                {
                    if (it.CurrentKey > nearbyPage)
                    {
                        // go back one step if we can
                        if (it.MovePrev() == false)
                            it.Seek(nearbyPage); // if we can't, go back to the original find
                    }
                }
                else // probably it is on the last entry, after the first page, so we'll use the last entry
                {
                    if (it.SeekToLast() == false)
                    {
                        // shouldn't actuallly happen, but same behavior as running out of space
                        page = AllocateMoreSpace(fst);
                        SetValue(fst, page.PageNumber, 0);
                        return page;
                    }
                }
                var startPage = it.CurrentKey;
                while (true)
                {
                    Slice slice;
                    using (it.Value(out slice))
                    {
                        var hasSpace = false;
                        var buffer = (ulong*) (slice.Content.Ptr);
                        for (int i = 0; i < BitmapSize / sizeof(ulong); i++)
                        {
                            if (buffer[i] != ulong.MaxValue)
                            {
                                hasSpace = true;
                                break;
                            }
                        }
                        if (hasSpace == false)
                        {
                            if (TryMoveNextCyclic(it, startPage) == false)
                                break;
                        }
                        for (int i = 0; i < BitmapSize*8; i++)
                        {
                            if (GetBitInBuffer(i, slice.Content.Ptr) == false)
                            {
                                var currentSectionStart = it.CurrentKey;
                                SetValue(fst, currentSectionStart, i);
                                
                                return _llt.ModifyPage(currentSectionStart + i);
                            }
                        }
                        if (TryMoveNextCyclic(it, startPage) == false)
                            break;
                    }
                }
                page = AllocateMoreSpace(fst);
                SetValue(fst, page.PageNumber, 0);
                return page;
            }
        }

        private static bool TryMoveNextCyclic(FixedSizeTree.IFixedSizeIterator it, long startPage)
        {
            if (it.MoveNext())
            {
                if (it.CurrentKey == startPage)
                    return false; // we went full circle
                return true;
            }
            // we run out space to move forward, let us go to the start and search from there
            return it.Seek(0) && it.CurrentKey != startPage;
        }

        private unsafe void SetValue(FixedSizeTree fst, long pageNumber, int positionInBitmap)
        {
            bool isNew;
            var ptr = fst.DirectAdd(pageNumber, out isNew);
            if (isNew)
                ThrowInvalidNewBuffer();

            ptr[positionInBitmap/8] |= (byte) (1 << (positionInBitmap%8));
        }

        private unsafe void UnsetValue(FixedSizeTree fst, long pageNumber, int positionInBitmap)
        {
            bool isNew;
            var ptr = fst.DirectAdd(pageNumber, out isNew);
            if (isNew)
                ThrowInvalidNewBuffer();

            ptr[positionInBitmap/8] &= (byte) ~(1 << (positionInBitmap%8));
        }

        private static unsafe bool GetBitInBuffer(int positionInBitmap, byte* ptr)
        {
            return (ptr[positionInBitmap/8] & (1 << (positionInBitmap%8))) != 0;
        }

        private static void ThrowInvalidNewBuffer()
        {
            throw new InvalidOperationException("Invalid attempt to set value on non existing buffer");
        }

        public unsafe void FreePage(long pageNumber)
        {
            var fst = _parentTree.FixedTreeFor(AllocationStorage, valSize: BitmapSize);
            using (var it = fst.Iterate())
            {
                if (it.Seek(pageNumber) == false) // this is past the end of the section
                {
                    if (it.SeekToLast() == false) // so go to the first section
                        ThrowInvalidEmptySectionState(pageNumber);
                }
                else if (it.CurrentKey != pageNumber) // if we aren't exactly on the start of the section
                {
                    if (it.MovePrev() == false)
                        ThrowInvalidPageReleased(pageNumber);
                }

                if (it.CurrentKey > pageNumber || 
                    it.CurrentKey + NumberOfPagesInSection < pageNumber)
                    ThrowInvalidPageReleased(pageNumber);

                var positionInBuffer = (int) (pageNumber - it.CurrentKey);
                UnsetValue(fst, it.CurrentKey, positionInBuffer);
                var page = _llt.ModifyPage(pageNumber);
                Memory.Set(page.Pointer, 0, Constants.Storage.PageSize);
                page.PageNumber = pageNumber;
                page.Flags = PageFlags.Single;
            }
        }

        public unsafe int GetNumberOfPreAllocatedFreePages()
        {
            var fst = _parentTree.FixedTreeFor(AllocationStorage, valSize: BitmapSize);

            if (fst.NumberOfEntries == 0)
                return 0;

            var count = 0;

            using (var it = fst.Iterate())
            {
                if (it.Seek(long.MinValue) == false)
                    throw new InvalidOperationException($"Could not seek to the first element of {fst.Name} tree");

                Slice slice;
                using (it.Value(out slice))
                {
                    for (int i = 0; i < NumberOfPagesInSection; i++)
                    {
                        if (GetBitInBuffer(i, slice.Content.Ptr) == false)
                        {
                            count++;
                        }
                    }
                }
            }

            return count;
        }

        internal FixedSizeTree GetAllocationStorageFst()
        {
            return _parentTree.FixedTreeFor(AllocationStorage, valSize: BitmapSize);
        }

        private static void ThrowInvalidPageReleased(long pageNumber)
        {
            throw new InvalidOperationException("Tried to released page " + pageNumber +
                                                " but couldn't find it in the allocation section");
        }

        private static void ThrowInvalidEmptySectionState(long pageNumber)
        {
            throw new InvalidOperationException("Tried to return " + pageNumber +
                                                " but there are no values in the allocator section, invalid state.");
        }

        public static void MaybePrefetchSections(Tree parentTree, LowLevelTransaction llt)
        {
            var fst = parentTree.FixedTreeFor(AllocationStorage, valSize: BitmapSize);
            using (var it = fst.Iterate())
            {
                if (it.Seek(long.MinValue) == false)
                    return;

                var list = new List<long>((int)(fst.NumberOfEntries * NumberOfPagesInSection));

                do
                {
                    for (int i = 0; i < NumberOfPagesInSection; i++)
                    {
                        list.Add(i + it.CurrentKey);
                    }
                    
                } while (it.MoveNext());

                llt.Environment.Options.DataPager.MaybePrefetchMemory(list);
            }
        }
    }
}
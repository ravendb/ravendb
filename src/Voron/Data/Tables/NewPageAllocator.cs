using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Platform;
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
        private const byte BitmapSize = sizeof(long) * 4;
        internal const int NumberOfPagesInSection = BitmapSize * 8;
        public const string AllocationStorageName = "Allocation-Storage";

        static NewPageAllocator()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, AllocationStorageName, ByteStringType.Immutable,
                    out AllocationStorage);
            }
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
            int numberOfPagesToAllocate = GetNumberOfPagesToAllocate();

            var allocatePage = _llt.AllocatePage(numberOfPagesToAllocate);
            _llt.BreakLargeAllocationToSeparatePages(allocatePage.PageNumber);

            var initialPageNumber = allocatePage.PageNumber;

            bool isNew;
            byte* ptr;
            using (fst.DirectAdd(initialPageNumber, out isNew, out ptr))
            {
                if (isNew == false)
                    ThrowInvalidExistingBuffer();

                // in 32 bits, we pre-allocate just 256 KB, not 2MB
                Debug.Assert(numberOfPagesToAllocate % 8 == 0);
                Debug.Assert(numberOfPagesToAllocate % 8 <= BitmapSize);
                Memory.Set(ptr, 0xFF, BitmapSize); // mark the pages that we haven't allocated as busy
                Memory.Set(ptr, 0, numberOfPagesToAllocate / 8); // mark just the first part as free

            }
            return allocatePage;
        }

        private unsafe int GetNumberOfPagesToAllocate()
        {
            int numberOfPagesToAllocate;
            if (_llt.Environment.Options.ForceUsing32BitsPager || PlatformDetails.Is32Bits)
                numberOfPagesToAllocate = BitmapSize;             // 256 KB
            else
                numberOfPagesToAllocate = NumberOfPagesInSection; // 2 MB
            return numberOfPagesToAllocate;
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
                        var buffer = (ulong*)(slice.Content.Ptr);
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
                        for (int i = 0; i < BitmapSize * 8; i++)
                        {
                            if (PtrBitVector.GetBitInPointer(buffer, i) == false)
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
            byte* ptr;
            using (fst.DirectAdd(pageNumber, out isNew, out ptr))
            {
                if (isNew)
                    ThrowInvalidNewBuffer();

                PtrBitVector.SetBitInPointer(ptr, positionInBitmap, true);
                // ptr[positionInBitmap / 8] |= (byte) (1 << (positionInBitmap % 8));
            }
        }

        private unsafe void UnsetValue(FixedSizeTree fst, long pageNumber, int positionInBitmap)
        {
            bool isNew;
            byte* ptr;
            using (fst.DirectAdd(pageNumber, out isNew, out ptr))
            {
                if (isNew)
                    ThrowInvalidNewBuffer();

                PtrBitVector.SetBitInPointer(ptr, positionInBitmap, false);
                // ptr[positionInBitmap / 8] &= (byte) ~(1 << (positionInBitmap % 8));
            }
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

                var positionInBuffer = (int)(pageNumber - it.CurrentKey);
                UnsetValue(fst, it.CurrentKey, positionInBuffer);
                var page = _llt.ModifyPage(pageNumber);
                Memory.Set(page.Pointer, 0, Constants.Storage.PageSize);
                page.PageNumber = pageNumber;
                page.Flags = PageFlags.Single;
            }
        }

        public unsafe Report GetNumberOfPreAllocatedFreePages()
        {
            var fst = _parentTree.FixedTreeFor(AllocationStorage, valSize: BitmapSize);

            if (fst.NumberOfEntries == 0)
                return new Report();

            var free = 0;

            using (var it = fst.Iterate())
            {
                if (it.Seek(long.MinValue) == false)
                    throw new InvalidOperationException($"Could not seek to the first element of {fst.Name} tree");

                Slice slice;
                using (it.Value(out slice))
                {
                    byte* ptr = slice.Content.Ptr;
                    for (int i = 0; i < NumberOfPagesInSection; i++)
                    {
                        if (PtrBitVector.GetBitInPointer(ptr, i) == false)
                        {
                            free++;
                        }
                    }
                }
            }

            int amountOfPagesActuallyAllocated = GetNumberOfPagesToAllocate();

            return new Report
            {
                NumberOfOriginallyAllocatedPages = fst.NumberOfEntries * amountOfPagesActuallyAllocated,
                NumberOfFreePages = free
            };
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

        private struct SectionsIterator : IEnumerator<long>
        {
            private readonly FixedSizeTree.IFixedSizeIterator _iterator;
            private int _index;
            private bool _isDone;

            public SectionsIterator(FixedSizeTree.IFixedSizeIterator iterator)
            {
                this._iterator = iterator;
                this._index = 0;
                this._isDone = false;

                this.Current = -1;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveNext()
            {
                if (this._index >= NumberOfPagesInSection)
                {
                    this._isDone = !_iterator.MoveNext();
                    if (_isDone)
                        return false;

                    this._index = 0;
                    this.Current = _iterator.CurrentKey;
                }
                else
                {
                    this.Current = this._index + _iterator.CurrentKey;
                    this._index++;
                }
                return true;
            }

            public void Reset()
            {
                throw new NotSupportedException($"{nameof(SectionsIterator)} does not support reset operations.");
            }

            public long Current
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get;
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                private set;
            }

            object IEnumerator.Current => Current;
            public void Dispose() { }
        }

        public static void MaybePrefetchSections(Tree parentTree, LowLevelTransaction llt)
        {
            var fst = parentTree.FixedTreeFor(AllocationStorage, valSize: BitmapSize);
            if (fst.NumberOfEntries > 4)
                return; // PERF: We will avoid to over-prefetch, specially when we are doing so adaptively.

            using (var it = fst.Iterate())
            {
                if (it.Seek(long.MinValue) == false)
                    return;

                llt.Environment.Options.DataPager.MaybePrefetchMemory(new SectionsIterator(it));
            }
        }

        public class Report
        {
            public long NumberOfFreePages { get; set; }

            public long NumberOfOriginallyAllocatedPages { get; set; }
        }
    }
}

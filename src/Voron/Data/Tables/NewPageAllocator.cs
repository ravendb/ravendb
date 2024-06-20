using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Platform;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Impl;
using Constants = Voron.Global.Constants;
using System.Diagnostics.CodeAnalysis;

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
    public sealed class NewPageAllocator
    {
        private readonly LowLevelTransaction _llt;
        private readonly Tree _parentTree;
        internal static readonly Slice AllocationStorage;
        internal static readonly Slice AllocationStorageSize;
        private int _numberOfPagesToAllocate;
        private const byte BitmapSize = sizeof(long)*4;
        internal const int NumberOfPagesInSection = BitmapSize*8;
        public const string AllocationStorageName = "Allocation-Storage";
        public const string AllocationStorageSizeName = "Allocation-Storage-Size";

        static NewPageAllocator()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, AllocationStorageName, ByteStringType.Immutable,
                    out AllocationStorage);

                Slice.From(ctx, AllocationStorageSizeName, ByteStringType.Immutable,
                    out AllocationStorageSize);
            }
        }


        public NewPageAllocator(LowLevelTransaction llt, Tree parentTree)
        {
            _llt = llt;
            _parentTree = parentTree;

            _numberOfPagesToAllocate = NumberOfPagesInSection;

            var fixedSizeTreeSize = _parentTree.FixedTreeFor(AllocationStorageSize, valSize: sizeof(int));
            using (fixedSizeTreeSize.Read(0, out var slice))
            {
                if (slice.HasValue)
                {
                    _numberOfPagesToAllocate = slice.CreateReader().ReadLittleEndianInt32();
                }
            }
        }

        public void Create()
        {
            // we need to decide what is the size that we'll be allocating upfront
            // we use different values for 32 / 64 bits system, but we *remember* what is
            // the first value that we allocated on, and then re-use this value regardless
            // of whatever we are *currently* running on 32/64 bits.
            // for the most part, we'll usually run in the same environment, so there is no
            // change here. The key is that we can now handle it better with integration.

            var fixedSizeTree = _parentTree.FixedTreeFor(AllocationStorage, valSize: BitmapSize);

            // we store this as a fixed size tree with a single element because we can't store raw values inside the RootObjects
            var fixedSizeTreeSize = _parentTree.FixedTreeFor(AllocationStorageSize, valSize: sizeof(int));

            if (fixedSizeTreeSize.NumberOfEntries == 0)
            {
                _numberOfPagesToAllocate = fixedSizeTree.NumberOfEntries != 0 ?
                    // It's always safe to use 64 bits size, even if we started out in 32 bits.
                    // So we'll default to that. This may cause changes in existing deployments on 32 bits, 
                    // but the cost of creating the additional value is relatively small optimization, so 
                    // it is safe to go with it.
                    NumberOfPagesInSection :
                    // if there are no values, we are new allocator, and can decide (and remember) based on
                    // the current bit value
                    ComputeNumberOfPagesToAllocate();

                fixedSizeTreeSize.Add(0, BitConverter.GetBytes(_numberOfPagesToAllocate));
            }
            else
            {
                using (fixedSizeTreeSize.Read(0, out var slice))
                {
                    _numberOfPagesToAllocate = slice.CreateReader().ReadLittleEndianInt32();
                }
            }

            if (fixedSizeTree.NumberOfEntries != 0)
                return;

            AllocateMoreSpace(fixedSizeTree);
        }


        private unsafe Page AllocateMoreSpace(FixedSizeTree fst)
        {
            var allocatePage = _llt.AllocateMultiplePageAndReturnFirst(_numberOfPagesToAllocate);

            var initialPageNumber = allocatePage.PageNumber;

            using (fst.DirectAdd(initialPageNumber, out bool isNew, out byte* ptr))
            {
                if (isNew == false)
                    ThrowInvalidExistingBuffer();

                // in 32 bits, we pre-allocate just 256 KB, not 2MB
                Debug.Assert(_numberOfPagesToAllocate % 8 == 0);
                Debug.Assert(_numberOfPagesToAllocate % 8 <= BitmapSize);
                Memory.Set(ptr, 0xFF, BitmapSize); // mark the pages that we haven't allocated as busy
                Memory.Set(ptr, 0, _numberOfPagesToAllocate / 8); // mark just the first part as free

            }
            return allocatePage;
        }

        private int ComputeNumberOfPagesToAllocate()
        {
            if (_llt.Environment.Options.ForceUsing32BitsPager || PlatformDetails.Is32Bits)
                return BitmapSize;             // 256 KB
            return  NumberOfPagesInSection; // 2 MB
        }

        [DoesNotReturn]
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
                        // shouldn't actually happen, but same behavior as running out of space
                        page = AllocateMoreSpace(fst);
                        SetValue(fst, page.PageNumber, 0);
                        return page;
                    }
                }
                var startPage = it.CurrentKey;
                while (true)
                {
                    using (it.Value(out Slice slice))
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
                            if (PtrBitVector.GetBitInPointer(buffer, i)) 
                                continue;

                            var currentSectionStart = it.CurrentKey;
                            SetValue(fst, currentSectionStart, i);
                                
                            return _llt.ModifyPage(currentSectionStart + i);
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
            // we run out of space to move forward, let us go to the start and search from there
            return it.Seek(0) && it.CurrentKey != startPage;
        }

        private unsafe void SetValue(FixedSizeTree fst, long pageNumber, int positionInBitmap)
        {
            using (fst.DirectAdd(pageNumber, out bool isNew, out byte* ptr))
            {
                if (isNew)
                    ThrowInvalidNewBuffer();

                PtrBitVector.SetBitInPointer(ptr, positionInBitmap, true);
            }
        }

        private unsafe void UnsetValue(FixedSizeTree fst, long pageNumber, int positionInBitmap)
        {
            using (fst.DirectAdd(pageNumber, out bool isNew, out byte* ptr))
            {
                if (isNew)
                    ThrowInvalidNewBuffer();

                PtrBitVector.SetBitInPointer(ptr, positionInBitmap, false);
            }
        }

        [DoesNotReturn]
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
                    it.CurrentKey + _numberOfPagesToAllocate <= pageNumber)
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

                using (it.Value(out Slice slice))
                {
                    byte* ptr = slice.Content.Ptr;
                    for (int i = 0; i < _numberOfPagesToAllocate; i++)
                    {
                        if (PtrBitVector.GetBitInPointer(ptr, i) == false)
                        {
                            free++;
                        }
                    }
                }
            }


            return new Report
            {
                NumberOfOriginallyAllocatedPages = fst.NumberOfEntries * _numberOfPagesToAllocate,
                NumberOfFreePages = free
            };
        }
        
        public unsafe List<long> AllPages()
        {
            var fst = _parentTree.FixedTreeFor(AllocationStorage, valSize: BitmapSize);

            var results = new List<long>();

            if (fst.PageCount == 0)
                return results;
            
            using (var it = fst.Iterate())
            {
                if (it.Seek(long.MinValue) == false)
                    throw new InvalidOperationException($"Could not seek to the first element of {fst.Name} tree");

                using (it.Value(out Slice slice))
                {
                    byte* ptr = slice.Content.Ptr;
                    for (int i = 0; i < _numberOfPagesToAllocate; i++)
                    {
                        if (PtrBitVector.GetBitInPointer(ptr, i) == false)
                        {
                            results.Add(it.CurrentKey + i);
                        }
                    }
                }
            }


            return results;
        }

        internal FixedSizeTree GetAllocationStorageFst()
        {
            return _parentTree.FixedTreeFor(AllocationStorage, valSize: BitmapSize);
        }

        [DoesNotReturn]
        private static void ThrowInvalidPageReleased(long pageNumber)
        {
            throw new InvalidOperationException("Tried to released page " + pageNumber +
                                                " but couldn't find it in the allocation section");
        }

        [DoesNotReturn]
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
            public void Dispose() {}
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

                llt.Environment.DataPager.MaybePrefetchMemory(llt.DataPagerState,new SectionsIterator(it));
            }
        }

        public sealed class Report
        {
            public long NumberOfFreePages { get; set; }

            public long NumberOfOriginallyAllocatedPages { get; set; }
        }
    }
}

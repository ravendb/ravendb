// -----------------------------------------------------------------------
//  <copyright file="ScratchBufferFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Sparrow.Threading;
using Voron.Global;
using Voron.Impl.Paging;

namespace Voron.Impl.Scratch
{
    public unsafe class ScratchBufferFile : IDisposable
    {
        private class PendingPage
        {
            public long Page;
            public long ValidAfterTransactionId;
        }

        private readonly AbstractPager _scratchPager;
        private readonly int _scratchNumber;

        private readonly Dictionary<long, LinkedList<PendingPage>> _freePagesBySize = new Dictionary<long, LinkedList<PendingPage>>(NumericEqualityComparer.BoxedInstanceInt64);
        private readonly Dictionary<long, LinkedList<long>> _freePagesBySizeAvailableImmediately = new Dictionary<long, LinkedList<long>>(NumericEqualityComparer.BoxedInstanceInt64);
        private readonly Dictionary<long, PageFromScratchBuffer> _allocatedPages = new Dictionary<long, PageFromScratchBuffer>(NumericEqualityComparer.BoxedInstanceInt64);
        private readonly DisposeOnce<SingleAttempt> _disposeOnceRunner;

        private long _allocatedPagesCount;
        private long _lastUsedPage;
        private long _txIdAfterWhichLatestFreePagesBecomeAvailable = -1;

        public long LastUsedPage => _lastUsedPage;

        public ScratchBufferFile(AbstractPager scratchPager, int scratchNumber)
        {
            _scratchPager = scratchPager;
            _scratchNumber = scratchNumber;
            _allocatedPagesCount = 0;

            scratchPager.AllocatedInBytesFunc = () => AllocatedPagesCount * Constants.Storage.PageSize;

            _disposeOnceRunner = new DisposeOnce<SingleAttempt>(() =>
            {
                _scratchPager.PagerState.DiscardOnTxCopy = true;
                _scratchPager.Dispose();
                ClearDictionaries();
            });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ClearDictionaries()
        {
            _allocatedPages.Clear();
            _freePagesBySizeAvailableImmediately.Clear();
            _freePagesBySize.Clear();
        }

        public void Reset()
        {
#if VALIDATE
            foreach (var free in _freePagesBySizeAvailableImmediately)
            {
                foreach (var freeAndAvailablePageNumber in free.Value)
                {
                    byte* freeAndAvailablePagePointer = _scratchPager.AcquirePagePointer(null, freeAndAvailablePageNumber, PagerState);
                    ulong freeAndAvailablePageSize = (ulong)free.Key * Constants.Storage.PageSize;
                    // This has to be forced, as the list of available pages should be protected by default, but this
                    // is a policy we implement inside the ScratchBufferFile only.
                    _scratchPager.UnprotectPageRange(freeAndAvailablePagePointer, freeAndAvailablePageSize, true);
                }
            }            
#endif
            _scratchPager.DiscardWholeFile();
            

#if VALIDATE
            foreach (var free in _freePagesBySize)
            {
                foreach (var val in free.Value)
                {
                    byte* freePageBySizePointer = _scratchPager.AcquirePagePointer(null, val.Page, PagerState);
                    ulong freePageBySizeSize = (ulong)free.Key * Constants.Storage.PageSize;
                    // This has to be forced, as the list of available pages should be protected by default, but this
                    // is a policy we implement inside the ScratchBufferFile only.
                    _scratchPager.UnprotectPageRange(freePageBySizePointer, freePageBySizeSize, true);
                }
            }
#endif
            ClearDictionaries();
            _txIdAfterWhichLatestFreePagesBecomeAvailable = -1;
            _lastUsedPage = 0;
            _allocatedPagesCount = 0;
        }

        public PagerState PagerState => _scratchPager.PagerState;

        public int Number => _scratchNumber;

        public int NumberOfAllocations => _allocatedPages.Count;

        public long Size => _scratchPager.NumberOfAllocatedPages * Constants.Storage.PageSize;

        public long NumberOfAllocatedPages => _scratchPager.NumberOfAllocatedPages;

        public long AllocatedPagesCount => _allocatedPagesCount;

        public long TxIdAfterWhichLatestFreePagesBecomeAvailable => _txIdAfterWhichLatestFreePagesBecomeAvailable;

        public long SizeAfterAllocation(long sizeToAllocate)
        {
            return (_lastUsedPage + sizeToAllocate) * Constants.Storage.PageSize;
        }

        public PageFromScratchBuffer Allocate(LowLevelTransaction tx, int numberOfPages, int sizeToAllocate)
        {
            var pagerState = _scratchPager.EnsureContinuous(_lastUsedPage, sizeToAllocate);
            tx?.EnsurePagerStateReference(pagerState);

            var result = new PageFromScratchBuffer(_scratchNumber, _lastUsedPage, sizeToAllocate, numberOfPages);

            _allocatedPagesCount += numberOfPages;
            _allocatedPages.Add(_lastUsedPage, result);
            _lastUsedPage += sizeToAllocate;

            return result;
        }

        public bool TryGettingFromAllocatedBuffer(LowLevelTransaction tx, int numberOfPages, long size, out PageFromScratchBuffer result)
        {
            result = null;

            LinkedList<long> listOfAvailableImmediately;
            if (_freePagesBySizeAvailableImmediately.TryGetValue(size, out listOfAvailableImmediately) && listOfAvailableImmediately.Count > 0)
            {
                var freeAndAvailablePageNumber = listOfAvailableImmediately.Last.Value;

                listOfAvailableImmediately.RemoveLast();

#if VALIDATE
                byte* freeAndAvailablePagePointer = _scratchPager.AcquirePagePointer(tx, freeAndAvailablePageNumber, PagerState);
                ulong freeAndAvailablePageSize = (ulong)size * Constants.Storage.PageSize;
                // This has to be forced, as the list of available pages should be protected by default, but this
                // is a policy we implement inside the ScratchBufferFile only.
                _scratchPager.UnprotectPageRange(freeAndAvailablePagePointer, freeAndAvailablePageSize, true);
#endif

                result = new PageFromScratchBuffer (_scratchNumber, freeAndAvailablePageNumber, size, numberOfPages);

                _allocatedPagesCount += numberOfPages;
                _allocatedPages.Add(freeAndAvailablePageNumber, result);

                return true;
            }

            LinkedList<PendingPage> list;
            if (!_freePagesBySize.TryGetValue(size, out list) || list.Count <= 0)
                return false;

            var val = list.Last.Value;

            if (val.ValidAfterTransactionId >= tx.Environment.PossibleOldestReadTransaction(tx))
                return false;

            list.RemoveLast();

#if VALIDATE
            byte* freePageBySizePointer = _scratchPager.AcquirePagePointer(tx, val.Page, PagerState);
            ulong freePageBySizeSize = (ulong)size * Constants.Storage.PageSize;
            // This has to be forced, as the list of available pages should be protected by default, but this
            // is a policy we implement inside the ScratchBufferFile only.
            _scratchPager.UnprotectPageRange(freePageBySizePointer, freePageBySizeSize, true);
#endif

            result = new PageFromScratchBuffer ( _scratchNumber, val.Page, size, numberOfPages );

            _allocatedPagesCount += numberOfPages;
            _allocatedPages.Add(val.Page, result);
            return true;
        }

        public bool HasActivelyUsedBytes(long oldestActiveTransaction)
        {
            if (_allocatedPagesCount > 0)
                return true;

            if (oldestActiveTransaction > _txIdAfterWhichLatestFreePagesBecomeAvailable)
                return false;

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Free(long pageNumber, long? txId)
        {
            long asOfTxId = txId ?? -1;

#if VALIDATE
            using (var tempTx = new TempPagerTransaction())
            {
                byte* pagePointer = _scratchPager.AcquirePagePointer(tempTx, pageNumber, PagerState);

                PageFromScratchBuffer temporary;
                if (_allocatedPages.TryGetValue(pageNumber, out temporary) != false)
                {
                    var page = new Page(pagePointer);
                    ulong pageSize = (ulong) (page.IsOverflow ? VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(page.OverflowSize) : 1) * Constants.Storage.PageSize;
                    // This has to be forced, as the scratchPager does NOT protect on allocate,
                    // (on the contrary, we force protection/unprotection when freeing a page and allocating it
                    // from the reserve)
                    _scratchPager.ProtectPageRange(pagePointer, pageSize, true);
                }
            }
#endif

            Free(pageNumber, asOfTxId);
        }

        internal void Free(long page, long asOfTxId)
        {
            if (_allocatedPages.TryGetValue(page, out PageFromScratchBuffer value) == false)
            {
                ThrowInvalidFreeOfUnusedPage(page);
                return; // never called
            }

            _allocatedPagesCount -= value.NumberOfPages;
            _allocatedPages.Remove(page);

            Debug.Assert(value.Size > 0);

            if (asOfTxId == -1)
            {
                // We are freeing without the pages being 'visible' to any party (for ex. rollbacks)
                if (_freePagesBySizeAvailableImmediately.TryGetValue(value.Size, out LinkedList<long> list) == false)
                {
                    list = new LinkedList<long>();
                    _freePagesBySizeAvailableImmediately[value.Size] = list;
                }
                list.AddFirst(value.PositionInScratchBuffer);
            }
            else
            {
                // We are freeing with the pages being 'visible' to any party (for ex. commits)
                if (_freePagesBySize.TryGetValue(value.Size, out LinkedList<PendingPage> list) == false)
                {
                    list = new LinkedList<PendingPage>();
                    _freePagesBySize[value.Size] = list;
                }

                list.AddFirst(new PendingPage
                {
                    Page = value.PositionInScratchBuffer,
                    ValidAfterTransactionId = asOfTxId
                });

                if (asOfTxId > _txIdAfterWhichLatestFreePagesBecomeAvailable)
                    _txIdAfterWhichLatestFreePagesBecomeAvailable = asOfTxId;
            }
        }

        private static void ThrowInvalidFreeOfUnusedPage(long page)
        {
            throw new InvalidOperationException("Attempt to free page that wasn't currently allocated: " + page);
        }

        public int CopyPage(I4KbBatchWrites destI4KbBatchWrites, long p, PagerState pagerState)
        {
            return _scratchPager.CopyPage(destI4KbBatchWrites, p, pagerState);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Page ReadPage(LowLevelTransaction tx, long p, PagerState pagerState = null)
        {
            return new Page(_scratchPager.AcquirePagePointerWithOverflowHandling(tx, p, pagerState));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* AcquirePagePointerWithOverflowHandling(IPagerLevelTransactionState tx, long p)
        {
            return _scratchPager.AcquirePagePointerWithOverflowHandling(tx, p);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* AcquirePagePointerForNewPage(LowLevelTransaction tx, long p, int numberOfPages)
        {
            return _scratchPager.AcquirePagePointerForNewPage(tx, p, numberOfPages);
        }

        internal Dictionary<long, long> GetMostAvailableFreePagesBySize()
        {
            return _freePagesBySize.Keys.ToDictionary(size => size, size =>
            {
                var list = _freePagesBySize[size].Last;

                var value = list?.Value;
                if (value == null)
                    return -1;

                return value.ValidAfterTransactionId;
            });
        }

        public void Dispose()
        {
            _disposeOnceRunner.Dispose();
        }

        public bool IsDisposed => _disposeOnceRunner.Disposed;

        public void BreakLargeAllocationToSeparatePages(IPagerLevelTransactionState tx, PageFromScratchBuffer value)
        {
            if (_allocatedPages.Remove(value.PositionInScratchBuffer) == false)
                InvalidAttemptToBreakupPageThatWasntAllocated(value);

            _allocatedPages.Add(value.PositionInScratchBuffer,
                       new PageFromScratchBuffer(value.ScratchFileNumber, value.PositionInScratchBuffer, 1, 1));

            for (int i = 1; i < value.NumberOfPages; i++)
            {
                _allocatedPages.Add(value.PositionInScratchBuffer + i,
                    new PageFromScratchBuffer(value.ScratchFileNumber, value.PositionInScratchBuffer + i, 1, 1));
            }

            _scratchPager.BreakLargeAllocationToSeparatePages(tx, value.PositionInScratchBuffer);
        }

        private static void InvalidAttemptToBreakupPageThatWasntAllocated(PageFromScratchBuffer value)
        {
            throw new InvalidOperationException("Attempt to break up a page that wasn't currently allocated: " +
                                                value.PositionInScratchBuffer);
        }

        public void EnsureMapped(LowLevelTransaction tx, long p, int numberOfPages)
        {
            _scratchPager.EnsureMapped(tx, p, numberOfPages);
        }

        public PageFromScratchBuffer ShrinkOverflowPage(PageFromScratchBuffer value, int newNumberOfPages)
        {
            if (_allocatedPages.Remove(value.PositionInScratchBuffer) == false)
                InvalidAttemptToShrinkPageThatWasntAllocated(value);

            Debug.Assert(value.NumberOfPages > 1);
            Debug.Assert(value.NumberOfPages > newNumberOfPages);

            var shrinked = new PageFromScratchBuffer(Number, value.PositionInScratchBuffer, value.Size, newNumberOfPages);

            _allocatedPages.Add(shrinked.PositionInScratchBuffer, shrinked);

            _allocatedPagesCount -= value.NumberOfPages - newNumberOfPages;

            return shrinked;
        }

        private static void InvalidAttemptToShrinkPageThatWasntAllocated(PageFromScratchBuffer value)
        {
            throw new InvalidOperationException($"Attempt to shrink a page that wasn't currently allocated: {value.PositionInScratchBuffer}");
        }
    }
}

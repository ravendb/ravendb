// -----------------------------------------------------------------------
//  <copyright file="ScratchBufferFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Sparrow.Threading;
using Voron.Global;
using Voron.Impl.Paging;
using Sparrow.Server.Utils;
using System.Diagnostics.CodeAnalysis;
using Sparrow.Binary;
using Sparrow.Server.LowMemory;

namespace Voron.Impl.Scratch
{
    public sealed class ScratchBufferFile : IDisposable
    {
        private sealed class PendingPage
        {
            public long Page;
            public long ValidAfterTransactionId;
            public long AllocatedInTransaction;
        }

        private readonly Pager _scratchPager;
        private Pager.State _scratchPagerState;
        private readonly int _scratchNumber;

        private readonly Dictionary<long, LinkedList<PendingPage>> _freePagesBySize = new();
        private readonly Dictionary<long, PageFromScratchBuffer> _allocatedPages = new();
        private readonly DisposeOnce<SingleAttempt> _disposeOnceRunner;

        private long _allocatedPagesCount;
        private long _lastUsedPage;
        private long _txIdAfterWhichLatestFreePagesBecomeAvailable = -1;
        private StrongReference<Func<long>> _strongRefToAllocateInBytesFunc;

        public long LastUsedPage => _lastUsedPage;

        public ScratchBufferFile(Pager scratchPager,  Pager.State scratchPagerState, int scratchNumber)
        {
            _scratchPager = scratchPager;
            _scratchPagerState = scratchPagerState;
            _scratchNumber = scratchNumber;
            _allocatedPagesCount = 0;

            _strongRefToAllocateInBytesFunc = new StrongReference<Func<long>>
            {
                Value = () => AllocatedPagesCount * Constants.Storage.PageSize
            };
            MemoryInformation.DirtyMemoryObjects.TryAdd(_strongRefToAllocateInBytesFunc);

            DebugInfo = new ScratchFileDebugInfo(this);

            _disposeOnceRunner = new DisposeOnce<SingleAttempt>(DisposeImpl);
        }

        private void DisposeImpl()
        {
            _strongRefToAllocateInBytesFunc.Value = null; // remove ref (so if there's a left over refs in DirtyMemoryObjects but also function as _disposed = true for racy func invoke)
            MemoryInformation.DirtyMemoryObjects.TryRemove(_strongRefToAllocateInBytesFunc);
            _strongRefToAllocateInBytesFunc = null;

            _scratchPager.Dispose();
            ClearDictionaries();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ClearDictionaries()
        {
            _allocatedPages.Clear();
            _freePagesBySize.Clear();
        }

        public void Reset()
        {
            _scratchPager.DiscardWholeFile(_scratchPagerState);

            ClearDictionaries();
            _txIdAfterWhichLatestFreePagesBecomeAvailable = -1;
            _lastUsedPage = 0;
            _allocatedPagesCount = 0;

            DebugInfo.NumberOfResets++;
            DebugInfo.LastResetTime = DateTime.UtcNow;
        }

        internal (Pager, Pager.State) GetPagerAndState() => (_scratchPager, _scratchPagerState);
        
        public Pager Pager => _scratchPager;

        public int Number => _scratchNumber;

        public int NumberOfAllocations => _allocatedPages.Count;

        public long Size => _scratchPagerState.NumberOfAllocatedPages * Constants.Storage.PageSize;

        public long NumberOfAllocatedPages => _scratchPagerState.NumberOfAllocatedPages;

        public long AllocatedPagesCount => _allocatedPagesCount;

        public long TxIdAfterWhichLatestFreePagesBecomeAvailable => _txIdAfterWhichLatestFreePagesBecomeAvailable;

        public ScratchFileDebugInfo DebugInfo { get; }

        public PageFromScratchBuffer Allocate(LowLevelTransaction tx, int numberOfPages, int sizeToAllocate, long pageNumber, Page previousVersion)
        {
            _scratchPager.EnsureContinuous(ref _scratchPagerState, _lastUsedPage, sizeToAllocate);
            
            var result = new PageFromScratchBuffer(this,_scratchPagerState, tx.Id, _lastUsedPage, pageNumber, previousVersion, sizeToAllocate, numberOfPages);

            _allocatedPagesCount += numberOfPages;
            _allocatedPages.Add(_lastUsedPage, result);
            _lastUsedPage += sizeToAllocate;

            return result;
        }

        public bool TryGettingFromAllocatedBuffer(LowLevelTransaction tx, int numberOfPages, int size, long pageNumber, Page previousVersion, out PageFromScratchBuffer result)
        {
            result = null;

            if (!_freePagesBySize.TryGetValue(size, out LinkedList<PendingPage> list) || list.Count <= 0)
                return false;

            var val = list.Last!.Value;

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

            result = new PageFromScratchBuffer(this, _scratchPagerState, tx.Id,val.Page, pageNumber, previousVersion, size, numberOfPages);

            _allocatedPagesCount += numberOfPages;
            _allocatedPages.Add(val.Page, result);
            return true;
        }

        public bool HasActivelyUsedBytes(long oldestActiveTransaction)
        {
            return _allocatedPagesCount > 0 || oldestActiveTransaction <= _txIdAfterWhichLatestFreePagesBecomeAvailable;
        }

        public bool Free(LowLevelTransaction tx, long page)
        {
            return Free(tx, tx.Id, page);
        }
        
        public bool Free(LowLevelTransaction tx, long asOfTxId, long page)
        {
#if VALIDATE
            // If we have encryption enabled, then VALIDATE calls are handled by the EncryptionBufferPool
            if (Pager.Options.Encryption.IsEnabled == false)
            {
                using (var tempTx = new TempPagerTransaction())
                {
                    var pagePointer = _scratchPager.AcquirePagePointer(tempTx, pageNumber, PagerState);
                    if (_allocatedPages.TryGetValue(pageNumber, out _))
                    {
                        var page = new Page(pagePointer);
                        var pageSize = (ulong)(page.IsOverflow ? VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(page.OverflowSize) : 1) *
                                       Constants.Storage.PageSize;
                        _scratchPager.ProtectPageRange(pagePointer, pageSize, true);
                    }
                }
            }
#endif
            
            if (_allocatedPages.TryGetValue(page, out PageFromScratchBuffer value) == false)
            {
                ThrowInvalidFreeOfUnusedPage(page);
                return default; // never called
            }

            tx.ForgetAboutScratchPage(value);
            DebugInfo.LastFreeTime = DateTime.UtcNow;
            // use current write tx id to prevent from overriding a scratch page by write tx 
            // while there might be old read tx looking at it, so we'll only allocate from it
            // _after_ all transactions are past the _current_ write transaction
            DebugInfo.LastAsOfTxIdWhenFree = asOfTxId;

            _allocatedPagesCount -= value.NumberOfPages;
            _allocatedPages.Remove(page);

            Debug.Assert(value.NumberOfPages > 0);

            if (_freePagesBySize.TryGetValue(value.Size, out LinkedList<PendingPage> list) == false)
            {
                list = new LinkedList<PendingPage>();
                _freePagesBySize[value.Size] = list;
            }

            list.AddFirst(new PendingPage
            {
                Page = value.PositionInScratchBuffer,
                ValidAfterTransactionId = asOfTxId,
                AllocatedInTransaction = value.AllocatedInTransaction,
            });

            _txIdAfterWhichLatestFreePagesBecomeAvailable = asOfTxId;

            return NumberOfAllocations == 0; 
        }

        public ref Pager.State GetStateRef() => ref _scratchPagerState;

        [DoesNotReturn]
        private static void ThrowInvalidFreeOfUnusedPage(long page)
        {
            throw new InvalidOperationException("Attempt to free page that wasn't currently allocated: " + page);
        }

        public void Dispose()
        {
            _disposeOnceRunner.Dispose();
        }

        public bool IsDisposed => _disposeOnceRunner.Disposed;

        public PageFromScratchBuffer ShrinkOverflowPage(PageFromScratchBuffer value, int newNumberOfPages)
        {
            if (_allocatedPages.Remove(value.PositionInScratchBuffer) == false)
                InvalidAttemptToShrinkPageThatWasntAllocated(value);

            Debug.Assert(value.NumberOfPages > 1);
            Debug.Assert(value.NumberOfPages > newNumberOfPages);

            var shrinked = value with
            {
                NumberOfPages = newNumberOfPages, 
                PreviousVersion = value.PreviousVersion
            }; 

            _allocatedPages.Add(shrinked.PositionInScratchBuffer, shrinked);

            _allocatedPagesCount -= value.NumberOfPages - newNumberOfPages;

            return shrinked;
        }

        private static void InvalidAttemptToShrinkPageThatWasntAllocated(PageFromScratchBuffer value)
        {
            throw new InvalidOperationException($"Attempt to shrink a page that wasn't currently allocated: {value.PositionInScratchBuffer}");
        }

        public sealed class ScratchFileDebugInfo
        {
            private readonly ScratchBufferFile _parent;

            public ScratchFileDebugInfo(ScratchBufferFile parent)
            {
                _parent = parent;
            }

            public DateTime? LastResetTime { get; set; }

            public int NumberOfResets { get; set; }

            public DateTime? LastFreeTime { get; set; }

            public long LastAsOfTxIdWhenFree { get; set; }

            internal Dictionary<long, (long ValidAfterTransactionId, long AllocatedInTransaction)> GetMostAvailableFreePagesBySize()
            {
                return _parent._freePagesBySize.Keys.ToDictionary(size => size, size =>
                {
                    if (_parent._freePagesBySize.TryGetValue(size, out var pendingPages) == false)
                        return (-1, -1);

                    var value = pendingPages.Last?.Value;
                    if (value == null)
                        return (-1, -1);

                    return (value.ValidAfterTransactionId, value.AllocatedInTransaction);
                });
            }

            internal List<PageFromScratchBuffer> GetFirst10AllocatedPages()
            {
                var pages = new List<PageFromScratchBuffer>();

                foreach (var key in _parent._allocatedPages.Keys)
                {
                    if (_parent._allocatedPages.TryGetValue(key, out var pageFromScratchBuffer) == false)
                        continue;

                    if (pageFromScratchBuffer == null)
                        continue;

                    pages.Add(pageFromScratchBuffer);

                    if (pages.Count == 10)
                        break;
                }

                return pages;
            }
        }

        [Conditional("DEBUG")]
        public void VerifyMatch(long pageNumberInDataFile, long positionInScratchBuffer, int numberOfPages)
        {
            if (_allocatedPages.TryGetValue(positionInScratchBuffer, out var allocated) is false)
                return;
            
            if(allocated.PageNumberInDataFile != pageNumberInDataFile || 
               allocated.NumberOfPages != numberOfPages)
                throw new InvalidOperationException(
                    $"Failed to verify page {pageNumberInDataFile} when reading scratch page {positionInScratchBuffer}, values different!" +
                    $"Page: {pageNumberInDataFile} vs. {allocated.PageNumberInDataFile} ({numberOfPages} vs {allocated.NumberOfPages})!");

        }

        [Conditional("DEBUG")]
        public void AssertNoPagesAllocatedInTransactionOlderThan(long txId)
        {
            foreach (PageFromScratchBuffer p in _allocatedPages.Values)
            {
                if (p.AllocatedInTransaction < txId)
                {
                    var message =
                        $"Found page #{p.PageNumberInDataFile} allocated in tx {p.AllocatedInTransaction} (scratch {p.File.Number}, pos in scratch: {p.PositionInScratchBuffer}) while we freed up to tx {txId}";

                    throw new InvalidOperationException(message);
                }
            }
        }
    }
}
